import { EventEmitter } from 'node:events';
import { Admin, Producer } from '@platformatic/kafka';
import { KafkaProducerService } from '../src/shared/modules/kafka/kafka-producer.service';
import {
  type KafkaBusEvent,
  KafkaConnectionState,
} from '../src/shared/modules/kafka/kafka.types';

jest.mock('@platformatic/kafka', () => ({
  Admin: jest.fn(),
  Producer: jest.fn(),
}));

jest.mock('../src/shared/config/kafka.config', () => ({
  KafkaConfig: {
    brokers: ['broker-1:9092', 'broker-2:9092'],
    clientId: 'bus-api-v6-test',
    tls: {
      enabled: false,
      rejectUnauthorized: true,
    },
    sasl: {
      mechanism: undefined,
      username: undefined,
      password: undefined,
    },
    connectionTimeout: 11000,
    brokerTimeout: 7000,
    requestTimeout: 31000,
    metadataRefreshInterval: 60000,
    retry: {
      attempts: 3,
      initialRetryTime: 100,
      maxRetryTime: 100,
    },
  },
}));

jest.mock('../src/shared/modules/global/logger.service', () => ({
  LoggerService: {
    forRoot: jest.fn(() => ({
      log: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    })),
  },
}));

type ProducerDouble = EventEmitter & {
  metadata: jest.Mock;
  send: jest.Mock;
  close: jest.Mock;
};

type AdminDouble = EventEmitter & {
  listTopics: jest.Mock;
  metadata: jest.Mock;
  close: jest.Mock;
};

interface ClientPair {
  producer: ProducerDouble;
  admin: AdminDouble;
}

interface Deferred<Value> {
  promise: Promise<Value>;
  resolve: (value: Value) => void;
  reject: (reason?: unknown) => void;
}

interface KafkaProducerServiceInternals {
  reconnectionTask?: Promise<void>;
  reconnectDelayTimer?: NodeJS.Timeout;
}

const ProducerMock = Producer as unknown as jest.Mock;
const AdminMock = Admin as unknown as jest.Mock;

/**
 * Creates the Platformatic producer surface used by the lifecycle tests.
 *
 * @returns An event-emitting producer double with successful defaults.
 */
function createProducerDouble(): ProducerDouble {
  return Object.assign(new EventEmitter(), {
    metadata: jest.fn().mockResolvedValue({}),
    send: jest.fn().mockResolvedValue({
      responses: [],
      unwritableNodes: [],
    }),
    close: jest.fn().mockResolvedValue(undefined),
  });
}

/**
 * Creates the Platformatic admin surface used by the lifecycle tests.
 *
 * The inherited `metadata` method is present only so tests can prove topic
 * discovery uses the Platformatic 2.8 `listTopics()` API.
 *
 * @returns An event-emitting admin double with successful defaults.
 */
function createAdminDouble(): AdminDouble {
  return Object.assign(new EventEmitter(), {
    listTopics: jest.fn().mockResolvedValue(['startup.topic']),
    metadata: jest.fn().mockResolvedValue({}),
    close: jest.fn().mockResolvedValue(undefined),
  });
}

/**
 * Creates one producer/admin pair for a fresh Kafka client generation.
 *
 * @returns A pair of independent event-emitting Kafka client doubles.
 */
function createClientPair(): ClientPair {
  return {
    producer: createProducerDouble(),
    admin: createAdminDouble(),
  };
}

/**
 * Creates a promise whose completion is controlled by the current test.
 *
 * @returns The promise and its externally callable resolve/reject functions.
 */
function createDeferred<Value>(): Deferred<Value> {
  let resolve: Deferred<Value>['resolve'] = () => undefined;
  let reject: Deferred<Value>['reject'] = () => undefined;
  const promise = new Promise<Value>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });

  return { promise, resolve, reject };
}

/**
 * Reads the active private recovery task after a lifecycle action schedules it.
 *
 * Tests use the task only as a deterministic completion boundary.
 *
 * @param service Kafka service whose recovery task should be active.
 * @returns The active recovery promise.
 * @throws Error when no recovery task is active.
 */
function getReconnectTask(service: KafkaProducerService): Promise<void> {
  const task = (service as unknown as KafkaProducerServiceInternals)
    .reconnectionTask;
  if (!task) {
    throw new Error('Expected a Kafka reconnection task to be active');
  }
  return task;
}

/**
 * Flushes promise continuations without advancing fake wall-clock timers.
 *
 * @param turns Maximum microtask turns to flush.
 * @returns A promise resolved after the requested continuations run.
 */
async function flushMicrotasks(turns = 12): Promise<void> {
  for (let turn = 0; turn < turns; turn += 1) {
    await Promise.resolve();
  }
}

describe('KafkaProducerService lifecycle', () => {
  let queuedProducers: ProducerDouble[];
  let queuedAdmins: AdminDouble[];

  /**
   * Queues a producer/admin pair for the next fresh client generation.
   *
   * @param pair Client pair returned by the next constructor calls.
   * @returns The supplied pair for convenient per-test configuration.
   */
  function queuePair(pair = createClientPair()): ClientPair {
    queuedProducers.push(pair.producer);
    queuedAdmins.push(pair.admin);
    return pair;
  }

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-07-29T00:00:00.000Z'));
    jest.clearAllMocks();
    jest.spyOn(Math, 'random').mockReturnValue(0);
    queuedProducers = [];
    queuedAdmins = [];

    ProducerMock.mockImplementation(() => {
      const producer = queuedProducers.shift();
      if (!producer) {
        throw new Error('No producer double was queued for this generation');
      }
      return producer;
    });
    AdminMock.mockImplementation(() => {
      const admin = queuedAdmins.shift();
      if (!admin) {
        throw new Error('No admin double was queued for this generation');
      }
      return admin;
    });
  });

  afterEach(() => {
    jest.clearAllTimers();
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  it('starts with an actively verified client pair and disables Platformatic retries', async () => {
    const pair = queuePair();
    pair.admin.listTopics.mockResolvedValueOnce([
      'zeta.topic',
      'alpha.topic',
      'alpha.topic',
    ]);
    const service = new KafkaProducerService();

    service.onModuleInit();
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Initializing,
      connected: false,
      healthy: false,
      ready: false,
    });
    await getReconnectTask(service);

    const expectedOptions = {
      clientId: 'bus-api-v6-test',
      bootstrapBrokers: ['broker-1:9092', 'broker-2:9092'],
      connectTimeout: 11000,
      requestTimeout: 31000,
      timeout: 7000,
      retries: 0,
      repeatOnStaleMetadata: false,
    };
    expect(Producer).toHaveBeenCalledTimes(1);
    expect(Producer).toHaveBeenCalledWith(expectedOptions);
    expect(Admin).toHaveBeenCalledTimes(1);
    expect(Admin).toHaveBeenCalledWith(expectedOptions);
    expect(pair.producer.metadata).toHaveBeenCalledWith({
      topics: [],
      forceUpdate: true,
    });
    expect(pair.admin.listTopics).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: true,
      healthy: true,
      ready: true,
      initializationAttempts: 1,
      reconnectAttempts: 0,
    });
    expect(service.getKafkaStatus().lastSuccessfulMetadataRefreshTime).toBe(
      '2026-07-29T00:00:00.000Z',
    );
  });

  it('uses Admin.listTopics for fresh sorted topic discovery', async () => {
    const pair = queuePair();
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);
    pair.admin.listTopics.mockResolvedValueOnce([
      'zeta.topic',
      'alpha.topic',
      'alpha.topic',
    ]);

    await expect(service.listTopics()).resolves.toEqual([
      'alpha.topic',
      'zeta.topic',
    ]);

    expect(pair.producer.metadata).toHaveBeenCalledTimes(2);
    expect(pair.producer.metadata).toHaveBeenLastCalledWith({
      topics: [],
      forceUpdate: true,
    });
    expect(pair.admin.listTopics).toHaveBeenCalledTimes(2);
    expect(pair.admin.metadata).not.toHaveBeenCalled();
  });

  it('runs the 60-second active probe as a single flight', async () => {
    const pair = queuePair();
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);
    const pendingMetadata = createDeferred<object>();
    pair.producer.metadata.mockReturnValueOnce(pendingMetadata.promise);
    pair.admin.listTopics.mockResolvedValueOnce(['refreshed.topic']);

    await jest.advanceTimersByTimeAsync(59999);
    expect(pair.producer.metadata).toHaveBeenCalledTimes(1);

    await jest.advanceTimersByTimeAsync(1);
    expect(pair.producer.metadata).toHaveBeenCalledTimes(2);
    expect(pair.admin.listTopics).toHaveBeenCalledTimes(2);

    const joinedProbe = service.listTopics();
    await jest.advanceTimersByTimeAsync(60000);
    expect(pair.producer.metadata).toHaveBeenCalledTimes(2);
    expect(pair.admin.listTopics).toHaveBeenCalledTimes(2);

    pendingMetadata.resolve({});
    await expect(joinedProbe).resolves.toEqual(['refreshed.topic']);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: true,
      healthy: true,
    });
  });

  it('deduplicates immediate active probes after broker disconnect events', async () => {
    const pair = queuePair();
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);
    pair.admin.listTopics.mockResolvedValueOnce(['after-disconnect.topic']);

    pair.producer.emit('client:broker:disconnect', {});
    pair.admin.emit('client:broker:disconnect', {});

    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: false,
      healthy: true,
    });

    jest.runAllTicks();
    await flushMicrotasks();

    expect(pair.producer.metadata).toHaveBeenCalledTimes(2);
    expect(pair.admin.listTopics).toHaveBeenCalledTimes(2);
    expect(Producer).toHaveBeenCalledTimes(1);
    expect(Admin).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: true,
      healthy: true,
    });
  });

  it('runs a follow-up probe when disconnect overlaps an active probe', async () => {
    const pair = queuePair();
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);
    const pendingMetadata = createDeferred<object>();
    pair.producer.metadata.mockReturnValueOnce(pendingMetadata.promise);

    const activeProbe = service.listTopics();
    pair.producer.emit('client:broker:disconnect', {});
    expect(service.getKafkaStatus().connected).toBe(false);

    pendingMetadata.resolve({});
    await activeProbe;
    jest.runAllTicks();
    await flushMicrotasks();

    expect(pair.producer.metadata).toHaveBeenCalledTimes(3);
    expect(pair.admin.listTopics).toHaveBeenCalledTimes(3);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: true,
      healthy: true,
    });
  });

  it('keeps active metadata traffic running beyond the broker idle boundary', async () => {
    const pair = queuePair();
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);

    await jest.advanceTimersByTimeAsync(11 * 60 * 1000);

    expect(pair.producer.metadata).toHaveBeenCalledTimes(12);
    expect(pair.admin.listTopics).toHaveBeenCalledTimes(12);
    expect(Producer).toHaveBeenCalledTimes(1);
    expect(Admin).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: true,
      healthy: true,
      ready: true,
    });
  });

  it('recovers startup with a fresh client pair after the first probe fails', async () => {
    const failedPair = queuePair();
    failedPair.producer.metadata.mockRejectedValueOnce(
      new Error('startup broker unavailable'),
    );
    const recoveredPair = queuePair();
    recoveredPair.admin.listTopics.mockResolvedValueOnce(['recovered.topic']);
    const service = new KafkaProducerService();

    service.onModuleInit();
    const recovery = getReconnectTask(service);
    await flushMicrotasks();
    await jest.advanceTimersByTimeAsync(100);
    await recovery;

    expect(Producer).toHaveBeenCalledTimes(2);
    expect(Admin).toHaveBeenCalledTimes(2);
    expect(failedPair.producer.close).toHaveBeenCalledTimes(1);
    expect(failedPair.admin.close).toHaveBeenCalledTimes(1);
    expect(recoveredPair.producer.metadata).toHaveBeenCalledTimes(1);
    expect(recoveredPair.admin.listTopics).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: true,
      healthy: true,
      ready: true,
      initializationAttempts: 2,
      reconnectAttempts: 0,
    });
  });

  it('deduplicates concurrent active-client failures into one recovery run', async () => {
    const activePair = queuePair();
    const replacementPair = queuePair();
    replacementPair.admin.listTopics.mockResolvedValueOnce([
      'replacement.topic',
    ]);
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);

    activePair.producer.emit('error', new Error('producer failed'));
    activePair.admin.emit('error', new Error('admin failed'));
    const recovery = getReconnectTask(service);
    await jest.advanceTimersByTimeAsync(100);
    await recovery;

    expect(Producer).toHaveBeenCalledTimes(2);
    expect(Admin).toHaveBeenCalledTimes(2);
    expect(activePair.producer.close).toHaveBeenCalledTimes(1);
    expect(activePair.admin.close).toHaveBeenCalledTimes(1);
    expect(replacementPair.producer.metadata).toHaveBeenCalledTimes(1);
    expect(replacementPair.admin.listTopics).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: true,
      healthy: true,
      initializationAttempts: 2,
    });
  });

  it('does not replay a failed publish and uses the recovered producer later', async () => {
    const activePair = queuePair();
    activePair.producer.send.mockRejectedValueOnce(
      new Error('produce transport failed'),
    );
    const replacementPair = queuePair();
    const service = new KafkaProducerService();
    const event: KafkaBusEvent = {
      topic: 'unit.test',
      originator: 'unit-suite',
      timestamp: 123456789,
      'mime-type': 'application/json',
      payload: { answer: 42 },
      key: 'event-key',
    };
    service.onModuleInit();
    await getReconnectTask(service);

    await expect(service.publishEvent(event)).rejects.toThrow(
      'Kafka failed to publish the event',
    );
    expect(activePair.producer.send).toHaveBeenCalledTimes(1);

    const sentMessage = activePair.producer.send.mock.calls[0][0].messages[0];
    expect(JSON.parse(sentMessage.value.toString())).toEqual(event);
    expect(sentMessage.key).toEqual(Buffer.from('event-key'));
    expect(
      Object.fromEntries(
        [...sentMessage.headers].map(([key, value]) => [
          key.toString(),
          value.toString(),
        ]),
      ),
    ).toEqual({
      originator: 'unit-suite',
      'mime-type': 'application/json',
      timestamp: '123456789',
      topic: 'unit.test',
    });

    const recovery = getReconnectTask(service);
    await jest.advanceTimersByTimeAsync(100);
    await recovery;
    await expect(service.publishEvent(event)).resolves.toBeUndefined();

    expect(activePair.producer.send).toHaveBeenCalledTimes(1);
    expect(replacementPair.producer.send).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      healthy: true,
      ready: true,
    });
  });

  it('does not recover for unknown-topic or serialization failures', async () => {
    const pair = queuePair();
    pair.producer.send.mockRejectedValueOnce(
      new Error('UNKNOWN_TOPIC_OR_PARTITION'),
    );
    const service = new KafkaProducerService();
    const event: KafkaBusEvent = {
      topic: 'missing.topic',
      originator: 'unit-suite',
      timestamp: 123456789,
      'mime-type': 'application/json',
      payload: {},
    };
    service.onModuleInit();
    await getReconnectTask(service);

    await expect(service.publishEvent(event)).rejects.toThrow(
      'Unknown event type',
    );

    const cyclicPayload: Record<string, unknown> = {};
    cyclicPayload.self = cyclicPayload;
    await expect(
      service.publishEvent({ ...event, payload: cyclicPayload }),
    ).rejects.toThrow('Kafka failed to publish the event');

    expect(pair.producer.send).toHaveBeenCalledTimes(1);
    expect(Producer).toHaveBeenCalledTimes(1);
    expect(Admin).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      healthy: true,
      ready: true,
    });
  });

  it('returns cached topics while a failed active probe recovers clients', async () => {
    const activePair = queuePair();
    const replacementPair = queuePair();
    replacementPair.admin.listTopics.mockResolvedValueOnce([
      'replacement.topic',
    ]);
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);
    activePair.producer.metadata.mockRejectedValueOnce(
      new Error('metadata transport failed'),
    );

    await expect(service.listTopics()).resolves.toEqual(['startup.topic']);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Reconnecting,
      connected: false,
      healthy: true,
      ready: false,
    });

    const recovery = getReconnectTask(service);
    await jest.advanceTimersByTimeAsync(100);
    await recovery;

    expect(activePair.producer.close).toHaveBeenCalledTimes(1);
    expect(replacementPair.producer.metadata).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Ready,
      connected: true,
      healthy: true,
    });
  });

  it('enters terminal failure after the fresh-client recovery budget is exhausted', async () => {
    const activePair = queuePair();
    const failedRecoveryPairs = Array.from({ length: 3 }, () => {
      const pair = queuePair();
      pair.producer.metadata.mockRejectedValueOnce(
        new Error('metadata probe failed'),
      );
      return pair;
    });
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);

    activePair.producer.emit('error', new Error('active producer failed'));
    const recovery = getReconnectTask(service);
    await jest.runAllTimersAsync();
    await recovery;

    expect(Producer).toHaveBeenCalledTimes(4);
    expect(Admin).toHaveBeenCalledTimes(4);
    for (const pair of failedRecoveryPairs) {
      expect(pair.producer.close).toHaveBeenCalledTimes(1);
      expect(pair.admin.close).toHaveBeenCalledTimes(1);
    }
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Failed,
      connected: false,
      healthy: false,
      ready: false,
      initializationAttempts: 4,
      reconnectAttempts: 3,
      lastFailureReason: 'Error: metadata probe failed',
    });
  });

  it('cancels recovery backoff and closes both clients safely during shutdown', async () => {
    const activePair = queuePair();
    const failedCandidate = queuePair();
    failedCandidate.producer.metadata.mockRejectedValueOnce(
      new Error('candidate probe failed'),
    );
    failedCandidate.producer.close.mockRejectedValueOnce(
      new Error('producer close failed'),
    );
    queuePair();
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);

    activePair.producer.emit('error', new Error('active producer failed'));
    await jest.advanceTimersByTimeAsync(50);
    await flushMicrotasks();
    expect(
      (service as unknown as KafkaProducerServiceInternals).reconnectDelayTimer,
    ).toBeDefined();

    await service.onApplicationShutdown('SIGTERM');

    expect(Producer).toHaveBeenCalledTimes(2);
    expect(Admin).toHaveBeenCalledTimes(2);
    expect(activePair.producer.close).toHaveBeenCalledTimes(1);
    expect(activePair.admin.close).toHaveBeenCalledTimes(1);
    expect(failedCandidate.producer.close).toHaveBeenCalledTimes(2);
    expect(failedCandidate.admin.close).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Stopped,
      connected: false,
      healthy: false,
      ready: false,
    });
    expect(() =>
      failedCandidate.producer.emit('error', new Error('late producer error')),
    ).not.toThrow();
    expect(() =>
      failedCandidate.admin.emit('error', new Error('late admin error')),
    ).not.toThrow();

    await jest.advanceTimersByTimeAsync(60000);
    expect(Producer).toHaveBeenCalledTimes(2);
    expect(Admin).toHaveBeenCalledTimes(2);
  });

  it('bounds shutdown while an active metadata verification is pending', async () => {
    const pair = queuePair();
    const service = new KafkaProducerService();
    service.onModuleInit();
    await getReconnectTask(service);
    const pendingMetadata = createDeferred<object>();
    pair.producer.metadata.mockReturnValueOnce(pendingMetadata.promise);

    await jest.advanceTimersByTimeAsync(60000);
    const shutdown = service.onApplicationShutdown('SIGTERM');
    await jest.advanceTimersByTimeAsync(10000);
    await shutdown;

    expect(pair.producer.close).toHaveBeenCalledTimes(1);
    expect(pair.admin.close).toHaveBeenCalledTimes(1);
    expect(service.getKafkaStatus()).toMatchObject({
      state: KafkaConnectionState.Stopped,
      connected: false,
      healthy: false,
      ready: false,
    });

    pendingMetadata.resolve({});
    await flushMicrotasks();
  });
});
