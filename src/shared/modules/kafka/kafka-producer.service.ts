import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import {
  Admin,
  type AdminOptions,
  type ConnectionPoolEventPayload,
  type MessageToProduce,
  Producer,
  type ProducerOptions,
  type ProduceResult,
  type SASLOptions,
} from '@platformatic/kafka';
import { KafkaConfig } from '../../config/kafka.config';
import { LoggerService } from '../global/logger.service';
import { KafkaMetadataException, KafkaPublishException } from './kafka.errors';
import {
  KafkaConnectionState,
  type KafkaBusEvent,
  KafkaFailureClassification,
  KafkaPublishErrorKind,
  type KafkaStatus,
} from './kafka.types';

interface ClosableKafkaClient {
  close(): Promise<void>;
}

type BusProducer = Producer<Buffer, Buffer, Buffer, Buffer>;
type KafkaClientRole = 'producer' | 'admin';

interface KafkaClientListeners {
  error: (error: Error) => void;
  brokerDisconnect?: (payload: ConnectionPoolEventPayload) => void;
  brokerFailed?: (payload: ConnectionPoolEventPayload) => void;
}

interface KafkaClientSet {
  producer: BusProducer;
  admin: Admin;
  generation: number;
  producerListeners: KafkaClientListeners;
  adminListeners: KafkaClientListeners;
  candidateFailure?: Error;
  closePromise?: Promise<boolean>;
}

interface ActiveMetadataProbe {
  clients: KafkaClientSet;
  promise: Promise<string[]>;
}

/** Current production broker idle timeout used to validate probe cadence. */
const KAFKA_BROKER_IDLE_TIMEOUT_MS = 600000;

/** Prevents a failed client close from blocking recovery or ECS shutdown. */
const KAFKA_CLIENT_CLOSE_TIMEOUT_MS = 5000;

/** Keeps all Kafka shutdown bookkeeping inside the ECS stop window. */
const KAFKA_SHUTDOWN_SETTLE_TIMEOUT_MS = 10000;

/**
 * Keeps a detached Kafka emitter safe if it emits a late EventEmitter `error`
 * after an asynchronous close failure.
 *
 * @returns Nothing.
 */
const ignoreDetachedKafkaError = (): void => undefined;

/**
 * Owns the Bus API's shared Kafka producer and metadata-client lifecycle.
 *
 * Future application services use this class to publish complete event bodies,
 * retrieve live topic names with a last-known fallback, and report Kafka
 * readiness without depending directly on `@platformatic/kafka`.
 */
@Injectable()
export class KafkaProducerService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly logger = LoggerService.forRoot(KafkaProducerService.name);
  private readonly clientOptions: ProducerOptions<
    Buffer,
    Buffer,
    Buffer,
    Buffer
  > &
    AdminOptions;
  private clients?: KafkaClientSet;
  private recoveryCandidate?: KafkaClientSet;
  private activeMetadataProbe?: ActiveMetadataProbe;
  private metadataProbeRequestedAfterCurrent?: KafkaClientSet;
  private reconnectionTask?: Promise<void>;
  private reconnectRequested = false;
  private metadataRefreshTimer?: NodeJS.Timeout;
  private reconnectDelayTimer?: NodeJS.Timeout;
  private resolveReconnectDelay?: () => void;
  private state = KafkaConnectionState.Initializing;
  private initializationAttempts = 0;
  private reconnectAttempts = 0;
  private lastFailureReason?: string;
  private lastSuccessfulMetadataRefreshTime?: string;
  private lastKnownTopics?: string[];
  private activeConnectionVerified = false;
  private hasEverBeenReady = false;
  private nextClientGeneration = 0;
  private shuttingDown = false;

  /**
   * Validates configuration and prepares immutable Platformatic client options.
   *
   * Synchronous configuration errors are intentionally fatal; network failures
   * are handled by the bounded asynchronous recovery lifecycle.
   */
  constructor() {
    this.validateConfiguration();
    this.clientOptions = this.createClientOptions();
  }

  /**
   * Starts bounded Kafka initialization without blocking HTTP application boot.
   *
   * Health requests read cached lifecycle state while this background task
   * creates and actively verifies the initial producer/admin pair.
   *
   * @returns Nothing; completion is reflected in Kafka lifecycle state.
   */
  onModuleInit(): void {
    void this.scheduleReconnect();
  }

  /**
   * Stops Kafka activity and closes the producer and admin clients defensively.
   *
   * Nest invokes this method for application shutdown hooks. In-flight probes
   * and candidate verification are interrupted by closing their clients.
   * Shutdown failures are logged and do not prevent the other client closing.
   *
   * @param signal Optional process signal supplied by Nest during shutdown.
   * @returns A promise that resolves after all shutdown work has settled.
   */
  async onApplicationShutdown(signal?: string): Promise<void> {
    this.shuttingDown = true;
    this.state = KafkaConnectionState.ShuttingDown;
    this.activeConnectionVerified = false;
    this.reconnectRequested = false;
    this.metadataProbeRequestedAfterCurrent = undefined;
    this.clearMetadataRefreshTimer();
    this.cancelReconnectDelay();
    this.logger.log({
      message: 'Kafka shutdown started',
      signal: signal ?? 'application-close',
    });

    const currentClients = this.clients;
    const recoveryCandidate = this.recoveryCandidate;
    this.clients = undefined;
    this.recoveryCandidate = undefined;

    const tasks: Promise<unknown>[] = [
      this.closeClientSet(currentClients),
      this.closeClientSet(recoveryCandidate),
    ];
    if (this.activeMetadataProbe) {
      tasks.push(this.activeMetadataProbe.promise);
    }
    if (this.reconnectionTask) {
      tasks.push(this.reconnectionTask);
    }

    await this.settleShutdownTasks(tasks);

    // A candidate can only appear at an await boundary; this second close is a
    // defensive sweep for shutdowns racing that boundary.
    await Promise.allSettled([
      this.closeClientSet(this.clients),
      this.closeClientSet(this.recoveryCandidate),
    ]);
    this.clients = undefined;
    this.recoveryCandidate = undefined;
    this.state = KafkaConnectionState.Stopped;
    this.logger.log('Kafka shutdown completed');
  }

  /**
   * Publishes a complete v5-compatible event body to its submitted topic.
   *
   * The event is JSON encoded as the Kafka value. An optional event key is
   * included only when supplied, and the exact `originator`, `mime-type`,
   * `timestamp`, and `topic` headers are emitted as buffers.
   *
   * @param event Complete internal bus event to publish.
   * @returns A promise that resolves after Kafka accepts the send request.
   * @throws KafkaPublishException with a bad-request classification for unknown
   * topic failures, or a server-error classification for all other failures.
   */
  async publishEvent(event: KafkaBusEvent): Promise<void> {
    let publishStarted = false;
    let publishClients: KafkaClientSet | undefined;

    try {
      publishClients = this.clients;
      if (!this.isReady() || !publishClients) {
        throw new KafkaPublishException(
          'Kafka producer is not ready',
          KafkaPublishErrorKind.KafkaFailure,
          KafkaFailureClassification.ServerError,
        );
      }

      const value = this.serializeEvent(event);
      const message: MessageToProduce<Buffer, Buffer, Buffer, Buffer> = {
        topic: event.topic,
        value,
        headers: this.createHeaders(event),
      };

      if (event.key !== undefined) {
        message.key = this.toBuffer(event.key);
      }

      publishStarted = true;
      const result = await publishClients.producer.send({
        messages: [message],
      });
      const sendFailure = this.findSendResultFailure(result);
      if (sendFailure !== undefined) {
        throw sendFailure;
      }

      this.logger.log({
        message: 'Kafka event published successfully',
        topic: event.topic,
        keyPresent: event.key !== undefined,
      });
    } catch (error) {
      const exception =
        error instanceof KafkaPublishException
          ? error
          : this.classifyPublishFailure(event.topic, error);
      this.logger.error(
        {
          message: 'Kafka event publish failed',
          topic: event.topic,
          keyPresent: event.key !== undefined,
          classification: exception.classification,
          kind: exception.kind,
        },
        this.getErrorTrace(error),
      );

      if (
        publishStarted &&
        publishClients &&
        exception.kind !== KafkaPublishErrorKind.UnknownTopic
      ) {
        this.handleKafkaFailure(
          'Kafka publish operation failed; scheduling client recovery',
          error,
          publishClients,
        );
      }

      throw exception;
    }
  }

  /**
   * Retrieves a fresh list of Kafka topics with a last-known fallback.
   *
   * Every invocation first forces a live `metadata` query for all topics.
   * Successful results replace the cache and refresh timestamp. A failed query
   * returns a defensive cache copy only when a prior successful query exists.
   *
   * @returns A defensive copy of fresh or last-known topic names.
   * @throws KafkaMetadataException when metadata fails before any cache exists.
   */
  async listTopics(): Promise<string[]> {
    try {
      if (!this.isReady() || !this.clients) {
        throw new Error('Kafka metadata client is not ready');
      }

      return await this.getOrStartActiveMetadataProbe();
    } catch (error) {
      this.logger.warn({
        message: 'Kafka topic metadata refresh failed',
        reason: this.getFailureReason(error),
      });

      if (this.lastKnownTopics !== undefined) {
        this.logger.warn({
          message: 'Using last-known Kafka topic metadata',
          topicCount: this.lastKnownTopics.length,
        });
        return [...this.lastKnownTopics];
      }

      throw new KafkaMetadataException(
        'Unable to retrieve Kafka topic metadata',
        error,
      );
    }
  }

  /**
   * Returns a snapshot of the current Kafka connection and readiness state.
   *
   * Future health services use this method without receiving mutable internal
   * state or Kafka client references.
   *
   * @returns A new status object describing the current Kafka state.
   */
  getKafkaStatus(): KafkaStatus {
    return {
      state: this.state,
      connected: this.activeConnectionVerified,
      healthy: this.isHealthy(),
      ready: this.isReady(),
      initializationAttempts: this.initializationAttempts,
      reconnectAttempts: this.reconnectAttempts,
      lastFailureReason: this.lastFailureReason,
      lastSuccessfulMetadataRefreshTime: this.lastSuccessfulMetadataRefreshTime,
    };
  }

  /**
   * Indicates whether Kafka initialization succeeded and shutdown has not begun.
   *
   * Future publish and health services use this as the lightweight readiness
   * check before interacting with the producer.
   *
   * @returns `true` only while the shared Kafka layer is ready.
   */
  isReady(): boolean {
    return this.state === KafkaConnectionState.Ready && !this.shuttingDown;
  }

  /**
   * Indicates whether cached lifecycle state is healthy for the load balancer.
   *
   * Startup remains unhealthy until Kafka succeeds once, protecting rolling
   * deployments from replacing a working task with a bad configuration.
   * Runtime reconnecting remains healthy while its bounded task is active.
   *
   * @returns `true` when the service is ready or actively recovering.
   */
  isHealthy(): boolean {
    if (this.shuttingDown) {
      return false;
    }

    return (
      this.state === KafkaConnectionState.Ready ||
      (this.state === KafkaConnectionState.Reconnecting &&
        this.hasEverBeenReady &&
        this.reconnectionTask !== undefined)
    );
  }

  /**
   * Validates related Kafka timing and recovery values before startup.
   *
   * @returns Nothing when configuration is safe.
   * @throws Error for invalid timing relationships or retry values.
   */
  private validateConfiguration(): void {
    const positiveTimings = [
      ['connectionTimeout', KafkaConfig.connectionTimeout],
      ['brokerTimeout', KafkaConfig.brokerTimeout],
      ['requestTimeout', KafkaConfig.requestTimeout],
      ['metadataRefreshInterval', KafkaConfig.metadataRefreshInterval],
    ] as const;

    for (const [name, value] of positiveTimings) {
      if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error(`Kafka ${name} must be a positive integer`);
      }
    }

    if (KafkaConfig.brokerTimeout >= KafkaConfig.requestTimeout) {
      throw new Error('Kafka brokerTimeout must be less than requestTimeout');
    }

    if (KafkaConfig.metadataRefreshInterval >= KAFKA_BROKER_IDLE_TIMEOUT_MS) {
      throw new Error(
        `Kafka metadataRefreshInterval must be less than ${KAFKA_BROKER_IDLE_TIMEOUT_MS}`,
      );
    }

    if (
      !Number.isSafeInteger(KafkaConfig.retry.attempts) ||
      KafkaConfig.retry.attempts <= 0
    ) {
      throw new Error('Kafka retry attempts must be a positive integer');
    }

    for (const [name, value] of [
      ['initialRetryTime', KafkaConfig.retry.initialRetryTime],
      ['maxRetryTime', KafkaConfig.retry.maxRetryTime],
    ] as const) {
      if (!Number.isSafeInteger(value) || value < 0) {
        throw new Error(`Kafka ${name} must be a non-negative integer`);
      }
    }

    if (KafkaConfig.retry.initialRetryTime > KafkaConfig.retry.maxRetryTime) {
      throw new Error('Kafka initialRetryTime must not exceed maxRetryTime');
    }
  }

  /**
   * Maps the v6 Kafka configuration into Platformatic producer/admin options.
   *
   * Platformatic retries and stale-metadata send replay are disabled so the
   * application owns one visible, bounded fresh-client recovery budget.
   *
   * @returns Shared client options containing brokers and independent timeouts.
   * @throws Error when a configured SASL mechanism is unsupported.
   */
  private createClientOptions(): ProducerOptions<
    Buffer,
    Buffer,
    Buffer,
    Buffer
  > &
    AdminOptions {
    const options: ProducerOptions<Buffer, Buffer, Buffer, Buffer> &
      AdminOptions = {
      clientId: KafkaConfig.clientId,
      bootstrapBrokers: KafkaConfig.brokers,
      connectTimeout: KafkaConfig.connectionTimeout,
      requestTimeout: KafkaConfig.requestTimeout,
      timeout: KafkaConfig.brokerTimeout,
      retries: 0,
      repeatOnStaleMetadata: false,
    };

    if (KafkaConfig.tls.enabled) {
      options.tls = {
        rejectUnauthorized: KafkaConfig.tls.rejectUnauthorized,
      };
    }

    const sasl = this.createSaslOptions();
    if (sasl) {
      options.sasl = sasl;
    }

    return options;
  }

  /**
   * Converts the configured SASL mechanism and credentials for Platformatic.
   *
   * @returns Platformatic SASL options, or `undefined` when SASL is not set.
   * @throws Error when `KAFKA_SASL_MECHANISM` is not a supported mechanism.
   */
  private createSaslOptions(): SASLOptions | undefined {
    const configuredMechanism = KafkaConfig.sasl.mechanism;
    if (!configuredMechanism) {
      return undefined;
    }

    const mechanismMap: Record<string, SASLOptions['mechanism']> = {
      plain: 'PLAIN',
      'scram-sha-256': 'SCRAM-SHA-256',
      'scram-sha-512': 'SCRAM-SHA-512',
      oauthbearer: 'OAUTHBEARER',
      gssapi: 'GSSAPI',
    };
    const mechanism = mechanismMap[configuredMechanism.toLowerCase()];

    if (!mechanism) {
      throw new Error('Unsupported Kafka SASL mechanism');
    }

    return {
      mechanism,
      username: KafkaConfig.sasl.username,
      password: KafkaConfig.sasl.password,
    };
  }

  /**
   * Creates an isolated producer/admin candidate with safe error listeners.
   *
   * Candidate clients are not published into service state until both pass a
   * live metadata verification. Candidate error events are retained so a pair
   * cannot be marked ready after emitting a terminal error.
   *
   * @returns A promise resolving to a newly allocated, inactive client pair.
   */
  private async createClientSet(): Promise<KafkaClientSet> {
    let producer: BusProducer | undefined;
    let admin: Admin | undefined;

    try {
      producer = new Producer<Buffer, Buffer, Buffer, Buffer>(
        this.clientOptions,
      );
      producer.on('error', ignoreDetachedKafkaError);
      admin = new Admin(this.clientOptions);
      admin.on('error', ignoreDetachedKafkaError);

      const clients = {
        producer,
        admin,
        generation: (this.nextClientGeneration += 1),
      } as KafkaClientSet;
      const candidateErrorListener = (error: Error): void => {
        clients.candidateFailure = error;
      };
      const candidateDisconnectListener = (): void => {
        clients.candidateFailure ??= new Error(
          'Kafka candidate disconnected during verification',
        );
      };

      producer.removeListener('error', ignoreDetachedKafkaError);
      admin.removeListener('error', ignoreDetachedKafkaError);
      clients.producerListeners = {
        error: candidateErrorListener,
        brokerDisconnect: candidateDisconnectListener,
      };
      clients.adminListeners = {
        error: candidateErrorListener,
        brokerDisconnect: candidateDisconnectListener,
      };
      this.attachClientListeners(producer, clients.producerListeners);
      this.attachClientListeners(admin, clients.adminListeners);
      return clients;
    } catch (error) {
      await Promise.all([
        this.closeClient('producer candidate', producer),
        this.closeClient('admin candidate', admin),
      ]);
      throw error;
    }
  }

  /**
   * Atomically installs a verified candidate and attaches lifecycle listeners.
   *
   * @param clients Verified producer/admin candidate.
   * @param topics Topic names returned by the candidate's admin request.
   * @returns Nothing.
   */
  private activateClientSet(clients: KafkaClientSet, topics: string[]): void {
    this.detachClientListeners(clients, false);
    this.attachActiveClientListeners(clients);
    this.clients = clients;
    this.activeConnectionVerified = true;
    this.hasEverBeenReady = true;
    this.state = KafkaConnectionState.Ready;
    this.reconnectAttempts = 0;
    this.lastFailureReason = undefined;
    this.lastKnownTopics = [...topics];
    this.lastSuccessfulMetadataRefreshTime = new Date().toISOString();
    this.scheduleMetadataRefresh();
    this.logger.log({
      message: 'Kafka producer/admin client set activated',
      generation: clients.generation,
      topicCount: topics.length,
    });
  }

  /**
   * Attaches generation-fenced error and broker lifecycle listeners.
   *
   * @param clients Verified active client pair.
   * @returns Nothing.
   */
  private attachActiveClientListeners(clients: KafkaClientSet): void {
    clients.producerListeners = this.createActiveClientListeners(
      clients,
      'producer',
    );
    clients.adminListeners = this.createActiveClientListeners(clients, 'admin');

    this.attachClientListeners(clients.producer, clients.producerListeners);
    this.attachClientListeners(clients.admin, clients.adminListeners);
  }

  /**
   * Creates event listeners fenced to one active client generation.
   *
   * @param clients Client pair owning the listeners.
   * @param role Safe client role used in log context.
   * @returns Listener functions for one Platformatic client.
   */
  private createActiveClientListeners(
    clients: KafkaClientSet,
    role: KafkaClientRole,
  ): KafkaClientListeners {
    return {
      error: (error: Error): void => {
        this.handleKafkaFailure(`Kafka ${role} client error`, error, clients);
      },
      brokerDisconnect: (): void => {
        this.handleBrokerDisconnect(clients, role);
      },
      brokerFailed: (): void => {
        this.handleBrokerFailure(clients, role);
      },
    };
  }

  /**
   * Adds the service's listeners to one Platformatic client.
   *
   * @param client Producer or admin EventEmitter.
   * @param listeners Listener set to attach.
   * @returns Nothing.
   */
  private attachClientListeners(
    client: BusProducer | Admin,
    listeners: KafkaClientListeners,
  ): void {
    client.on('error', listeners.error);
    if (listeners.brokerDisconnect) {
      client.on('client:broker:disconnect', listeners.brokerDisconnect);
    }
    if (listeners.brokerFailed) {
      client.on('client:broker:failed', listeners.brokerFailed);
    }
  }

  /**
   * Removes service listeners before a client pair is replaced or closed.
   *
   * @param clients Client pair to detach.
   * @param keepErrorSafe Whether to leave no-op late-error listeners.
   * @returns Nothing.
   */
  private detachClientListeners(
    clients: KafkaClientSet,
    keepErrorSafe = true,
  ): void {
    this.detachSingleClientListeners(
      clients.producer,
      clients.producerListeners,
      keepErrorSafe,
    );
    this.detachSingleClientListeners(
      clients.admin,
      clients.adminListeners,
      keepErrorSafe,
    );
  }

  /**
   * Removes one listener set and optionally consumes late error events.
   *
   * @param client Producer or admin client.
   * @param listeners Listener set previously attached to the client.
   * @param keepErrorSafe Whether to attach the no-op error listener.
   * @returns Nothing.
   */
  private detachSingleClientListeners(
    client: BusProducer | Admin,
    listeners: KafkaClientListeners,
    keepErrorSafe: boolean,
  ): void {
    client.removeListener('error', listeners.error);
    if (listeners.brokerDisconnect) {
      client.removeListener(
        'client:broker:disconnect',
        listeners.brokerDisconnect,
      );
    }
    if (listeners.brokerFailed) {
      client.removeListener('client:broker:failed', listeners.brokerFailed);
    }
    if (keepErrorSafe) {
      client.on('error', ignoreDetachedKafkaError);
    }
  }

  /**
   * Performs a live producer probe and obtains the actual all-topic list.
   *
   * Platformatic 2.8 filters `metadata({ topics: [] })` to an empty topic map,
   * so the producer call is used only as a live probe and `Admin.listTopics()`
   * supplies the topic cache.
   *
   * @param clients Client pair to verify.
   * @returns Sorted topic names returned by the admin client.
   * @throws Propagates producer/admin metadata or candidate errors.
   */
  private async verifyClientSet(clients: KafkaClientSet): Promise<string[]> {
    const [, listedTopics] = await Promise.all([
      clients.producer.metadata({ topics: [], forceUpdate: true }),
      clients.admin.listTopics(),
    ]);

    if (clients.candidateFailure) {
      throw clients.candidateFailure;
    }

    return [...new Set(listedTopics)].sort((left, right) =>
      left.localeCompare(right),
    );
  }

  /**
   * Starts or joins the active generation's single-flight metadata probe.
   *
   * A stale probe is generation-fenced: it may settle for its caller, but it
   * cannot update readiness, topic cache, or trigger recovery for newer clients.
   *
   * @returns Fresh sorted topic names for the current client generation.
   * @throws Error when no ready clients exist or live verification fails.
   */
  private getOrStartActiveMetadataProbe(): Promise<string[]> {
    const clients = this.clients;
    if (!clients || !this.isReady()) {
      return Promise.reject(new Error('Kafka clients are not ready'));
    }

    if (this.activeMetadataProbe?.clients === clients) {
      return this.activeMetadataProbe.promise;
    }

    const promise = this.verifyClientSet(clients)
      .then((topics) => {
        if (this.clients === clients && this.isReady()) {
          this.recordSuccessfulMetadataProbe(clients, topics);
        }
        return [...topics];
      })
      .catch((error: unknown) => {
        this.handleKafkaFailure(
          'Kafka active metadata verification failed',
          error,
          clients,
        );
        throw error;
      })
      .finally(() => {
        if (this.activeMetadataProbe?.promise === promise) {
          this.activeMetadataProbe = undefined;
        }
        if (this.metadataProbeRequestedAfterCurrent === clients) {
          this.metadataProbeRequestedAfterCurrent = undefined;
          queueMicrotask(() => {
            if (this.clients === clients && this.isReady()) {
              void this.getOrStartActiveMetadataProbe().catch(() => undefined);
            }
          });
        }
      });
    this.activeMetadataProbe = { clients, promise };
    return promise;
  }

  /**
   * Records a successful current-generation metadata verification.
   *
   * @param clients Client pair that completed verification.
   * @param topics Fresh topic names.
   * @returns Nothing.
   */
  private recordSuccessfulMetadataProbe(
    clients: KafkaClientSet,
    topics: string[],
  ): void {
    if (this.clients !== clients || !this.isReady()) {
      return;
    }

    this.activeConnectionVerified = true;
    this.lastKnownTopics = [...topics];
    this.lastSuccessfulMetadataRefreshTime = new Date().toISOString();
    this.scheduleMetadataRefresh();
    this.logger.log({
      message: 'Kafka producer/admin metadata refreshed successfully',
      generation: clients.generation,
      topicCount: topics.length,
    });
  }

  /**
   * Schedules the next non-overlapping background metadata verification.
   *
   * @returns Nothing.
   */
  private scheduleMetadataRefresh(): void {
    this.clearMetadataRefreshTimer();
    if (!this.isReady() || !this.clients) {
      return;
    }

    const timer = setTimeout(() => {
      if (this.metadataRefreshTimer === timer) {
        this.metadataRefreshTimer = undefined;
      }
      void this.getOrStartActiveMetadataProbe().catch(() => undefined);
    }, KafkaConfig.metadataRefreshInterval);
    timer.unref();
    this.metadataRefreshTimer = timer;
  }

  /**
   * Cancels the pending background metadata timer, when present.
   *
   * @returns Nothing.
   */
  private clearMetadataRefreshTimer(): void {
    if (this.metadataRefreshTimer) {
      clearTimeout(this.metadataRefreshTimer);
      this.metadataRefreshTimer = undefined;
    }
  }

  /**
   * Reacts to a broker disconnect with an immediate deduplicated live probe.
   *
   * Platformatic removes the socket after forwarding the event, so the probe is
   * queued to the next microtask. A disconnect alone is not terminal.
   *
   * @param clients Client pair that emitted the event.
   * @param role Safe client role used in diagnostic logs.
   * @returns Nothing.
   */
  private handleBrokerDisconnect(
    clients: KafkaClientSet,
    role: KafkaClientRole,
  ): void {
    if (this.clients !== clients || !this.isReady()) {
      return;
    }

    this.activeConnectionVerified = false;
    this.logger.warn({
      message: 'Kafka broker connection disconnected; verifying clients',
      client: role,
      generation: clients.generation,
    });

    if (this.activeMetadataProbe?.clients === clients) {
      this.metadataProbeRequestedAfterCurrent = clients;
      return;
    }

    queueMicrotask(() => {
      if (this.clients === clients && this.isReady()) {
        void this.getOrStartActiveMetadataProbe().catch(() => undefined);
      }
    });
  }

  /**
   * Logs an individual broker connection failure without rebuilding clients.
   *
   * One bootstrap broker can fail while another succeeds; the active Kafka
   * operation or follow-up metadata probe determines whole-client availability.
   *
   * @param clients Client pair that emitted the event.
   * @param role Safe client role used in diagnostic logs.
   * @returns Nothing.
   */
  private handleBrokerFailure(
    clients: KafkaClientSet,
    role: KafkaClientRole,
  ): void {
    if (this.clients !== clients || this.shuttingDown) {
      return;
    }

    this.logger.warn({
      message: 'Kafka broker connection attempt failed',
      client: role,
      generation: clients.generation,
    });
  }

  /**
   * Moves a current-generation terminal failure into bounded recovery.
   *
   * @param context Safe diagnostic context.
   * @param error Failure that caused recovery.
   * @param clients Expected active client pair, when generation fencing applies.
   * @returns Nothing; recovery runs asynchronously.
   */
  private handleKafkaFailure(
    context: string,
    error: unknown,
    clients?: KafkaClientSet,
  ): void {
    if (
      this.shuttingDown ||
      this.state === KafkaConnectionState.Failed ||
      (clients !== undefined && this.clients !== clients)
    ) {
      return;
    }

    const wasReady = this.state === KafkaConnectionState.Ready;
    this.activeConnectionVerified = false;
    this.lastFailureReason = this.getFailureReason(error);
    this.clearMetadataRefreshTimer();
    this.state = KafkaConnectionState.Reconnecting;
    this.logger.error(context, this.getErrorTrace(error));
    void this.scheduleReconnect(wasReady);
  }

  /**
   * Starts or joins the single shared Kafka recovery task.
   *
   * A failure from a newly activated generation during the current task's final
   * microtask is queued so the service cannot remain Reconnecting without work.
   *
   * @param queueAfterCurrent Whether a newly activated client needs another run.
   * @returns The active bounded recovery task.
   */
  private scheduleReconnect(queueAfterCurrent = false): Promise<void> {
    if (this.shuttingDown) {
      return Promise.resolve();
    }

    if (this.reconnectionTask) {
      if (queueAfterCurrent) {
        this.reconnectRequested = true;
      }
      return this.reconnectionTask;
    }

    const task = this.performReconnect().finally(() => {
      if (this.reconnectionTask === task) {
        this.reconnectionTask = undefined;
      }

      const reconnectRequested = this.reconnectRequested;
      this.reconnectRequested = false;
      if (
        reconnectRequested &&
        !this.shuttingDown &&
        this.state === KafkaConnectionState.Reconnecting
      ) {
        void this.scheduleReconnect();
      }
    });
    this.reconnectionTask = task;
    return task;
  }

  /**
   * Recreates and verifies Kafka clients within the configured attempt budget.
   *
   * Each attempt uses a fresh local pair and one Platformatic request attempt.
   * Only a fully verified candidate is installed into service state.
   *
   * @returns A promise that settles after success, exhaustion, or shutdown.
   */
  private async performReconnect(): Promise<void> {
    const runtimeRecovery = this.hasEverBeenReady;
    const previousClients = this.clients;
    this.clients = undefined;
    this.activeConnectionVerified = false;
    this.clearMetadataRefreshTimer();
    await this.closeClientSet(previousClients);

    for (let attempt = 1; attempt <= KafkaConfig.retry.attempts; attempt += 1) {
      if (this.shuttingDown) {
        return;
      }

      this.reconnectAttempts = attempt;
      if (attempt > 1 || runtimeRecovery) {
        const delayAttempt = runtimeRecovery ? attempt : attempt - 1;
        await this.waitForReconnectDelay(
          this.calculateReconnectDelay(delayAttempt),
        );
      }
      if (this.shuttingDown) {
        return;
      }

      this.initializationAttempts += 1;
      this.logger.log({
        message: 'Kafka producer/admin recovery attempt started',
        attempt,
        maxAttempts: KafkaConfig.retry.attempts,
      });

      let candidate: KafkaClientSet | undefined;
      try {
        candidate = await this.createClientSet();
        if (this.shuttingDown) {
          await this.closeClientSet(candidate);
          return;
        }
        this.recoveryCandidate = candidate;
        const topics = await this.verifyClientSet(candidate);

        if (this.shuttingDown) {
          await this.closeClientSet(candidate);
          return;
        }
        if (candidate.candidateFailure) {
          throw candidate.candidateFailure;
        }

        this.recoveryCandidate = undefined;
        this.activateClientSet(candidate, topics);
        this.logger.log({
          message: 'Kafka producer/admin recovery completed',
          attempt,
        });
        return;
      } catch (error) {
        if (this.recoveryCandidate === candidate) {
          this.recoveryCandidate = undefined;
        }
        await this.closeClientSet(candidate);
        this.lastFailureReason = this.getFailureReason(error);
        this.logger.error(
          {
            message: 'Kafka producer/admin recovery attempt failed',
            attempt,
            maxAttempts: KafkaConfig.retry.attempts,
          },
          this.getErrorTrace(error),
        );

        if (!this.shuttingDown && attempt < KafkaConfig.retry.attempts) {
          this.state = KafkaConnectionState.Reconnecting;
        }
      }
    }

    if (!this.shuttingDown) {
      this.reconnectAttempts = KafkaConfig.retry.attempts;
      this.state = KafkaConnectionState.Failed;
      this.logger.error(
        'Kafka recovery attempts exhausted',
        this.lastFailureReason ?? 'Kafka recovery attempts exhausted',
      );
    }
  }

  /**
   * Calculates capped equal-jitter delay for a fresh-client retry.
   *
   * @param attempt One-based retry delay index.
   * @returns Delay between half and all of the capped exponential value.
   */
  private calculateReconnectDelay(attempt: number): number {
    const exponent = Math.max(attempt - 1, 0);
    const exponential = Math.min(
      KafkaConfig.retry.initialRetryTime * Math.pow(2, exponent),
      KafkaConfig.retry.maxRetryTime,
    );
    const minimum = Math.floor(exponential / 2);
    return minimum + Math.floor(Math.random() * (exponential - minimum + 1));
  }

  /**
   * Waits for a reconnect delay that shutdown can cancel immediately.
   *
   * @param delayMs Delay in milliseconds.
   * @returns A promise resolved by the timer or shutdown cancellation.
   */
  private async waitForReconnectDelay(delayMs: number): Promise<void> {
    if (delayMs <= 0 || this.shuttingDown) {
      return;
    }

    await new Promise<void>((resolve) => {
      const finish = (): void => {
        if (this.reconnectDelayTimer === timer) {
          this.reconnectDelayTimer = undefined;
          this.resolveReconnectDelay = undefined;
        }
        resolve();
      };
      const timer = setTimeout(finish, delayMs);
      timer.unref();
      this.reconnectDelayTimer = timer;
      this.resolveReconnectDelay = () => {
        clearTimeout(timer);
        finish();
      };
    });
  }

  /**
   * Cancels an active reconnect backoff during shutdown.
   *
   * @returns Nothing.
   */
  private cancelReconnectDelay(): void {
    this.resolveReconnectDelay?.();
  }

  /**
   * Waits a bounded time for Kafka shutdown bookkeeping to settle.
   *
   * All input promises retain rejection handlers after the deadline, while the
   * Nest shutdown hook can still finish inside ECS's process stop window.
   *
   * @param tasks Probe, recovery, and close promises active at shutdown.
   * @returns A promise resolved when all tasks settle or the deadline expires.
   */
  private async settleShutdownTasks(tasks: Promise<unknown>[]): Promise<void> {
    let timeout: NodeJS.Timeout | undefined;
    const deadline = new Promise<'timeout'>((resolve) => {
      timeout = setTimeout(
        () => resolve('timeout'),
        KAFKA_SHUTDOWN_SETTLE_TIMEOUT_MS,
      );
      timeout.unref();
    });
    const completion = Promise.allSettled(tasks).then(() => 'settled' as const);
    const result = await Promise.race([completion, deadline]);

    if (timeout) {
      clearTimeout(timeout);
    }
    if (result === 'timeout') {
      this.logger.warn({
        message: 'Kafka shutdown task deadline reached',
        timeoutMs: KAFKA_SHUTDOWN_SETTLE_TIMEOUT_MS,
      });
    }
  }

  /**
   * Serializes the complete event object as a Kafka message value.
   *
   * @param event Complete submitted bus event.
   * @returns UTF-8 JSON buffer containing every serializable event field.
   * @throws Error when JSON serialization fails or returns no value.
   */
  private serializeEvent(event: KafkaBusEvent): Buffer {
    const serialized = JSON.stringify(event);
    if (serialized === undefined) {
      throw new Error('Kafka event could not be serialized');
    }
    return Buffer.from(serialized);
  }

  /**
   * Builds the exact buffer-backed Kafka headers required for bus events.
   *
   * @param event Event supplying originator, MIME type, timestamp, and topic.
   * @returns A map containing exactly the four required Kafka headers.
   */
  private createHeaders(event: KafkaBusEvent): Map<Buffer, Buffer> {
    return new Map<Buffer, Buffer>([
      [Buffer.from('originator'), Buffer.from(event.originator)],
      [Buffer.from('mime-type'), Buffer.from(event['mime-type'])],
      [Buffer.from('timestamp'), Buffer.from(String(event.timestamp))],
      [Buffer.from('topic'), Buffer.from(event.topic)],
    ]);
  }

  /**
   * Converts a supported event key into its Kafka buffer representation.
   *
   * @param value Submitted string or buffer key.
   * @returns The original buffer or a UTF-8 buffer for a string key.
   */
  private toBuffer(value: string | Buffer): Buffer {
    return Buffer.isBuffer(value) ? value : Buffer.from(value);
  }

  /**
   * Finds error information returned in a resolved Kafka send result.
   *
   * Platformatic normally rejects failed sends, but this also handles response
   * shapes containing error fields/codes or unwritable broker nodes.
   *
   * @param result Resolved Platformatic producer result.
   * @returns An error retaining the first failure signal, or `undefined` for a
   * successful result.
   */
  private findSendResultFailure(result: ProduceResult): Error | undefined {
    if (result.unwritableNodes && result.unwritableNodes.length > 0) {
      return new Error('Kafka send reported unwritable broker nodes');
    }

    const signal = this.findNestedErrorSignal(result, new Set<unknown>(), 0);
    if (signal === undefined) {
      return undefined;
    }
    if (signal instanceof Error) {
      return signal;
    }

    const error = new Error('Kafka send result reported an error') as Error & {
      cause?: unknown;
    };
    error.cause = signal;
    return error;
  }

  /**
   * Recursively inspects bounded Kafka response structures for error signals.
   *
   * @param value Current response value to inspect.
   * @param visited Previously inspected objects, used to avoid cycles.
   * @param depth Current traversal depth, bounded to protect publish latency.
   * @returns The first meaningful error value/object, or `undefined`.
   */
  private findNestedErrorSignal(
    value: unknown,
    visited: Set<unknown>,
    depth: number,
  ): unknown {
    if (depth > 6 || value === null || value === undefined) {
      return undefined;
    }
    if (value instanceof Error) {
      return value;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        const failure = this.findNestedErrorSignal(item, visited, depth + 1);
        if (failure !== undefined) {
          return failure;
        }
      }
      return undefined;
    }
    if (value instanceof Map) {
      if (visited.has(value)) {
        return undefined;
      }

      visited.add(value);
      for (const [key, nestedValue] of value) {
        const keyFailure = this.findNestedErrorSignal(key, visited, depth + 1);
        if (keyFailure !== undefined) {
          return keyFailure;
        }

        const valueFailure = this.findNestedErrorSignal(
          nestedValue,
          visited,
          depth + 1,
        );
        if (valueFailure !== undefined) {
          return valueFailure;
        }
      }
      return undefined;
    }
    if (typeof value !== 'object' || visited.has(value)) {
      return undefined;
    }

    visited.add(value);
    const record = value as Record<string, unknown>;
    const directError = record.error ?? record.errors;
    if (this.hasMeaningfulErrorValue(directError)) {
      return directError;
    }
    if (this.hasFailureCode(record.errorCode)) {
      return record;
    }

    for (const nestedKey of ['topics', 'partitions', 'results', 'responses']) {
      const failure = this.findNestedErrorSignal(
        record[nestedKey],
        visited,
        depth + 1,
      );
      if (failure !== undefined) {
        return failure;
      }
    }
    return undefined;
  }

  /**
   * Determines whether a direct error field contains a meaningful failure.
   *
   * @param value Error field value returned by Kafka.
   * @returns `true` for non-empty/non-zero failure values.
   */
  private hasMeaningfulErrorValue(value: unknown): boolean {
    if (
      value === undefined ||
      value === null ||
      value === false ||
      value === 0
    ) {
      return false;
    }
    if (Array.isArray(value)) {
      return value.length > 0;
    }
    return value !== '';
  }

  /**
   * Determines whether a Kafka error code represents a failure.
   *
   * @param value Numeric or string Kafka error code.
   * @returns `true` when the code is present and not a no-error value.
   */
  private hasFailureCode(value: unknown): boolean {
    if (typeof value === 'number') {
      return value !== 0;
    }
    if (typeof value === 'string') {
      const normalized = value.toLowerCase().replace(/[^a-z0-9]/g, '');
      return (
        normalized !== '' && normalized !== '0' && normalized !== 'noerror'
      );
    }
    return false;
  }

  /**
   * Maps a publish failure to the Kafka layer's client/server classifications.
   *
   * @param topic Submitted event topic used in legacy-compatible messages.
   * @param cause Thrown error or failed send-result value.
   * @returns A classified Kafka publish exception retaining the original cause.
   */
  private classifyPublishFailure(
    topic: string,
    cause: unknown,
  ): KafkaPublishException {
    if (this.isUnknownTopicFailure(cause)) {
      return new KafkaPublishException(
        `Unknown event type "${topic}"`,
        KafkaPublishErrorKind.UnknownTopic,
        KafkaFailureClassification.BadRequest,
        cause,
      );
    }

    return new KafkaPublishException(
      'Kafka failed to publish the event',
      KafkaPublishErrorKind.KafkaFailure,
      KafkaFailureClassification.ServerError,
      cause,
    );
  }

  /**
   * Detects Kafka unknown-topic variants across errors and response objects.
   *
   * @param value Thrown value or send-result error signal to classify.
   * @returns `true` for names, codes, or messages equivalent to
   * `UnknownTopicOrPartition`.
   */
  private isUnknownTopicFailure(value: unknown): boolean {
    return this.containsUnknownTopicSignal(value, new Set<unknown>(), 0);
  }

  /**
   * Recursively searches a bounded error/cause structure for unknown-topic data.
   *
   * @param value Current failure value to inspect.
   * @param visited Previously inspected objects, used to avoid cycles.
   * @param depth Current traversal depth, bounded to avoid pathological objects.
   * @returns `true` when an unknown-topic signal is found.
   */
  private containsUnknownTopicSignal(
    value: unknown,
    visited: Set<unknown>,
    depth: number,
  ): boolean {
    if (depth > 6 || value === null || value === undefined) {
      return false;
    }
    if (typeof value === 'number') {
      return value === 3;
    }
    if (typeof value === 'string') {
      return this.isUnknownTopicText(value);
    }
    if (Array.isArray(value)) {
      return value.some((item) =>
        this.containsUnknownTopicSignal(item, visited, depth + 1),
      );
    }
    if (value instanceof Map) {
      if (visited.has(value)) {
        return false;
      }

      visited.add(value);
      for (const [key, nestedValue] of value) {
        if (
          this.containsUnknownTopicSignal(key, visited, depth + 1) ||
          this.containsUnknownTopicSignal(nestedValue, visited, depth + 1)
        ) {
          return true;
        }
      }
      return false;
    }
    if (typeof value !== 'object' || visited.has(value)) {
      return false;
    }

    visited.add(value);
    const record = value as Record<string, unknown>;
    for (const key of [
      'name',
      'code',
      'errorCode',
      'type',
      'message',
      'error',
      'errors',
      'cause',
    ]) {
      if (this.containsUnknownTopicSignal(record[key], visited, depth + 1)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Matches normalized Kafka unknown-topic names and legacy broker messages.
   *
   * @param value Error name, code, type, or message text.
   * @returns `true` when the text indicates an unknown topic or partition.
   */
  private isUnknownTopicText(value: string): boolean {
    const normalized = value.toLowerCase().replace(/[^a-z0-9]/g, '');
    return (
      normalized === '3' ||
      normalized.includes('unknowntopicorpartition') ||
      normalized.includes('unknowntopic') ||
      normalized.includes('unknowneventtype') ||
      normalized.includes('serverdoesnothostthistopicpartition')
    );
  }

  /**
   * Detaches and closes one producer/admin pair exactly once.
   *
   * Both closes run even when one rejects, and detached clients retain a no-op
   * error listener so late EventEmitter errors cannot terminate the process.
   *
   * @param clients Client pair to close, when one exists.
   * @returns `true` when both clients close successfully.
   */
  private closeClientSet(clients?: KafkaClientSet): Promise<boolean> {
    if (!clients) {
      return Promise.resolve(true);
    }
    if (clients.closePromise) {
      return clients.closePromise;
    }

    this.detachClientListeners(clients);
    clients.closePromise = Promise.all([
      this.closeClient('producer', clients.producer),
      this.closeClient('admin', clients.admin),
    ]).then(([producerClosed, adminClosed]) => producerClosed && adminClosed);
    return clients.closePromise;
  }

  /**
   * Closes one Kafka client while allowing the remaining shutdown work to run.
   *
   * @param clientName Safe client label used in contextual logs.
   * @param client Producer or admin client to close, when initialized.
   * @returns `true` when no client exists or close succeeds; otherwise `false`.
   */
  private async closeClient(
    clientName: string,
    client?: ClosableKafkaClient,
  ): Promise<boolean> {
    if (!client) {
      return true;
    }

    let timeout: NodeJS.Timeout | undefined;
    let deadlineReached = false;
    const close = Promise.resolve()
      .then(async () => {
        let lastError: unknown;
        for (let attempt = 1; attempt <= 2; attempt += 1) {
          try {
            await client.close();
            return;
          } catch (error) {
            lastError = error;
            if (attempt === 1) {
              this.logger.warn({
                message: `Kafka ${clientName} close failed; retrying once`,
                reason: this.getFailureReason(error),
              });
            }
          }
        }
        throw lastError;
      })
      .then(() => {
        if (!deadlineReached) {
          this.logger.log(`Kafka ${clientName} closed successfully`);
        }
        return true;
      })
      .catch((error: unknown) => {
        this.logger.error(
          `Kafka ${clientName} close failed`,
          this.getErrorTrace(error),
        );
        return false;
      });
    const deadline = new Promise<boolean>((resolve) => {
      timeout = setTimeout(() => {
        deadlineReached = true;
        this.logger.error(
          `Kafka ${clientName} close timed out`,
          `Close did not settle within ${KAFKA_CLIENT_CLOSE_TIMEOUT_MS}ms`,
        );
        resolve(false);
      }, KAFKA_CLIENT_CLOSE_TIMEOUT_MS);
      timeout.unref();
    });

    const closed = await Promise.race([close, deadline]);
    if (timeout) {
      clearTimeout(timeout);
    }
    return closed;
  }

  /**
   * Produces a concise reason string for readiness and warning messages.
   *
   * @param error Unknown failure value received from Kafka or serialization.
   * @returns Error name/message or a safe string representation.
   */
  private getFailureReason(error: unknown): string {
    if (error instanceof Error) {
      return `${error.name}: ${error.message}`;
    }
    return String(error);
  }

  /**
   * Produces diagnostic error detail for the redacting application logger.
   *
   * @param error Unknown failure value received from Kafka or serialization.
   * @returns Stack trace when available, otherwise a concise reason string.
   */
  private getErrorTrace(error: unknown): string {
    return error instanceof Error
      ? (error.stack ?? error.message)
      : this.getFailureReason(error);
  }
}
