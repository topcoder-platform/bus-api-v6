import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import {
  Admin,
  type AdminOptions,
  type ClusterMetadata,
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
  private producer?: Producer<Buffer, Buffer, Buffer, Buffer>;
  private admin?: Admin;
  private initializationPromise?: Promise<void>;
  private state = KafkaConnectionState.Initializing;
  private initializationAttempts = 0;
  private lastFailureReason?: string;
  private lastSuccessfulMetadataRefreshTime?: string;
  private lastKnownTopics?: string[];
  private shuttingDown = false;

  /**
   * Initializes one producer and one admin client and verifies Kafka metadata.
   *
   * Nest invokes this method during module startup. Initialization failures are
   * recorded in readiness state and logged so future health handling can report
   * the unavailable dependency without exposing credentials.
   *
   * @returns A promise that resolves when the startup attempt has settled.
   */
  async onModuleInit(): Promise<void> {
    this.initializationPromise = this.initializeKafka();

    try {
      await this.initializationPromise;
    } catch (error) {
      this.state = KafkaConnectionState.Failed;
      this.lastFailureReason = this.getFailureReason(error);
      this.logger.error(
        'Kafka producer/admin initialization failed',
        this.getErrorTrace(error),
      );
    }
  }

  /**
   * Stops Kafka activity and closes the producer and admin clients defensively.
   *
   * Nest invokes this method for application shutdown hooks. Any in-flight
   * initialization is allowed to settle before both clients are closed.
   * Shutdown failures are logged and do not prevent the other client closing.
   *
   * @param signal Optional process signal supplied by Nest during shutdown.
   * @returns A promise that resolves after all shutdown work has settled.
   */
  async onApplicationShutdown(signal?: string): Promise<void> {
    this.shuttingDown = true;
    this.state = KafkaConnectionState.ShuttingDown;
    this.logger.log({
      message: 'Kafka shutdown started',
      signal: signal ?? 'application-close',
    });

    if (this.initializationPromise) {
      try {
        await this.initializationPromise;
      } catch {
        this.logger.warn(
          'Kafka initialization had failed before shutdown; closing available clients',
        );
      }
    }

    const producerClosed = await this.closeClient('producer', this.producer);
    const adminClosed = await this.closeClient('admin', this.admin);
    this.producer = undefined;
    this.admin = undefined;
    this.state = KafkaConnectionState.Stopped;

    if (producerClosed && adminClosed) {
      this.logger.log('Kafka shutdown completed successfully');
      return;
    }

    this.logger.warn('Kafka shutdown completed with client close failures');
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
    try {
      if (!this.isReady() || !this.producer) {
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

      const result = await this.producer.send({ messages: [message] });
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
      return await this.fetchTopicMetadata();
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
      connected: Boolean(
        this.producer?.isConnected() && this.admin?.isConnected(),
      ),
      ready: this.isReady(),
      initializationAttempts: this.initializationAttempts,
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
   * Creates and connects both Kafka clients, then verifies live metadata.
   *
   * @returns A promise that resolves after the producer connection and metadata
   * check succeed.
   * @throws Propagates Kafka client construction, connection, or metadata errors.
   */
  private async initializeKafka(): Promise<void> {
    this.initializationAttempts += 1;
    this.state = KafkaConnectionState.Initializing;
    this.logger.log({
      message: 'Kafka producer/admin initialization started',
      attempt: this.initializationAttempts,
    });

    const options = this.createClientOptions();
    this.producer = new Producer<Buffer, Buffer, Buffer, Buffer>(options);
    this.admin = new Admin(options);

    await this.producer.connectToBrokers(null);
    await this.fetchTopicMetadata();

    if (this.shuttingDown) {
      return;
    }

    this.state = KafkaConnectionState.Ready;
    this.lastFailureReason = undefined;
    this.logger.log('Kafka producer/admin initialization completed');
  }

  /**
   * Maps the v6 Kafka configuration into Platformatic producer/admin options.
   *
   * @returns Shared client options containing brokers, timeouts, capped retry
   * delay behavior, and configured TLS/SASL settings.
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
      timeout: KafkaConfig.requestTimeout,
      retries: KafkaConfig.retry.attempts,
      retryDelay: (_client, _operationId, attempt) =>
        this.calculateRetryDelay(attempt),
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
   * Calculates exponential retry delay capped by the configured maximum.
   *
   * @param attempt One-based retry attempt supplied by Platformatic.
   * @returns Retry delay in milliseconds.
   */
  private calculateRetryDelay(attempt: number): number {
    const exponent = Math.max(attempt - 1, 0);
    const delay = KafkaConfig.retry.initialRetryTime * Math.pow(2, exponent);
    return Math.min(delay, KafkaConfig.retry.maxRetryTime);
  }

  /**
   * Forces a live query for all topic metadata and updates the last-known
   * successful snapshot only after that broker request succeeds.
   *
   * @returns A defensive copy of normalized, sorted topic names.
   * @throws Error when the admin client is unavailable or the query fails.
   */
  private async fetchTopicMetadata(): Promise<string[]> {
    if (!this.admin) {
      throw new Error('Kafka admin client is not initialized');
    }

    const metadata: ClusterMetadata = await this.admin.metadata({
      topics: [],
      forceUpdate: true,
    });
    const topics = [...metadata.topics.keys()].sort((left, right) =>
      left.localeCompare(right),
    );
    this.lastKnownTopics = [...topics];
    this.lastSuccessfulMetadataRefreshTime = new Date().toISOString();
    this.logger.log({
      message: 'Kafka topic metadata refreshed successfully',
      topicCount: topics.length,
    });
    return [...topics];
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

    try {
      await client.close();
      this.logger.log(`Kafka ${clientName} closed successfully`);
      return true;
    } catch (error) {
      this.logger.error(
        `Kafka ${clientName} close failed`,
        this.getErrorTrace(error),
      );
      return false;
    }
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
