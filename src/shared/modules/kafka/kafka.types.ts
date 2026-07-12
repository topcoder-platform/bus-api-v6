/** Lifecycle states reported by the shared Kafka producer layer. */
export enum KafkaConnectionState {
  Initializing = 'initializing',
  Ready = 'ready',
  Failed = 'failed',
  ShuttingDown = 'shutting-down',
  Stopped = 'stopped',
}

/** Failure classifications exposed for future HTTP-layer status mapping. */
export enum KafkaFailureClassification {
  BadRequest = 'bad-request',
  ServerError = 'server-error',
}

/** Publish failure kinds identified by the Kafka layer. */
export enum KafkaPublishErrorKind {
  UnknownTopic = 'unknown-topic',
  KafkaFailure = 'kafka-failure',
}

/** Metadata failure kinds identified by the Kafka layer. */
export enum KafkaMetadataErrorKind {
  KafkaFailure = 'kafka-failure',
}

/**
 * Internal event shape accepted by the shared Kafka publisher.
 *
 * Additional fields are intentionally allowed so the complete submitted v5
 * event body can be serialized without dropping caller-provided properties.
 */
export interface KafkaBusEvent {
  topic: string;
  originator: string;
  timestamp: string | number;
  'mime-type': string;
  payload: unknown;
  key?: string | Buffer;
  [field: string]: unknown;
}

/** Readiness and connection information exposed for future health checks. */
export interface KafkaStatus {
  state: KafkaConnectionState;
  connected: boolean;
  ready: boolean;
  initializationAttempts: number;
  lastFailureReason?: string;
  lastSuccessfulMetadataRefreshTime?: string;
}
