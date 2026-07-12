import {
  KafkaFailureClassification,
  KafkaMetadataErrorKind,
  KafkaPublishErrorKind,
} from './kafka.types';

/**
 * Represents a classified failure while publishing an event to Kafka.
 *
 * Future HTTP services can inspect `classification` without coupling this
 * infrastructure layer to Nest HTTP exceptions.
 */
export class KafkaPublishException extends Error {
  /**
   * Creates a classified Kafka publish exception.
   *
   * @param message Safe failure message intended for upstream handling.
   * @param kind Specific publish failure kind detected by the Kafka layer.
   * @param classification Client or server classification for future mapping.
   * @param cause Original thrown value or failed send result, when available.
   */
  constructor(
    message: string,
    public readonly kind: KafkaPublishErrorKind,
    public readonly classification: KafkaFailureClassification,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = KafkaPublishException.name;
  }
}

/**
 * Represents a server-classified failure to retrieve Kafka topic metadata.
 *
 * The original cause remains available for safe logging and future handling.
 */
export class KafkaMetadataException extends Error {
  public readonly kind = KafkaMetadataErrorKind.KafkaFailure;
  public readonly classification = KafkaFailureClassification.ServerError;

  /**
   * Creates a Kafka metadata exception.
   *
   * @param message Safe failure message intended for upstream handling.
   * @param cause Original metadata failure, when available.
   */
  constructor(
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = KafkaMetadataException.name;
  }
}
