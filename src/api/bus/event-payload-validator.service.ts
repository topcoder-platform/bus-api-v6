import { BadRequestException, Injectable } from '@nestjs/common';
import { KafkaBusEvent } from '../../shared/modules/kafka/kafka.types';

const TOPIC_PATTERN = /^([a-zA-Z0-9]+\.)+[a-zA-Z0-9]+$/;
const REQUIRED_FIELDS = [
  'topic',
  'originator',
  'timestamp',
  'mime-type',
  'payload',
] as const;

/** Validates the legacy event contract without changing the submitted body. */
@Injectable()
export class EventPayloadValidatorService {
  /**
   * Checks required own properties, string fields, topic syntax, and key type.
   *
   * The validated object is returned by reference, preserving all additional
   * fields and the complete original shape for Kafka serialization.
   *
   * @param body Raw JSON body received by the events controller.
   * @returns The same body reference typed as a Kafka bus event.
   * @throws BadRequestException when the body violates the legacy contract.
   */
  validate(body: unknown): KafkaBusEvent {
    if (body === null || typeof body !== 'object' || Array.isArray(body)) {
      this.badRequest('Event body must be an object');
    }

    const event = body as Record<string, unknown>;
    for (const field of REQUIRED_FIELDS) {
      if (!Object.prototype.hasOwnProperty.call(event, field)) {
        this.badRequest(`"${field}" is required`);
      }
    }

    if (typeof event.topic !== 'string' || !TOPIC_PATTERN.test(event.topic)) {
      this.badRequest(
        '"topic" must be a fully qualified name - dot separated string',
      );
    }

    for (const field of ['originator', 'timestamp', 'mime-type'] as const) {
      if (typeof event[field] !== 'string' || event[field].length === 0) {
        this.badRequest(`"${field}" must be a non-empty string`);
      }
    }

    if (
      Object.prototype.hasOwnProperty.call(event, 'key') &&
      (typeof event.key !== 'string' || event.key.length === 0)
    ) {
      this.badRequest('"key" must be a non-empty string');
    }

    return event as KafkaBusEvent;
  }

  /**
   * Raises a consistently shaped legacy-style validation error.
   *
   * @param message Explanation returned to the API caller.
   * @returns Never returns because it always throws.
   * @throws BadRequestException on every invocation.
   */
  private badRequest(message: string): never {
    throw new BadRequestException({ message });
  }
}
