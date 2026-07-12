import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  ServiceUnavailableException,
} from '@nestjs/common';
import {
  KafkaMetadataException,
  KafkaPublishException,
} from '../../shared/modules/kafka/kafka.errors';
import { KafkaProducerService } from '../../shared/modules/kafka/kafka-producer.service';
import {
  KafkaFailureClassification,
  KafkaPublishErrorKind,
} from '../../shared/modules/kafka/kafka.types';
import { HealthResponseDto } from './dto/health-response.dto';
import { EventPayloadValidatorService } from './event-payload-validator.service';

/** Coordinates event validation, Kafka operations, and HTTP error mapping. */
@Injectable()
export class BusService {
  /**
   * Creates the Bus application service.
   *
   * @param kafkaProducer Shared Kafka publishing and metadata layer.
   * @param eventValidator Non-mutating legacy event validator.
   */
  constructor(
    private readonly kafkaProducer: KafkaProducerService,
    private readonly eventValidator: EventPayloadValidatorService,
  ) {}

  /**
   * Validates and publishes an event while preserving its complete body.
   *
   * @param body Raw submitted HTTP JSON body.
   * @returns A promise that resolves after Kafka accepts the event.
   * @throws BadRequestException for invalid events or unknown Kafka topics.
   * @throws InternalServerErrorException for other Kafka publish failures.
   */
  async publishEvent(body: unknown): Promise<void> {
    const event = this.eventValidator.validate(body);

    try {
      await this.kafkaProducer.publishEvent(event);
    } catch (error) {
      if (
        error instanceof KafkaPublishException &&
        (error.classification === KafkaFailureClassification.BadRequest ||
          error.kind === KafkaPublishErrorKind.UnknownTopic)
      ) {
        throw new BadRequestException({
          message: `Unknown event type "${event.topic}"`,
        });
      }

      throw new InternalServerErrorException({
        message: 'Unable to publish event',
      });
    }
  }

  /**
   * Retrieves fresh or Kafka-layer fallback topic metadata.
   *
   * @returns A promise resolving to available Kafka topic names.
   * @throws InternalServerErrorException when Kafka has no usable metadata.
   */
  async listTopics(): Promise<string[]> {
    try {
      return await this.kafkaProducer.listTopics();
    } catch (error) {
      if (error instanceof KafkaMetadataException) {
        throw new InternalServerErrorException({
          message: 'Unable to retrieve Kafka topics',
        });
      }

      throw new InternalServerErrorException({
        message: 'Unable to retrieve Kafka topics',
      });
    }
  }

  /**
   * Reports health only when the Kafka layer is both ready and connected.
   *
   * @returns The exact successful health response.
   * @throws ServiceUnavailableException while Kafka is unavailable.
   */
  getHealth(): HealthResponseDto {
    const status = this.kafkaProducer.getKafkaStatus();
    if (!status.ready || !status.connected) {
      throw new ServiceUnavailableException({
        message: 'Kafka is not ready or connected',
      });
    }

    return { health: 'ok' };
  }
}
