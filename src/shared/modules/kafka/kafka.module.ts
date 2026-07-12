import { Module } from '@nestjs/common';
import { KafkaProducerService } from './kafka-producer.service';

/**
 * Registers and exports the shared producer-focused Kafka integration.
 *
 * Importing modules can inject `KafkaProducerService` for publishing, topic
 * metadata, and readiness state without creating additional Kafka clients.
 */
@Module({
  providers: [KafkaProducerService],
  exports: [KafkaProducerService],
})
export class KafkaModule {}
