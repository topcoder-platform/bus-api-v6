import { ServiceUnavailableException } from '@nestjs/common';
import { BusService } from '../src/api/bus/bus.service';
import type { EventPayloadValidatorService } from '../src/api/bus/event-payload-validator.service';
import type { KafkaProducerService } from '../src/shared/modules/kafka/kafka-producer.service';
import {
  KafkaConnectionState,
  type KafkaStatus,
} from '../src/shared/modules/kafka/kafka.types';

jest.mock('../src/shared/modules/kafka/kafka-producer.service', () => ({
  KafkaProducerService: jest.fn(),
}));

type KafkaProducerDouble = {
  getKafkaStatus: jest.Mock;
  isReady: jest.Mock;
  listTopics: jest.Mock;
  publishEvent: jest.Mock;
};

/**
 * Creates a complete cached Kafka lifecycle snapshot for health tests.
 *
 * @param overrides Status fields that differ from a ready healthy baseline.
 * @returns A Kafka status object suitable for the Bus service boundary.
 */
function createKafkaStatus(overrides: Partial<KafkaStatus> = {}): KafkaStatus {
  return {
    state: KafkaConnectionState.Ready,
    connected: true,
    healthy: true,
    ready: true,
    initializationAttempts: 1,
    reconnectAttempts: 0,
    lastSuccessfulMetadataRefreshTime: '2026-07-29T00:00:00.000Z',
    ...overrides,
  };
}

describe('BusService cached health', () => {
  let kafkaProducer: KafkaProducerDouble;
  let service: BusService;

  beforeEach(() => {
    kafkaProducer = {
      getKafkaStatus: jest.fn(),
      isReady: jest.fn(),
      listTopics: jest.fn(),
      publishEvent: jest.fn(),
    };
    service = new BusService(
      kafkaProducer as unknown as KafkaProducerService,
      {} as EventPayloadValidatorService,
    );
  });

  it.each([KafkaConnectionState.Ready, KafkaConnectionState.Reconnecting])(
    'reports cached %s lifecycle state as healthy',
    (state) => {
      kafkaProducer.getKafkaStatus.mockReturnValue(
        createKafkaStatus({
          state,
          connected: false,
          healthy: true,
          ready: false,
        }),
      );

      expect(service.getHealth()).toEqual({ health: 'ok' });
      expect(kafkaProducer.getKafkaStatus).toHaveBeenCalledTimes(1);
      expect(kafkaProducer.isReady).not.toHaveBeenCalled();
      expect(kafkaProducer.listTopics).not.toHaveBeenCalled();
      expect(kafkaProducer.publishEvent).not.toHaveBeenCalled();
    },
  );

  it.each([
    KafkaConnectionState.Initializing,
    KafkaConnectionState.Failed,
    KafkaConnectionState.ShuttingDown,
    KafkaConnectionState.Stopped,
  ])('rejects cached terminal %s lifecycle state', (state) => {
    kafkaProducer.getKafkaStatus.mockReturnValue(
      createKafkaStatus({
        state,
        connected: false,
        healthy: false,
        ready: false,
      }),
    );

    expect(() => service.getHealth()).toThrow(ServiceUnavailableException);
    expect(kafkaProducer.getKafkaStatus).toHaveBeenCalledTimes(1);
    expect(kafkaProducer.isReady).not.toHaveBeenCalled();
    expect(kafkaProducer.listTopics).not.toHaveBeenCalled();
    expect(kafkaProducer.publishEvent).not.toHaveBeenCalled();
  });
});
