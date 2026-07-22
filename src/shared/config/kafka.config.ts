import { parseBoolean, parseCommaSeparated, parseInteger } from './env.utils';

/**
 * V6-only Kafka connection configuration for publishing and topic metadata.
 *
 * The shared Kafka layer maps these values into its producer and admin clients.
 */
export const KafkaConfig = {
  brokers: parseCommaSeparated(process.env.KAFKA_URL, ['localhost:9092']),
  clientId: process.env.KAFKA_CLIENT_ID || 'bus-api-v6',
  tls: {
    enabled: parseBoolean(process.env.KAFKA_TLS_ENABLED, false),
    rejectUnauthorized: parseBoolean(
      process.env.KAFKA_TLS_REJECT_UNAUTHORIZED,
      true,
    ),
  },
  sasl: {
    mechanism: process.env.KAFKA_SASL_MECHANISM || undefined,
    username: process.env.KAFKA_SASL_USERNAME || undefined,
    password: process.env.KAFKA_SASL_PASSWORD || undefined,
  },
  connectionTimeout: parseInteger(process.env.KAFKA_CONNECTION_TIMEOUT, 10000),
  requestTimeout: parseInteger(process.env.KAFKA_REQUEST_TIMEOUT, 30000),
  retry: {
    attempts: parseInteger(process.env.KAFKA_RETRY_ATTEMPTS, 5),
    initialRetryTime: parseInteger(process.env.KAFKA_INITIAL_RETRY_TIME, 100),
    maxRetryTime: parseInteger(process.env.KAFKA_MAX_RETRY_TIME, 30000),
  },
} as const;
