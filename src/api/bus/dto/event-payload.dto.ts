import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/** Swagger-only schema for the complete legacy-compatible event body. */
export class EventPayloadDto {
  @ApiProperty({
    description:
      'Dot-separated fully qualified event type. Extra request fields are preserved and published unchanged.',
    pattern: '^([a-zA-Z0-9]+\\.)+[a-zA-Z0-9]+$',
    example: 'notifications.action.email.project.created',
  })
  topic!: string;

  @ApiProperty({
    description: 'Service or component that originated the event.',
    example: 'tc-notifications',
  })
  originator!: string;

  @ApiProperty({
    description: 'Event timestamp represented as a legacy string field.',
    example: '2018-04-13T00:00:00Z',
  })
  timestamp!: string;

  @ApiProperty({
    name: 'mime-type',
    description: 'MIME type describing the event payload.',
    example: 'application/json',
  })
  'mime-type'!: string;

  @ApiProperty({
    description: 'Event content; its shape is determined by `mime-type`.',
    type: Object,
    example: { projectId: 12345 },
  })
  payload!: unknown;

  @ApiPropertyOptional({
    description: 'Optional string key used for Kafka message partitioning.',
    example: 'project-12345',
  })
  key?: string;

  [field: string]: unknown;
}
