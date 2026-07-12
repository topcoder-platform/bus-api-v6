import { ApiProperty } from '@nestjs/swagger';

/** Swagger schema for the exact successful Kafka-backed health response. */
export class HealthResponseDto {
  @ApiProperty({
    description:
      'Healthy status returned only while Kafka is ready and connected.',
    enum: ['ok'],
    example: 'ok',
  })
  health!: 'ok';
}
