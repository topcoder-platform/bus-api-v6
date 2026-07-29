import { ApiProperty } from '@nestjs/swagger';

/** Swagger schema for the exact successful Kafka-backed health response. */
export class HealthResponseDto {
  @ApiProperty({
    description:
      'Healthy after Kafka first becomes ready, including bounded runtime recovery.',
    enum: ['ok'],
    example: 'ok',
  })
  health!: 'ok';
}
