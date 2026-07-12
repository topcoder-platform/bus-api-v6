import { ApiProperty } from '@nestjs/swagger';

/** Swagger schema for legacy-style Bus API error bodies. */
export class ErrorResponseDto {
  @ApiProperty({
    description: 'Human-readable explanation of the request failure.',
    example: 'Unknown event type "notifications.action.unknown"',
  })
  message!: string;
}
