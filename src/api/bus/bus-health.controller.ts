import { Controller, Get, Head } from '@nestjs/common';
import {
  ApiOkResponse,
  ApiOperation,
  ApiServiceUnavailableResponse,
  ApiTags,
} from '@nestjs/swagger';
import { BusService } from './bus.service';
import { ErrorResponseDto } from './dto/error-response.dto';
import { HealthResponseDto } from './dto/health-response.dto';

/** Exposes unauthenticated health backed by cached Kafka lifecycle state. */
@ApiTags('Health')
@Controller('bus/health')
export class BusHealthController {
  /**
   * Creates the health controller.
   *
   * @param busService Application service that evaluates Kafka health.
   */
  constructor(private readonly busService: BusService) {}

  /**
   * Returns the exact healthy response while the process can serve or recover.
   *
   * @returns `{ health: "ok" }` after readiness or during runtime recovery.
   * @throws ServiceUnavailableException after recovery fails or during stop.
   */
  @Get()
  @ApiOperation({ summary: 'Report Kafka-backed service health' })
  @ApiOkResponse({ type: HealthResponseDto })
  @ApiServiceUnavailableResponse({ type: ErrorResponseDto })
  get(): HealthResponseDto {
    return this.busService.getHealth();
  }

  /**
   * Applies the same cached health decision as `GET` without a response body.
   *
   * @returns Nothing after readiness or during bounded runtime recovery.
   * @throws ServiceUnavailableException after recovery fails or during stop.
   */
  @Head()
  @ApiOperation({ summary: 'Check Kafka-backed service health' })
  @ApiOkResponse({
    description: 'Kafka has become ready or runtime recovery is active.',
  })
  @ApiServiceUnavailableResponse({ type: ErrorResponseDto })
  head(): void {
    this.busService.getHealth();
  }
}
