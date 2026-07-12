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

/** Exposes unauthenticated health backed by Kafka readiness and connectivity. */
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
   * Returns the exact healthy response when Kafka is ready and connected.
   *
   * @returns `{ health: "ok" }` while the Kafka dependency is healthy.
   * @throws ServiceUnavailableException while Kafka is unavailable.
   */
  @Get()
  @ApiOperation({ summary: 'Report Kafka-backed service health' })
  @ApiOkResponse({ type: HealthResponseDto })
  @ApiServiceUnavailableResponse({ type: ErrorResponseDto })
  get(): HealthResponseDto {
    return this.busService.getHealth();
  }

  /**
   * Applies the same Kafka health decision as `GET` without a response body.
   *
   * @returns Nothing when Kafka is ready and connected.
   * @throws ServiceUnavailableException while Kafka is unavailable.
   */
  @Head()
  @ApiOperation({ summary: 'Check Kafka-backed service health' })
  @ApiOkResponse({ description: 'Kafka is ready and connected.' })
  @ApiServiceUnavailableResponse({ type: ErrorResponseDto })
  head(): void {
    this.busService.getHealth();
  }
}
