import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import {
  ApiAcceptedResponse,
  ApiBadRequestResponse,
  ApiBearerAuth,
  ApiBody,
  ApiForbiddenResponse,
  ApiInternalServerErrorResponse,
  ApiOperation,
  ApiTags,
  ApiUnauthorizedResponse,
} from '@nestjs/swagger';
import { Scopes } from '../../shared/decorators/scopes.decorator';
import { Scope } from '../../shared/enums/scopes.enum';
import { BusService } from './bus.service';
import { ErrorResponseDto } from './dto/error-response.dto';
import { EventPayloadDto } from './dto/event-payload.dto';

/** Handles scoped publication of complete legacy-compatible bus events. */
@ApiTags('Events')
@ApiBearerAuth()
@Controller('bus/events')
export class BusEventsController {
  /**
   * Creates the event controller.
   *
   * @param busService Application service that validates and publishes events.
   */
  constructor(private readonly busService: BusService) {}

  /**
   * Publishes a submitted event and returns `202 Accepted` without a body.
   *
   * @param body Raw JSON event body, including any caller-defined extra fields.
   * @returns A promise resolving with no response body after publication.
   * @throws BadRequestException for contract violations or unknown topics.
   * @throws InternalServerErrorException for other Kafka publish failures.
   */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  @Scopes(Scope.WriteBusApi)
  @ApiOperation({ summary: 'Publish an event to Kafka' })
  @ApiBody({
    type: EventPayloadDto,
    description:
      'Legacy event shape. Any additional fields are preserved and published unchanged.',
  })
  @ApiAcceptedResponse({ description: 'Event accepted and published.' })
  @ApiBadRequestResponse({ type: ErrorResponseDto })
  @ApiUnauthorizedResponse({ type: ErrorResponseDto })
  @ApiForbiddenResponse({ type: ErrorResponseDto })
  @ApiInternalServerErrorResponse({ type: ErrorResponseDto })
  async publish(@Body() body: unknown): Promise<void> {
    await this.busService.publishEvent(body);
  }
}
