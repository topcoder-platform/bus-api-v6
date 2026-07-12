import { Controller, Get, Head } from '@nestjs/common';
import {
  ApiBearerAuth,
  ApiForbiddenResponse,
  ApiInternalServerErrorResponse,
  ApiOkResponse,
  ApiOperation,
  ApiTags,
  ApiUnauthorizedResponse,
} from '@nestjs/swagger';
import { Scopes } from '../../shared/decorators/scopes.decorator';
import { Scope } from '../../shared/enums/scopes.enum';
import { BusService } from './bus.service';
import { ErrorResponseDto } from './dto/error-response.dto';

/** Handles scoped Kafka topic discovery and availability checks. */
@ApiTags('Topics')
@ApiBearerAuth()
@Controller('bus/topics')
export class BusTopicsController {
  /**
   * Creates the topics controller.
   *
   * @param busService Application service that retrieves Kafka topic metadata.
   */
  constructor(private readonly busService: BusService) {}

  /**
   * Lists available Kafka topic names.
   *
   * @returns A promise resolving to a string array of topic names.
   * @throws InternalServerErrorException when Kafka metadata is unavailable.
   */
  @Get()
  @Scopes(Scope.ReadBusTopics)
  @ApiOperation({ summary: 'List Kafka topics' })
  @ApiOkResponse({ type: String, isArray: true })
  @ApiUnauthorizedResponse({ type: ErrorResponseDto })
  @ApiForbiddenResponse({ type: ErrorResponseDto })
  @ApiInternalServerErrorResponse({ type: ErrorResponseDto })
  async list(): Promise<string[]> {
    return this.busService.listTopics();
  }

  /**
   * Checks topic metadata availability and emits no response body.
   *
   * @returns A promise resolving after the same metadata path as `GET` succeeds.
   * @throws InternalServerErrorException when Kafka metadata is unavailable.
   */
  @Head()
  @Scopes(Scope.ReadBusTopics)
  @ApiOperation({ summary: 'Check Kafka topic metadata availability' })
  @ApiOkResponse({ description: 'Topic metadata is available.' })
  @ApiUnauthorizedResponse({ type: ErrorResponseDto })
  @ApiForbiddenResponse({ type: ErrorResponseDto })
  @ApiInternalServerErrorResponse({ type: ErrorResponseDto })
  async head(): Promise<void> {
    await this.busService.listTopics();
  }
}
