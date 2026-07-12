import { Module } from '@nestjs/common';
import { BusEventsController } from './bus-events.controller';
import { BusHealthController } from './bus-health.controller';
import { BusTopicsController } from './bus-topics.controller';
import { BusService } from './bus.service';
import { EventPayloadValidatorService } from './event-payload-validator.service';

/** Registers the Bus endpoint controllers and their focused application services. */
@Module({
  controllers: [BusEventsController, BusTopicsController, BusHealthController],
  providers: [BusService, EventPayloadValidatorService],
})
export class BusModule {}
