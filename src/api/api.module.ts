import { Module } from '@nestjs/common';
import { BusModule } from './bus/bus.module';

/** Aggregates the feature modules exposed by the Bus API. */
@Module({ imports: [BusModule] })
export class ApiModule {}
