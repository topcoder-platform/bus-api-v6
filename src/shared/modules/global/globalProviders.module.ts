import { Global, Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { M2mScopeGuard } from '../../guards/m2m-scope.guard';
import { TokenValidatorMiddleware } from '../../request/tokenRequestValidator.middleware';
import { KafkaModule } from '../kafka/kafka.module';
import { JwtService } from './jwt.service';
import { LoggerService } from './logger.service';

/**
 * Registers the cross-cutting providers available throughout the service.
 *
 * The application logger, Kafka layer, JWT validation, token middleware, and
 * global M2M scope guard are available throughout the application.
 */
@Global()
@Module({
  imports: [KafkaModule],
  providers: [
    JwtService,
    TokenValidatorMiddleware,
    M2mScopeGuard,
    {
      provide: APP_GUARD,
      useExisting: M2mScopeGuard,
    },
    {
      provide: LoggerService,
      useFactory: () => new LoggerService('Global'),
    },
  ],
  exports: [
    LoggerService,
    KafkaModule,
    JwtService,
    TokenValidatorMiddleware,
    M2mScopeGuard,
  ],
})
export class GlobalProvidersModule {}
