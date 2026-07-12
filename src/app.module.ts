import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ApiModule } from './api/api.module';
import { GlobalProvidersModule } from './shared/modules/global/globalProviders.module';
import { TokenValidatorMiddleware } from './shared/request/tokenRequestValidator.middleware';

/** Root module composing the Bus API, shared providers, and token middleware. */
@Module({
  imports: [GlobalProvidersModule, ApiModule],
})
export class AppModule implements NestModule {
  /**
   * Applies optional Bearer-token validation before every application route.
   *
   * Protected handlers are subsequently authorized by the global scope guard;
   * handlers without scope metadata, including health, remain anonymous.
   *
   * @param consumer Nest middleware registry for the application.
   * @returns Nothing.
   */
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(TokenValidatorMiddleware).forRoutes('*');
  }
}
