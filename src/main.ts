import 'dotenv/config';
import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import * as cors from 'cors';
import { NextFunction, Request, Response } from 'express';
import { ApiModule } from './api/api.module';
import { AppModule } from './app.module';
import { ServerConfig } from './shared/config/server.config';
import { LoggerService } from './shared/modules/global/logger.service';

/**
 * Bootstraps the Topcoder Bus API v6 HTTP application.
 *
 * Startup configures the constant `/v6` prefix, CORS, HTTP logging, 15 MB body
 * parsers, validation, Swagger documentation, process error handlers, and the
 * configured listen port. It is invoked once from this module at process start.
 *
 * @returns A promise that resolves after the HTTP server begins listening.
 * @throws Propagates Nest application creation or listen failures.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    rawBody: true,
    logger: ['error', 'warn', 'log', 'debug', 'verbose'],
  });
  const logger = LoggerService.forRoot('Bootstrap');
  const requestLogger = LoggerService.forRoot('HttpRequest');

  app.enableShutdownHooks();

  app.setGlobalPrefix(ServerConfig.routePrefix);

  const allowedOrigins: (string | RegExp)[] = [
    /^https?:\/\/localhost(?::\d+)?$/i,
    /^https?:\/\/([\w-]+\.)*topcoder\.com(?::\d+)?$/i,
    /^https?:\/\/([\w-]+\.)*topcoder-dev\.com(?::\d+)?$/i,
  ];
  if (ServerConfig.corsAllowedOrigin) {
    allowedOrigins.push(ServerConfig.corsAllowedOrigin);
  }

  const corsOptions: cors.CorsOptions = {
    allowedHeaders:
      'Origin, X-Requested-With, Content-Type, Accept, Authorization, Access-Control-Allow-Origin, Access-Control-Allow-Headers,currentOrg,overrideOrg,x-atlassian-cloud-id,x-api-key,x-orgid',
    credentials: true,
    methods: 'GET, HEAD, POST, OPTIONS, PUT, DELETE, PATCH',
    origin: (origin, callback) => {
      if (!origin) {
        callback(null, true);
        return;
      }

      const isAllowed = allowedOrigins.some((allowedOrigin) =>
        allowedOrigin instanceof RegExp
          ? allowedOrigin.test(origin)
          : allowedOrigin === origin,
      );
      callback(null, isAllowed ? origin : false);
    },
  };
  app.use(cors(corsOptions));

  app.use((req: Request, res: Response, next: NextFunction) => {
    const startedAt = Date.now();
    requestLogger.log(`Request ${req.method} ${req.originalUrl}`);

    res.on('finish', () => {
      requestLogger.log(
        `Response ${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now() - startedAt}ms`,
      );
    });
    next();
  });

  app.useBodyParser('json', { limit: ServerConfig.bodySizeLimit });
  app.useBodyParser('urlencoded', {
    limit: ServerConfig.bodySizeLimit,
    extended: true,
  });

  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: false,
      forbidUnknownValues: false,
    }),
  );

  const swaggerConfig = new DocumentBuilder()
    .setTitle('Topcoder Bus API v6')
    .setDescription(
      'Publishes events, lists Kafka topics, and reports Kafka-backed health. ' +
        'Event publishing requires the `write:bus_api` M2M scope; topic listing ' +
        'requires the `read:bus_topics` M2M scope. Health is unauthenticated.',
    )
    .setVersion('6.0')
    .addBearerAuth({
      type: 'http',
      scheme: 'bearer',
      bearerFormat: 'JWT',
      name: 'JWT',
      description: 'Topcoder M2M JWT containing the endpoint-required scope',
      in: 'header',
    })
    .build();
  const swaggerDocument = SwaggerModule.createDocument(app, swaggerConfig, {
    include: [ApiModule],
    deepScanRoutes: true,
  });
  SwaggerModule.setup('/v6/bus/api-docs', app, swaggerDocument);

  process.on('unhandledRejection', (reason) => {
    logger.error(
      'Unhandled Promise Rejection',
      reason instanceof Error ? reason.stack : String(reason),
    );
  });
  process.on('uncaughtException', (error: Error) => {
    logger.error(`Uncaught Exception: ${error.message}`, error.stack);
  });

  await app.listen(ServerConfig.port);
  logger.log(`Server started on port ${ServerConfig.port}`);
}

void bootstrap();
