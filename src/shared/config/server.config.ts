import { parseInteger } from './env.utils';

/** Constant HTTP route prefix for every runtime environment. */
export const API_ROUTE_PREFIX = 'v6';

/** Server and HTTP runtime configuration derived from the environment. */
export const ServerConfig = {
  port: parseInteger(process.env.PORT, 3000),
  nodeEnv: process.env.NODE_ENV || 'development',
  logLevel: process.env.LOG_LEVEL || 'info',
  corsAllowedOrigin: process.env.CORS_ALLOWED_ORIGIN?.trim() || undefined,
  bodySizeLimit: '15mb',
  routePrefix: API_ROUTE_PREFIX,
} as const;
