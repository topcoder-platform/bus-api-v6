import { parseJsonStringArray } from './env.utils';

const DEFAULT_VALID_ISSUERS = [
  'https://topcoder-dev.auth0.com/',
  'https://api.topcoder-dev.com',
  'https://api.topcoder.com',
] as const;

/** Authentication settings used to validate Topcoder Bearer tokens. */
export const AuthConfig = {
  authSecret: process.env.AUTH_SECRET,
  validIssuers: parseJsonStringArray(
    process.env.VALID_ISSUERS,
    DEFAULT_VALID_ISSUERS,
  ),
} as const;
