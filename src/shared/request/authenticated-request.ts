import { Request } from 'express';
import type { Principal } from 'tc-core-library-js';

/** Normalized token identity used by Bus API authorization. */
export type JwtUser = Principal;

/** Express request after optional Bearer-token validation. */
export interface AuthenticatedRequest extends Request {
  user?: JwtUser;
}
