import { Request } from 'express';

/** Normalized token identity used by Bus API authorization. */
export interface JwtUser {
  isMachine: boolean;
  scopes: string[];
}

/** Express request after optional Bearer-token validation. */
export interface AuthenticatedRequest extends Request {
  user?: JwtUser;
}
