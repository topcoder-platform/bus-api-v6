import {
  Injectable,
  NestMiddleware,
  UnauthorizedException,
} from '@nestjs/common';
import { NextFunction, Response } from 'express';
import { JwtService } from '../modules/global/jwt.service';
import { AuthenticatedRequest } from './authenticated-request';

/** Validates optional Bearer credentials and attaches a normalized JWT user. */
@Injectable()
export class TokenValidatorMiddleware implements NestMiddleware {
  /**
   * Creates token-validation middleware backed by the shared JWT service.
   *
   * @param jwtService Standard Topcoder token validation service.
   */
  constructor(private readonly jwtService: JwtService) {}

  /**
   * Validates a Bearer header when present and leaves missing auth untouched.
   *
   * Missing or non-Bearer authorization is deferred to scoped route guards so
   * unauthenticated health handlers remain accessible.
   *
   * @param request Incoming Express request that may receive a normalized user.
   * @param response Express response, unused but required by Nest middleware.
   * @param next Callback that continues the middleware chain.
   * @returns A promise that resolves after validation or continuing the chain.
   * @throws UnauthorizedException for malformed, invalid, or expired Bearer JWTs.
   */
  async use(
    request: AuthenticatedRequest,
    response: Response,
    next: NextFunction,
  ): Promise<void> {
    void response;
    const authorization = request.headers.authorization;
    if (!authorization || !authorization.startsWith('Bearer')) {
      next();
      return;
    }

    const match = /^Bearer\s+([^\s]+)$/.exec(authorization);
    if (!match) {
      throw new UnauthorizedException({ message: 'Invalid or expired token' });
    }

    try {
      request.user = await this.jwtService.validateToken(match[1]);
    } catch {
      throw new UnauthorizedException({ message: 'Invalid or expired token' });
    }

    next();
  }
}
