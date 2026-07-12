import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { SCOPES_KEY } from '../decorators/scopes.decorator';
import { AuthenticatedRequest } from '../request/authenticated-request';

/** Globally enforces declared Bus API scopes for M2M identities only. */
@Injectable()
export class M2mScopeGuard implements CanActivate {
  /**
   * Creates the guard with access to Nest handler metadata.
   *
   * @param reflector Metadata reader used to resolve `@Scopes` declarations.
   */
  constructor(private readonly reflector: Reflector) {}

  /**
   * Allows public handlers and authorizes scoped handlers using M2M scopes.
   *
   * @param context Current Nest request execution context.
   * @returns `true` when the request may reach the controller.
   * @throws UnauthorizedException when a protected request has no valid user.
   * @throws ForbiddenException for non-M2M users or insufficient M2M scopes.
   */
  canActivate(context: ExecutionContext): boolean {
    const requiredScopes = this.reflector.getAllAndOverride<string[]>(
      SCOPES_KEY,
      [context.getHandler(), context.getClass()],
    );
    if (!requiredScopes?.length) {
      return true;
    }

    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const user = request.user;
    if (
      !user ||
      typeof user.isMachine !== 'boolean' ||
      !Array.isArray(user.scopes) ||
      !user.scopes.every((scope) => typeof scope === 'string')
    ) {
      throw new UnauthorizedException({ message: 'Missing or invalid token' });
    }

    if (!user.isMachine) {
      throw new ForbiddenException({ message: 'M2M token required' });
    }

    const hasRequiredScope = requiredScopes.every((scope) =>
      user.scopes.includes(scope),
    );
    if (!hasRequiredScope) {
      throw new ForbiddenException({ message: 'Insufficient token scope' });
    }

    return true;
  }
}
