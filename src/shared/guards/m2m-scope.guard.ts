import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { ROLES_KEY } from '../decorators/roles.decorator';
import { SCOPES_KEY } from '../decorators/scopes.decorator';
import { AuthenticatedRequest } from '../request/authenticated-request';

/**
 * Globally enforces declared Bus API scopes for M2M identities.
 *
 * User tokens are rejected unless the handler also declares `@Roles` and the
 * user holds one of those roles.
 */
@Injectable()
export class M2mScopeGuard implements CanActivate {
  /**
   * Creates the guard with access to Nest handler metadata.
   *
   * @param reflector Metadata reader used to resolve `@Scopes` and `@Roles`.
   */
  constructor(private readonly reflector: Reflector) {}

  /**
   * Allows public handlers and authorizes scoped handlers using M2M scopes or,
   * for handlers that declare them, user roles.
   *
   * @param context Current Nest request execution context.
   * @returns `true` when the request may reach the controller.
   * @throws UnauthorizedException when a protected request has no valid user.
   * @throws ForbiddenException for users without a permitted role, or for
   * M2M tokens with insufficient scopes.
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
      if (this.hasAllowedRole(context, user.roles)) {
        return true;
      }
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

  /**
   * Checks the user's roles against the handler's `@Roles` declaration.
   *
   * @param context Current Nest request execution context.
   * @param userRoles Roles from the validated user token.
   * @returns `true` when the user holds a declared role, case-insensitively.
   */
  private hasAllowedRole(
    context: ExecutionContext,
    userRoles: unknown,
  ): boolean {
    const allowedRoles = this.reflector.getAllAndOverride<string[]>(ROLES_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);
    if (!allowedRoles?.length || !Array.isArray(userRoles)) {
      return false;
    }

    const normalizedRoles = userRoles
      .filter((role): role is string => typeof role === 'string')
      .map((role) => role.trim().toLowerCase());

    return allowedRoles.some((role) =>
      normalizedRoles.includes(role.toLowerCase()),
    );
  }
}
