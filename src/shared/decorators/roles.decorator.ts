import { SetMetadata } from '@nestjs/common';
import { UserRole } from '../enums/roles.enum';

/** Metadata key used by the M2M scope guard for user-token access. */
export const ROLES_KEY = 'roles';

/**
 * Declares the user roles that may invoke a scoped handler without an M2M token.
 *
 * Handlers without this decorator stay M2M-only. Role names are matched
 * case-insensitively by `M2mScopeGuard`.
 *
 * @param roles Topcoder user roles accepted by the protected handler.
 * @returns A Nest metadata decorator consumed by `M2mScopeGuard`.
 */
export const Roles = (...roles: UserRole[]): MethodDecorator & ClassDecorator =>
  SetMetadata(ROLES_KEY, roles);
