import { SetMetadata } from '@nestjs/common';
import { Scope } from '../enums/scopes.enum';

/** Metadata key used by the M2M scope guard. */
export const SCOPES_KEY = 'scopes';

/**
 * Declares the M2M scopes required to invoke a controller handler.
 *
 * @param scopes Scope values accepted by the protected handler.
 * @returns A Nest metadata decorator consumed by `M2mScopeGuard`.
 */
export const Scopes = (...scopes: Scope[]): MethodDecorator & ClassDecorator =>
  SetMetadata(SCOPES_KEY, scopes);
