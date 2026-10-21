import { ExecutionContext, ForbiddenException } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { M2mScopeGuard } from '../src/shared/guards/m2m-scope.guard';

/** Create a guard context from verified-principal fields; returns a Nest test context without I/O. */
function context(user: Record<string, unknown>): ExecutionContext {
  return {
    switchToHttp: () => ({ getRequest: () => ({ user }) }),
    getHandler: () => ({}),
    getClass: () => ({}),
  } as ExecutionContext;
}

describe('shared Bus scope policy', () => {
  const reflector = {
    getAllAndOverride: jest
      .fn()
      .mockReturnValue(['write:bus_api', 'read:bus_topics']),
  } as unknown as Reflector;
  const guard = new M2mScopeGuard(reflector);
  it('requires every declared permission on a verified service', () => {
    expect(
      guard.canActivate(
        context({
          kind: 'service',
          isMachine: true,
          scopes: ['write:bus_api', 'read:bus_topics'],
        }),
      ),
    ).toBe(true);
    expect(() =>
      guard.canActivate(
        context({
          kind: 'service',
          isMachine: true,
          scopes: ['write:bus_api'],
        }),
      ),
    ).toThrow(ForbiddenException);
  });
  it('does not use member OAuth scopes as machine permissions', () => {
    expect(() =>
      guard.canActivate(
        context({
          kind: 'user',
          isMachine: false,
          scopes: ['write:bus_api', 'read:bus_topics'],
        }),
      ),
    ).toThrow(ForbiddenException);
  });
});
