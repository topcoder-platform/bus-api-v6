import {
  ExecutionContext,
  ForbiddenException,
  UnauthorizedException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { BusEventsController } from '../src/api/bus/bus-events.controller';
import { BusHealthController } from '../src/api/bus/bus-health.controller';
import { BusTopicsController } from '../src/api/bus/bus-topics.controller';
import { M2mScopeGuard } from '../src/shared/guards/m2m-scope.guard';
import type { JwtUser } from '../src/shared/request/authenticated-request';

jest.mock('../src/shared/modules/kafka/kafka-producer.service', () => ({
  KafkaProducerService: jest.fn(),
}));

/**
 * Builds the minimal HTTP execution context read by the guard.
 *
 * @param controller Controller class whose metadata should be resolved.
 * @param methodName Controller method whose metadata should be resolved.
 * @param user Normalized token identity attached by the token middleware.
 * @returns An execution context for one controller handler.
 */
function createContext(
  controller: new (...args: never[]) => unknown,
  methodName: string,
  user?: JwtUser,
): ExecutionContext {
  const handler: unknown = Object.getOwnPropertyDescriptor(
    controller.prototype,
    methodName,
  )?.value;

  return {
    getClass: () => controller,
    getHandler: () => handler,
    switchToHttp: () => ({ getRequest: () => ({ user }) }),
  } as unknown as ExecutionContext;
}

/**
 * Creates a normalized token identity.
 *
 * @param overrides Identity fields that differ from a role-less user token.
 * @returns A JWT user suitable for guard tests.
 */
function createUser(overrides: Partial<JwtUser> = {}): JwtUser {
  return { isMachine: false, scopes: [], roles: [], ...overrides };
}

describe('M2mScopeGuard', () => {
  const guard = new M2mScopeGuard(new Reflector());
  const publish = 'publish';
  const listTopics = 'list';

  it('allows an administrator user token to publish events', () => {
    const user = createUser({ roles: ['Topcoder User', 'administrator'] });

    expect(
      guard.canActivate(createContext(BusEventsController, publish, user)),
    ).toBe(true);
  });

  it('matches the administrator role case-insensitively', () => {
    const user = createUser({ roles: ['Administrator'] });

    expect(
      guard.canActivate(createContext(BusEventsController, publish, user)),
    ).toBe(true);
  });

  it('rejects a non-administrator user token on event publishing', () => {
    const user = createUser({ roles: ['Topcoder User', 'copilot'] });

    expect(() =>
      guard.canActivate(createContext(BusEventsController, publish, user)),
    ).toThrow(ForbiddenException);
  });

  it('keeps topic listing M2M-only for administrator user tokens', () => {
    const user = createUser({ roles: ['administrator'] });

    expect(() =>
      guard.canActivate(createContext(BusTopicsController, listTopics, user)),
    ).toThrow(ForbiddenException);
  });

  it('allows an M2M token with the write scope to publish events', () => {
    const user = createUser({ isMachine: true, scopes: ['write:bus_api'] });

    expect(
      guard.canActivate(createContext(BusEventsController, publish, user)),
    ).toBe(true);
  });

  it('rejects an M2M token without the write scope', () => {
    const user = createUser({ isMachine: true, scopes: ['read:bus_topics'] });

    expect(() =>
      guard.canActivate(createContext(BusEventsController, publish, user)),
    ).toThrow(ForbiddenException);
  });

  it('rejects event publishing without a token', () => {
    expect(() =>
      guard.canActivate(createContext(BusEventsController, publish)),
    ).toThrow(UnauthorizedException);
  });

  it('leaves health unauthenticated', () => {
    expect(guard.canActivate(createContext(BusHealthController, 'get'))).toBe(
      true,
    );
  });
});
