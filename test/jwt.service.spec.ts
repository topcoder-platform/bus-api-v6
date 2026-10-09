import { JwtService } from '../src/shared/modules/global/jwt.service';

let mockAuthUser: Record<string, unknown> | undefined;

jest.mock('tc-core-library-js', () => ({
  middleware: {
    jwtAuthenticator:
      () =>
      (
        request: { authUser?: Record<string, unknown> },
        response: unknown,
        next: (error?: unknown) => void,
      ) => {
        request.authUser = mockAuthUser;
        next();
      },
  },
}));

describe('JwtService token normalization', () => {
  let service: JwtService;

  beforeEach(() => {
    service = new JwtService();
    service.onModuleInit();
  });

  it('keeps user roles and treats scope-less user tokens as non-machine', async () => {
    mockAuthUser = {
      userId: 40029484,
      handle: 'admin',
      roles: [' Topcoder User ', 'administrator', 7, ''],
    };

    await expect(service.validateToken('user-token')).resolves.toEqual({
      isMachine: false,
      scopes: [],
      roles: ['Topcoder User', 'administrator'],
    });
  });

  it('normalizes M2M tokens without a roles claim', async () => {
    mockAuthUser = { scope: 'write:bus_api read:bus_topics', isMachine: true };

    await expect(service.validateToken('m2m-token')).resolves.toEqual({
      isMachine: true,
      scopes: ['write:bus_api', 'read:bus_topics'],
      roles: [],
    });
  });
});
