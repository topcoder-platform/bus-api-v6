import { UnauthorizedException } from '@nestjs/common';
import { auth, Principal } from 'tc-core-library-js';
import { JwtService } from '../src/shared/modules/global/jwt.service';

const principal: Principal = {
  kind: 'service',
  isMachine: true,
  clientId: 'worker',
  scopes: ['read:groups'],
  roles: [],
  sub: 'service-account-id',
  iss: 'https://issuer.example/realms/m2m',
  aud: 'https://api.example',
  iat: 1000,
  exp: 1900,
};

describe('shared Keycloak validator adapter', () => {
  const verify = jest.fn();
  let service: JwtService;
  beforeEach(() => {
    jest.spyOn(auth, 'createValidator').mockReturnValue({ verify });
    verify.mockReset();
    service = new JwtService();
    service.onModuleInit();
  });
  afterEach(() => jest.restoreAllMocks());

  it('uses the verified service identity without inventing a member ID', async () => {
    verify.mockResolvedValue(principal);
    const user = await service.validateToken('signed-token');
    expect(verify).toHaveBeenCalledWith('signed-token');
    expect(user.isMachine).toBe(true);
    expect(user.userId).toBeUndefined();
    expect(user.scopes).toContain('read:groups');
  });

  it('preserves verified member identity and never turns member scopes into service permissions', async () => {
    verify.mockResolvedValue({
      ...principal,
      kind: 'user',
      isMachine: false,
      userId: 123,
      roles: ['administrator'],
      scopes: [],
    });
    const user = await service.validateToken('member-token');
    expect(String(user.userId)).toBe('123');
    expect(user.isMachine).toBe(false);
    expect(user.scopes).toEqual([]);
  });

  it('rejects invalid tokens without leaking provider errors', async () => {
    verify.mockRejectedValue(new Error('secret-detail'));
    await expect(service.validateToken('bad-token')).rejects.toBeInstanceOf(
      UnauthorizedException,
    );
    await expect(service.validateToken('bad-token')).rejects.not.toThrow(
      'secret-detail',
    );
  });
});

describe('USE_KEYCLOAK validator selection', () => {
  const saved = { ...process.env };
  const keycloakEnv = {
    KEYCLOAK_ISSUER: 'https://keycloak.example/realms/m2m',
    KEYCLOAK_AUDIENCE: 'https://api.example',
    KEYCLOAK_TOKEN_URL: 'https://keycloak.example/m2m/token',
  };
  const keycloakNames = Object.keys(keycloakEnv);

  /** Start the real core validator with only the given provider settings. */
  function start(env: Record<string, string>): void {
    process.env = { ...saved, ALLOW_LEGACY_USER_AUDIENCE: 'true', ...env };
    for (const name of [...keycloakNames, 'USE_KEYCLOAK']) {
      if (!(name in env)) delete process.env[name];
    }
    new JwtService().onModuleInit();
  }

  afterEach(() => {
    process.env = { ...saved };
  });

  it('starts in Auth0 mode without any Keycloak settings', () => {
    expect(() => start({ USE_KEYCLOAK: 'false' })).not.toThrow();
    expect(() => start({})).not.toThrow();
  });

  it('accepts Auth0 machine-token settings in Auth0 mode', () => {
    expect(() =>
      start({
        USE_KEYCLOAK: 'false',
        LEGACY_M2M_ISSUERS: '["https://topcoder-dev.auth0.com/"]',
        LEGACY_M2M_AUDIENCE: 'https://m2m.topcoder-dev.com/',
      }),
    ).not.toThrow();
  });

  it('requires the Keycloak settings when USE_KEYCLOAK=true', () => {
    expect(() => start({ USE_KEYCLOAK: 'true' })).toThrow(TypeError);
    expect(() => start({ USE_KEYCLOAK: 'true', ...keycloakEnv })).not.toThrow();
  });

  it('rejects a USE_KEYCLOAK value other than true or false', () => {
    expect(() => start({ USE_KEYCLOAK: 'keycloak' })).toThrow(
      'USE_KEYCLOAK must be true or false',
    );
  });
});
