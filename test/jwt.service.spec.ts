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
