import { createJwtVerifier } from 'tc-core-library-js';
import { JwtService } from '../src/shared/modules/global/jwt.service';

jest.mock('tc-core-library-js', () => ({ createJwtVerifier: jest.fn() }));

describe('JwtService', () => {
  const verify = jest.fn();
  let service: JwtService;

  beforeEach(() => {
    jest.mocked(createJwtVerifier).mockReturnValue({ verify });
    service = new JwtService();
    service.onModuleInit();
  });

  it('passes verified user roles to the authorization guard', async () => {
    verify.mockResolvedValueOnce({
      userId: 40029484,
      roles: ['Topcoder User', 'Administrator'],
      scopes: [],
      isMachine: false,
    });

    await expect(service.validateToken('user-token')).resolves.toEqual({
      isMachine: false,
      scopes: [],
      roles: ['Topcoder User', 'Administrator'],
    });
    expect(verify).toHaveBeenCalledWith('user-token');
  });

  it('preserves the scopes for a verified machine token', async () => {
    verify.mockResolvedValueOnce({
      isMachine: true,
      scopes: ['write:bus_api', 'read:bus_topics'],
      roles: [],
    });

    await expect(service.validateToken('m2m-token')).resolves.toEqual({
      isMachine: true,
      scopes: ['write:bus_api', 'read:bus_topics'],
      roles: [],
    });
  });
});
