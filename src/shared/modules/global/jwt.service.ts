import {
  Injectable,
  OnModuleInit,
  UnauthorizedException,
} from '@nestjs/common';
import { auth } from 'tc-core-library-js';
import { AuthConfig } from '../../config/auth.config';
import { JwtUser } from '../../request/authenticated-request';

/** Adapts the common core validator to Nest; all token trust and classification live in core. */
@Injectable()
export class JwtService implements OnModuleInit {
  private validator?: ReturnType<typeof auth.createValidator>;

  /**
   * Build the core validator once configuration has loaded. Nest calls this at startup.
   * `USE_KEYCLOAK=true` requires `KEYCLOAK_ISSUER`, `KEYCLOAK_AUDIENCE` and
   * `KEYCLOAK_TOKEN_URL`; unset or `false` (Auth0) needs none of them and trusts Auth0
   * machine tokens through `LEGACY_M2M_ISSUERS`/`LEGACY_M2M_AUDIENCE`. Member tokens use
   * `AUTH_SECRET` and `VALID_ISSUERS` in both modes. Other core settings come from the
   * environment.
   * @returns Nothing.
   * @throws TypeError when `USE_KEYCLOAK` is not true/false or the selected settings are invalid.
   */
  onModuleInit(): void {
    this.validator = auth.createValidator({
      AUTH_SECRET: AuthConfig.authSecret,
      VALID_ISSUERS: JSON.stringify(AuthConfig.validIssuers),
    });
  }

  /**
   * Verify a compact bearer JWT through core and return its common principal.
   * @param token JWT without the Bearer prefix.
   * @returns Verified user/service identity, including authoritative machine status.
   * @throws UnauthorizedException for invalid tokens or an uninitialized validator.
   */
  async validateToken(token: string): Promise<JwtUser> {
    try {
      if (!this.validator) throw new Error('Validator unavailable');
      return await this.validator.verify(token);
    } catch {
      throw new UnauthorizedException({ message: 'Invalid or expired token' });
    }
  }
}
