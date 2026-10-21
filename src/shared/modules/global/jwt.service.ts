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

  /** Initialize trusted settings after configuration loads; throws on missing Keycloak opt-in. */
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
