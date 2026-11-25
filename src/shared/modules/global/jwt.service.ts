import {
  Injectable,
  OnModuleInit,
  UnauthorizedException,
} from '@nestjs/common';
import { createJwtVerifier, type JwtVerifier } from 'tc-core-library-js';
import { AuthConfig } from '../../config/auth.config';
import { JwtUser } from '../../request/authenticated-request';

/** Verifies callers through tc-core and exposes only the Bus API identity, scopes, and roles. */
@Injectable()
export class JwtService implements OnModuleInit {
  private verifier?: JwtVerifier;

  /** Initializes the configured verifier; returns void and throws for invalid issuer/key configuration. */
  onModuleInit(): void {
    this.verifier = createJwtVerifier({
      AUTH_SECRET: AuthConfig.authSecret,
      VALID_ISSUERS: AuthConfig.validIssuers,
      AUTH0_AUDIENCE: process.env.AUTH0_AUDIENCE,
      JWT_AUDIENCE: process.env.JWT_AUDIENCE,
    });
  }

  /** Verifies a compact token and returns the caller's identity, scopes, and roles; throws UnauthorizedException on failure. */
  async validateToken(token: string): Promise<JwtUser> {
    try {
      if (!this.verifier) throw new Error('JWT verifier is not initialized');
      const { isMachine, scopes, roles } = await this.verifier.verify(token);
      return { isMachine, scopes, roles };
    } catch {
      throw new UnauthorizedException({ message: 'Invalid or expired token' });
    }
  }
}
