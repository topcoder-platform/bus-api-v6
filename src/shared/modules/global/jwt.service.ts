import {
  Injectable,
  OnModuleInit,
  UnauthorizedException,
} from '@nestjs/common';
import { createJwtVerifier, type JwtVerifier } from 'tc-core-library-js';
import { AuthConfig } from '../../config/auth.config';
import { JwtUser } from '../../request/authenticated-request';

/** Verifies callers through tc-core and exposes only the Bus API identity and scopes. */
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

  /** Verifies a compact token and returns machine identity/scopes; throws UnauthorizedException on failure. */
  async validateToken(token: string): Promise<JwtUser> {
    try {
      if (!this.verifier) throw new Error('JWT verifier is not initialized');
      const payload = await this.verifier.verify(token);
      return { isMachine: payload.isMachine, scopes: payload.scopes };
    } catch {
      throw new UnauthorizedException({ message: 'Invalid or expired token' });
    }
  }
}
