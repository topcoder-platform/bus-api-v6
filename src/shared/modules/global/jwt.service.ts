import {
  Injectable,
  OnModuleInit,
  UnauthorizedException,
} from '@nestjs/common';
import { AuthConfig } from '../../config/auth.config';
import { JwtUser } from '../../request/authenticated-request';

interface AuthenticatorRequest {
  headers: { authorization: string };
  authUser?: Record<string, unknown>;
}

interface AuthenticatorResponse {
  status: (statusCode: number) => AuthenticatorResponse;
  json: (body?: unknown) => void;
  send: (...args: unknown[]) => void;
  end: (body?: unknown) => void;
}

type JwtAuthenticator = (
  request: AuthenticatorRequest,
  response: AuthenticatorResponse,
  next: (error?: unknown) => void,
) => void;

interface TcCoreLibrary {
  middleware: {
    jwtAuthenticator: (options: {
      AUTH_SECRET?: string;
      VALID_ISSUERS: string;
    }) => JwtAuthenticator;
  };
}

// tc-core-library-js is CommonJS-only and does not publish TypeScript types.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const tcCore = require('tc-core-library-js') as TcCoreLibrary;

/** Validates Topcoder JWTs and exposes only the identity needed by Bus API. */
@Injectable()
export class JwtService implements OnModuleInit {
  private authenticator?: JwtAuthenticator;

  /**
   * Creates the standard Topcoder JWT authenticator from active auth settings.
   *
   * Nest invokes this once during module initialization.
   *
   * @returns Nothing.
   * @throws Error when the auth secret or issuer configuration is invalid.
   */
  onModuleInit(): void {
    this.authenticator = tcCore.middleware.jwtAuthenticator({
      AUTH_SECRET: AuthConfig.authSecret,
      VALID_ISSUERS: JSON.stringify(AuthConfig.validIssuers),
    });
  }

  /**
   * Validates a raw Bearer token and normalizes its M2M identity and scopes.
   * Tokens with at least one usable `scope` or `scopes` value are treated as
   * M2M identities, while explicit machine identities remain supported.
   *
   * @param token JWT value without the `Bearer` scheme prefix.
   * @returns The normalized token identity.
   * @throws UnauthorizedException when validation fails or yields no payload.
   */
  async validateToken(token: string): Promise<JwtUser> {
    if (!this.authenticator) {
      throw new UnauthorizedException({ message: 'Invalid or expired token' });
    }

    const payload = await this.authenticate(token);
    const scopes = this.normalizeScopes(payload.scopes ?? payload.scope);

    return {
      isMachine: scopes.length > 0 || payload.isMachine === true,
      scopes,
    };
  }

  /**
   * Runs the callback-based Topcoder authenticator as a promise.
   *
   * @param token JWT value to submit to the authenticator.
   * @returns The decoded payload attached by the authenticator.
   * @throws UnauthorizedException when the authenticator rejects the token.
   */
  private authenticate(token: string): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      const request: AuthenticatorRequest = {
        headers: { authorization: `Bearer ${token}` },
      };
      const rejectUnauthorized = (): void => {
        reject(
          new UnauthorizedException({ message: 'Invalid or expired token' }),
        );
      };
      const response: AuthenticatorResponse = {
        status: () => response,
        json: rejectUnauthorized,
        send: rejectUnauthorized,
        end: rejectUnauthorized,
      };

      try {
        this.authenticator?.(request, response, (error?: unknown) => {
          if (error || !request.authUser) {
            rejectUnauthorized();
            return;
          }
          resolve(request.authUser);
        });
      } catch {
        rejectUnauthorized();
      }
    });
  }

  /**
   * Converts either standard space-delimited or array scope claims to strings.
   *
   * @param value Decoded token scope claim.
   * @returns A de-duplicated list of non-empty scopes.
   */
  private normalizeScopes(value: unknown): string[] {
    const rawScopes = Array.isArray(value)
      ? value
      : typeof value === 'string'
        ? value.split(' ')
        : [];

    return [
      ...new Set(
        rawScopes
          .filter((scope): scope is string => typeof scope === 'string')
          .map((scope) => scope.trim())
          .filter(Boolean),
      ),
    ];
  }
}
