import {
  Injectable,
  LoggerService as NestLoggerService,
  LogLevel,
} from '@nestjs/common';
import { createLogger, format, Logger, transports } from 'winston';

/**
 * Winston-backed application logger with contextual output and secret redaction.
 *
 * The service implements NestJS's logger contract. Consumers may inject the
 * global instance or use `forRoot` when a dedicated context is needed.
 */
@Injectable()
export class LoggerService implements NestLoggerService {
  private static readonly SENSITIVE_VALUE_PATTERN =
    /\b([A-Za-z0-9_-]*(?:api[_-]?key|client[_-]?secret|cookie|pass(?:word)?|private[_-]?key|secret|session|token)[A-Za-z0-9_-]*\b\s*[:=]\s*)([^,\s;]+)/gi;
  private static readonly SENSITIVE_JSON_VALUE_PATTERN =
    /("(?:[A-Za-z0-9_-]*(?:api[_-]?key|client[_-]?secret|cookie|pass(?:word)?|private[_-]?key|secret|session|token)[A-Za-z0-9_-]*)"\s*:\s*")([^"]+)(")/gi;
  private static readonly AUTHORIZATION_HEADER_PATTERN =
    /\b(Authorization\s*:\s*)(?:Bearer|Basic)\s+[^,\s;]+/gi;
  private static readonly BEARER_TOKEN_PATTERN =
    /\b(Bearer\s+)[A-Za-z0-9\-._~+/]+=*\b/gi;

  private context?: string;
  private readonly logger: Logger;

  /**
   * Creates an application logger and its console transport.
   *
   * The global provider factory and bootstrap supply context labels so Nest
   * never attempts to resolve this string parameter as a dependency.
   *
   * @param context Optional default context included with each entry.
   */
  constructor(context?: string) {
    this.context = context;
    this.logger = createLogger({
      level: process.env.LOG_LEVEL || 'info',
      format: format.combine(
        format.timestamp(),
        format.printf((entry) => {
          const timestamp =
            typeof entry.timestamp === 'string'
              ? entry.timestamp
              : new Date().toISOString();
          const level =
            typeof entry.level === 'string'
              ? entry.level.toUpperCase()
              : 'INFO';
          const entryContext =
            typeof entry.context === 'string' ? entry.context : undefined;

          return `[${timestamp}] [${level}] ${entryContext ? `[${entryContext}] ` : ''}${this.serializeMessage(entry.message)}`;
        }),
      ),
      transports: [new transports.Console()],
    });
  }

  /**
   * Creates a logger labeled for one application area.
   *
   * Bootstrap and future services use this factory when they cannot use the
   * injected global instance directly.
   *
   * @param context Context label included with log entries.
   * @returns A new logger bound to the supplied context.
   */
  static forRoot(context: string): LoggerService {
    return new LoggerService(context);
  }

  /**
   * Updates the default context used by subsequent entries.
   *
   * Injected consumers use this to identify their application area.
   *
   * @param context Context label included with log entries.
   * @returns Nothing.
   */
  setContext(context: string): void {
    this.context = context;
  }

  /**
   * Writes an informational entry for normal application activity.
   *
   * @param message Message or structured payload to serialize and redact.
   * @param context Optional context overriding the default context.
   * @returns Nothing.
   */
  log(message: unknown, context?: string): void {
    this.printMessage('log', message, context || this.context);
  }

  /**
   * Writes an error entry, optionally including a sanitized stack trace.
   *
   * @param message Error message, error instance, or structured payload.
   * @param trace Optional stack trace associated with the error.
   * @param context Optional context overriding the default context.
   * @returns Nothing.
   */
  error(message: unknown, trace?: string, context?: string): void {
    const messageWithTrace = trace
      ? `${this.serializeMessage(message)} | trace=${this.sanitizeString(trace)}`
      : message;
    this.printMessage('error', messageWithTrace, context || this.context);
  }

  /**
   * Writes a warning entry for recoverable or unexpected conditions.
   *
   * @param message Message or structured payload to serialize and redact.
   * @param context Optional context overriding the default context.
   * @returns Nothing.
   */
  warn(message: unknown, context?: string): void {
    this.printMessage('warn', message, context || this.context);
  }

  /**
   * Writes a debug entry used during diagnosis and local development.
   *
   * @param message Message or structured payload to serialize and redact.
   * @param context Optional context overriding the default context.
   * @returns Nothing.
   */
  debug(message: unknown, context?: string): void {
    this.printMessage('debug', message, context || this.context);
  }

  /**
   * Writes a verbose entry with fine-grained runtime detail.
   *
   * @param message Message or structured payload to serialize and redact.
   * @param context Optional context overriding the default context.
   * @returns Nothing.
   */
  verbose(message: unknown, context?: string): void {
    this.printMessage('verbose', message, context || this.context);
  }

  /**
   * Normalizes a NestJS log level and forwards the entry to Winston.
   *
   * @param level NestJS log level to write.
   * @param message Message payload to serialize.
   * @param context Optional context label.
   * @returns Nothing.
   */
  private printMessage(
    level: LogLevel,
    message: unknown,
    context?: string,
  ): void {
    const normalizedMessage = this.serializeMessage(message);
    if (level === 'log') {
      this.logger.info(normalizedMessage, { context });
      return;
    }

    this.logger.log(level, normalizedMessage, { context });
  }

  /**
   * Converts a message payload into a sanitized string for console output.
   *
   * @param message Message payload supplied by a logger consumer.
   * @returns A serialized string with common secret patterns redacted.
   */
  private serializeMessage(message: unknown): string {
    if (typeof message === 'string') {
      return this.sanitizeString(message);
    }
    if (message instanceof Error) {
      return `${message.name}: ${this.sanitizeString(message.message)}`;
    }
    if (typeof message === 'object' && message !== null) {
      try {
        return this.sanitizeString(JSON.stringify(message));
      } catch {
        return '[unserializable object payload]';
      }
    }

    return this.sanitizeString(String(message));
  }

  /**
   * Redacts obvious credential and token patterns from a string.
   *
   * @param value Raw log message or stack trace.
   * @returns The sanitized log value.
   */
  private sanitizeString(value: string): string {
    return value
      .replace(LoggerService.AUTHORIZATION_HEADER_PATTERN, '$1[REDACTED]')
      .replace(LoggerService.BEARER_TOKEN_PATTERN, '$1[REDACTED]')
      .replace(LoggerService.SENSITIVE_JSON_VALUE_PATTERN, '$1[REDACTED]$3')
      .replace(LoggerService.SENSITIVE_VALUE_PATTERN, '$1[REDACTED]');
  }
}
