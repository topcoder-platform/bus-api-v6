/**
 * Parses an optional environment value as a boolean.
 *
 * The values `true`, `1`, `yes`, and `on` are treated as true; `false`, `0`,
 * `no`, and `off` are treated as false. Unset or unrecognized values use the
 * supplied default. Configuration modules use this helper for boolean flags.
 *
 * @param value Raw environment variable value.
 * @param defaultValue Value returned when the input is absent or unrecognized.
 * @returns The parsed boolean or the supplied default.
 */
export function parseBoolean(
  value: string | undefined,
  defaultValue: boolean,
): boolean {
  if (value === undefined) {
    return defaultValue;
  }

  const normalizedValue = value.trim().toLowerCase();
  if (['true', '1', 'yes', 'on'].includes(normalizedValue)) {
    return true;
  }
  if (['false', '0', 'no', 'off'].includes(normalizedValue)) {
    return false;
  }

  return defaultValue;
}

/**
 * Parses an optional environment value as a non-negative integer.
 *
 * Configuration modules use this helper for ports, timeouts, and retry counts.
 * Invalid, negative, or unset inputs use the supplied default.
 *
 * @param value Raw environment variable value.
 * @param defaultValue Value returned when the input is absent or invalid.
 * @returns The parsed non-negative integer or the supplied default.
 */
export function parseInteger(
  value: string | undefined,
  defaultValue: number,
): number {
  if (value === undefined || value.trim() === '') {
    return defaultValue;
  }

  const parsedValue = Number(value);
  return Number.isInteger(parsedValue) && parsedValue >= 0
    ? parsedValue
    : defaultValue;
}

/**
 * Parses a comma-separated environment value into non-empty trimmed entries.
 *
 * Configuration modules use this helper for broker and similar value lists.
 *
 * @param value Raw comma-separated environment variable value.
 * @param defaultValue Entries returned when the input contains no values.
 * @returns A list of normalized entries or a copy of the supplied default.
 */
export function parseCommaSeparated(
  value: string | undefined,
  defaultValue: readonly string[],
): string[] {
  const entries = (value ?? '')
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);

  return entries.length > 0 ? entries : [...defaultValue];
}

/**
 * Parses a JSON environment value containing only strings.
 *
 * Authentication configuration uses this helper for accepted issuer lists.
 * Malformed JSON and arrays containing non-string entries use the default.
 *
 * @param value Raw JSON environment variable value.
 * @param defaultValue Entries returned when parsing or validation fails.
 * @returns A validated string array or a copy of the supplied default.
 */
export function parseJsonStringArray(
  value: string | undefined,
  defaultValue: readonly string[],
): string[] {
  if (value === undefined || value.trim() === '') {
    return [...defaultValue];
  }

  try {
    const parsedValue: unknown = JSON.parse(value);
    if (
      Array.isArray(parsedValue) &&
      parsedValue.every((entry) => typeof entry === 'string')
    ) {
      return parsedValue;
    }
  } catch {
    return [...defaultValue];
  }

  return [...defaultValue];
}
