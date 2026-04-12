import { SchemaParseError } from './errors.js';
import type { ParsedNamedSchemaInput } from './parser-root.js';

const MISSING_TYPE_MESSAGE = 'undefined type name:';

function toSchemaFullName(input: ParsedNamedSchemaInput): string {
  return typeof input.rawSchemaRoot.namespace === 'string'
    ? `${input.rawSchemaRoot.namespace}.${String(input.rawSchemaRoot.name)}`
    : String(input.rawSchemaRoot.name);
}

export function assertUniqueRootNames(
  inputs: readonly ParsedNamedSchemaInput[]
): void {
  const seenByFullName = new Map<string, string>();

  for (const input of inputs) {
    const fullName = toSchemaFullName(input);
    const existingFilePath = seenByFullName.get(fullName);

    if (existingFilePath !== undefined) {
      throw new SchemaParseError(
        input.filePath,
        `Duplicate top-level Avro schema name '${fullName}' in '${existingFilePath}' and '${input.filePath}'.`
      );
    }

    seenByFullName.set(fullName, input.filePath);
  }
}

export function shouldRetryAfterMissingReference(error: unknown): boolean {
  return error instanceof Error &&
    error.message.includes(MISSING_TYPE_MESSAGE);
}

export function createUnresolvedSchemaError(
  pendingFiles: readonly string[]
): SchemaParseError {
  return new SchemaParseError(
    pendingFiles[0]!,
    `Failed to resolve Avro named references across schema files: ${pendingFiles
      .map((filePath) => `'${filePath}'`)
      .sort()
      .join(', ')}.`
  );
}
