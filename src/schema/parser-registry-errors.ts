import { SchemaParseError } from './errors.js';
import type { ParsedRecordSchemaInput } from './parser-record.js';

const MISSING_TYPE_MESSAGE = 'undefined type name:';

function toSchemaFullName(input: ParsedRecordSchemaInput): string {
  return typeof input.rawSchemaRecord.namespace === 'string'
    ? `${input.rawSchemaRecord.namespace}.${String(input.rawSchemaRecord.name)}`
    : String(input.rawSchemaRecord.name);
}

export function assertUniqueRootNames(
  inputs: readonly ParsedRecordSchemaInput[]
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
