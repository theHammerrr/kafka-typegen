import type { Schema } from 'avsc';

import { SchemaParseError } from './errors.js';
import type { SchemaLoadResult } from './types.js';

export interface ParsedRecordSchemaInput {
  readonly filePath: string;
  readonly rawSchemaRecord: Record<string, unknown>;
}

export function parseRecordSchema(
  result: SchemaLoadResult
): ParsedRecordSchemaInput {
  let rawSchema: unknown;

  try {
    rawSchema = JSON.parse(result.rawSchema);
  } catch (error) {
    throw new SchemaParseError(
      result.filePath,
      `Schema file '${result.filePath}' does not contain valid JSON: ${error instanceof Error ? error.message : 'Unknown error.'}`,
      { cause: error }
    );
  }

  if (
    rawSchema === null ||
    typeof rawSchema !== 'object' ||
    Array.isArray(rawSchema)
  ) {
    throw new SchemaParseError(
      result.filePath,
      `Schema file '${result.filePath}' must contain a top-level Avro record object.`
    );
  }

  const rawSchemaRecord = rawSchema as Record<string, unknown>;
  validateRecordSchema(rawSchemaRecord, result.filePath);

  return {
    filePath: result.filePath,
    rawSchemaRecord
  };
}

export function toRecordSchema(
  schemaInput: ParsedRecordSchemaInput
): Schema {
  return schemaInput.rawSchemaRecord as Schema;
}

function validateRecordSchema(
  rawSchemaRecord: Record<string, unknown>,
  filePath: string
): void {
  if (rawSchemaRecord.type !== 'record') {
    throw new SchemaParseError(
      filePath,
      `Schema file '${filePath}' must define a top-level Avro record.`
    );
  }

  if (
    typeof rawSchemaRecord.name !== 'string' ||
    rawSchemaRecord.name.length === 0
  ) {
    throw new SchemaParseError(
      filePath,
      `Schema file '${filePath}' must define a non-empty record name.`
    );
  }

  if (!Array.isArray(rawSchemaRecord.fields)) {
    throw new SchemaParseError(
      filePath,
      `Schema file '${filePath}' must define a 'fields' array for the record.`
    );
  }
}
