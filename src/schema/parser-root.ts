import type { Schema } from 'avsc';

import { SchemaParseError } from './errors.js';
import type { ParsedSchemaRootType, SchemaLoadResult } from './types.js';

const SUPPORTED_ROOT_TYPES = new Set<ParsedSchemaRootType>(['record', 'enum', 'fixed']);

export interface ParsedNamedSchemaInput {
  readonly filePath: string;
  readonly rawSchemaRoot: Record<string, unknown>;
  readonly rootType: ParsedSchemaRootType;
}

export function parseNamedSchema(
  result: SchemaLoadResult
): ParsedNamedSchemaInput {
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
      `Schema file '${result.filePath}' must contain a top-level named Avro schema object.`
    );
  }

  const rawSchemaRoot = rawSchema as Record<string, unknown>;
  const rootType = validateNamedSchema(rawSchemaRoot, result.filePath);

  return {
    filePath: result.filePath,
    rawSchemaRoot,
    rootType
  };
}

export function toNamedSchema(
  schemaInput: ParsedNamedSchemaInput
): Schema {
  return schemaInput.rawSchemaRoot as Schema;
}

function validateNamedSchema(
  rawSchemaRoot: Record<string, unknown>,
  filePath: string
): ParsedSchemaRootType {
  const rootType = rawSchemaRoot.type;

  if (
    typeof rootType !== 'string' ||
    !SUPPORTED_ROOT_TYPES.has(rootType as ParsedSchemaRootType)
  ) {
    throw new SchemaParseError(
      filePath,
      `Schema file '${filePath}' must define a top-level Avro record, enum, or fixed.`
    );
  }

  if (
    typeof rawSchemaRoot.name !== 'string' ||
    rawSchemaRoot.name.length === 0
  ) {
    throw new SchemaParseError(
      filePath,
      `Schema file '${filePath}' must define a non-empty ${rootType} name.`
    );
  }

  if (rootType === 'record' && !Array.isArray(rawSchemaRoot.fields)) {
    throw new SchemaParseError(
      filePath,
      `Schema file '${filePath}' must define a 'fields' array for the record.`
    );
  }

  if (rootType === 'enum' && !Array.isArray(rawSchemaRoot.symbols)) {
    throw new SchemaParseError(
      filePath,
      `Schema file '${filePath}' must define a 'symbols' array for the enum.`
    );
  }

  if (
    rootType === 'fixed' &&
    (!Number.isInteger(rawSchemaRoot.size) || Number(rawSchemaRoot.size) <= 0)
  ) {
    throw new SchemaParseError(
      filePath,
      `Schema file '${filePath}' must define a positive integer 'size' for the fixed type.`
    );
  }

  return rootType as ParsedSchemaRootType;
}
