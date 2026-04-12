import avsc from 'avsc';
import type { Schema, Type } from 'avsc';

import { SchemaParseError } from './errors.js';
import type { ParsedSchema, SchemaLoadResult } from './types.js';
import { normalizeField } from './field-format.js';
import {
  type ParsedNamedSchemaInput,
  parseNamedSchema,
  toNamedSchema
} from './parser-root.js';
import {
  assertUniqueRootNames,
  createUnresolvedSchemaError,
  shouldRetryAfterMissingReference
} from './parser-registry-errors.js';
import {
  cloneAvroRegistry,
  restoreAvroRegistry
} from './parser-registry-state.js';

const { Type: AvroType } = avsc;

interface PendingSchemaParse {
  readonly input: ParsedNamedSchemaInput;
  readonly result: SchemaLoadResult;
}

function toParsedSchema(input: ParsedNamedSchemaInput, avroType: Type): ParsedSchema {
  const schemaName = input.rawSchemaRoot.name as string;
  const fields = input.rootType === 'record'
    ? (input.rawSchemaRoot.fields as unknown[]).map((field) =>
        normalizeField(schemaName, field as Record<string, unknown>)
      )
    : [];

  return {
    avroType,
    fields,
    filePath: input.filePath,
    name: schemaName,
    ...(typeof input.rawSchemaRoot.namespace === 'string'
      ? { namespace: input.rawSchemaRoot.namespace }
      : {}),
    rootType: input.rootType,
    rawSchema: input.rawSchemaRoot
  };
}

export function parseSchemaWithRegistry(
  result: SchemaLoadResult,
  registry?: Record<string, Type>
): ParsedSchema {
  const input = parseNamedSchema(result);

  try {
    const avroType = registry === undefined
      ? AvroType.forSchema(toNamedSchema(input))
      : AvroType.forSchema(toNamedSchema(input), { registry });

    return toParsedSchema(input, avroType);
  } catch (error) {
    throw new SchemaParseError(
      result.filePath,
      `Failed to parse Avro schema '${result.filePath}': ${error instanceof Error ? error.message : 'Unknown error.'}`,
      { cause: error }
    );
  }
}

export function parseSchemasWithSharedRegistry(
  results: readonly SchemaLoadResult[],
  externalTypeMappings: Readonly<Record<string, string>> = {}
): readonly ParsedSchema[] {
  const registry: Record<string, Type> = createSeededRegistry(externalTypeMappings);
  const parsedByPath = new Map<string, ParsedSchema>();
  const pending: PendingSchemaParse[] = results.map((result) => ({
    input: parseNamedSchema(result),
    result
  }));

  assertUniqueRootNames(pending.map((entry) => entry.input));

  while (pending.length > 0) {
    let parsedInPass = false;

    for (let index = pending.length - 1; index >= 0; index -= 1) {
      const entry = pending[index]!;
      const registrySnapshot = cloneAvroRegistry(registry);

      try {
        const avroType = AvroType.forSchema(toNamedSchema(entry.input), {
          registry
        });

        parsedByPath.set(entry.result.filePath, toParsedSchema(entry.input, avroType));
        pending.splice(index, 1);
        parsedInPass = true;
      } catch (error) {
        if (!shouldRetryAfterMissingReference(error)) {
          throw new SchemaParseError(
            entry!.result.filePath,
            `Failed to parse Avro schema '${entry!.result.filePath}': ${error instanceof Error ? error.message : 'Unknown error.'}`,
            { cause: error }
          );
        }

        restoreAvroRegistry(registry, registrySnapshot);
      }
    }

    if (!parsedInPass) {
      throw createUnresolvedSchemaError(
        pending.map((entry) => entry.result.filePath)
      );
    }
  }

  return results.map((result) => parsedByPath.get(result.filePath) as ParsedSchema);
}

function createSeededRegistry(
  externalTypeMappings: Readonly<Record<string, string>>
): Record<string, Type> {
  const registry: Record<string, Type> = {};

  for (const fullName of Object.keys(externalTypeMappings)) {
    registry[fullName] = AvroType.forSchema(
      toExternalPlaceholderSchema(fullName),
      { registry }
    );
  }

  return registry;
}

function toExternalPlaceholderSchema(fullName: string): Schema {
  const segments = fullName.split('.');
  const name = segments.at(-1) ?? fullName;
  const namespace = segments.length > 1
    ? segments.slice(0, -1).join('.')
    : undefined;

  return {
    fields: [],
    name,
    ...(namespace !== undefined ? { namespace } : {}),
    type: 'record'
  } as Schema;
}
