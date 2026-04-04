import avsc from 'avsc';
import type { Type } from 'avsc';

import { SchemaParseError } from './errors.js';
import type { ParsedSchema, SchemaLoadResult } from './types.js';
import { normalizeField } from './field-format.js';
import {
  type ParsedRecordSchemaInput,
  parseRecordSchema,
  toRecordSchema
} from './parser-record.js';
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
  readonly input: ParsedRecordSchemaInput;
  readonly result: SchemaLoadResult;
}

function toParsedSchema(input: ParsedRecordSchemaInput, avroType: Type): ParsedSchema {
  const recordName = input.rawSchemaRecord.name as string;
  const fields = (input.rawSchemaRecord.fields as unknown[]).map((field) =>
    normalizeField(recordName, field as Record<string, unknown>)
  );

  return {
    avroType,
    fields,
    filePath: input.filePath,
    name: recordName,
    ...(typeof input.rawSchemaRecord.namespace === 'string'
      ? { namespace: input.rawSchemaRecord.namespace }
      : {}),
    rawSchema: input.rawSchemaRecord
  };
}

export function parseSchemaWithRegistry(
  result: SchemaLoadResult,
  registry?: Record<string, Type>
): ParsedSchema {
  const input = parseRecordSchema(result);

  try {
    const avroType = registry === undefined
      ? AvroType.forSchema(toRecordSchema(input))
      : AvroType.forSchema(toRecordSchema(input), { registry });

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
  results: readonly SchemaLoadResult[]
): readonly ParsedSchema[] {
  const registry: Record<string, Type> = {};
  const parsedByPath = new Map<string, ParsedSchema>();
  const pending: PendingSchemaParse[] = results.map((result) => ({
    input: parseRecordSchema(result),
    result
  }));

  assertUniqueRootNames(pending.map((entry) => entry.input));

  while (pending.length > 0) {
    let parsedInPass = false;

    for (let index = pending.length - 1; index >= 0; index -= 1) {
      const entry = pending[index]!;
      const registrySnapshot = cloneAvroRegistry(registry);

      try {
        const avroType = AvroType.forSchema(toRecordSchema(entry.input), {
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
