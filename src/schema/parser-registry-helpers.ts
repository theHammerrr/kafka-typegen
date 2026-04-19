import avsc from 'avsc';
import type { Schema, Type } from 'avsc';
import { normalizeField } from './field-format.js';
import type { ParsedSchema } from './types.js';
import type { ParsedNamedSchemaInput } from './parser-root.js';

const { Type: AvroType } = avsc;

export function toParsedSchema(input: ParsedNamedSchemaInput, avroType: Type): ParsedSchema {
  const schemaName = input.rawSchemaRoot.name as string;
  const fields = input.rootType === 'record'
    ? (input.rawSchemaRoot.fields as unknown[]).map((field) => normalizeField(schemaName, field as Record<string, unknown>))
    : [];

  return {
    avroType,
    fields,
    filePath: input.filePath,
    name: schemaName,
    ...(typeof input.rawSchemaRoot.namespace === 'string' ? { namespace: input.rawSchemaRoot.namespace } : {}),
    rootType: input.rootType,
    rawSchema: input.rawSchemaRoot
  };
}

export function createSeededRegistry(externalTypeMappings: Readonly<Record<string, string>>): Record<string, Type> {
  const registry: Record<string, Type> = {};

  for (const fullName of Object.keys(externalTypeMappings)) {
    registry[fullName] = AvroType.forSchema(toExternalPlaceholderSchema(fullName), { registry });
  }

  return registry;
}

function toExternalPlaceholderSchema(fullName: string): Schema {
  const segments = fullName.split('.');
  const name = segments.at(-1) ?? fullName;
  const namespace = segments.length > 1 ? segments.slice(0, -1).join('.') : undefined;

  return { fields: [], name, ...(namespace !== undefined ? { namespace } : {}), type: 'record' } as Schema;
}
