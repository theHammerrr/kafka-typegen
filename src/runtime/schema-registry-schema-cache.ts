import avsc from 'avsc';
import type { Schema } from 'avsc';

import type {
  SchemaRegistryRuntimeClient,
  SchemaRegistryRuntimeSchema
} from './types.js';

const { Type } = avsc;

export type AvroType = ReturnType<typeof Type.forSchema>;

export interface CachedSchemaEntry {
  readonly avroType: AvroType;
  readonly schemaId: number;
}

export function normalizeSchemaDefinition(
  schemaDefinition: SchemaRegistryRuntimeSchema,
  source: string
): CachedSchemaEntry {
  const rawSchema =
    typeof schemaDefinition.schema === 'string'
      ? JSON.parse(schemaDefinition.schema)
      : schemaDefinition.schema;

  if (
    rawSchema === null ||
    typeof rawSchema !== 'object' ||
    Array.isArray(rawSchema)
  ) {
    throw new Error(
      `Schema Registry ${source} must resolve to a top-level Avro schema object.`
    );
  }

  return {
    avroType: Type.forSchema(rawSchema as Schema),
    schemaId: schemaDefinition.schemaId
  };
}

export async function getSchemaForSubject(
  subjectName: string,
  schemaRegistry: SchemaRegistryRuntimeClient,
  schemasById: Map<number, CachedSchemaEntry>,
  schemasBySubject: Map<string, CachedSchemaEntry>
): Promise<CachedSchemaEntry> {
  const cachedSchema = schemasBySubject.get(subjectName);
  if (cachedSchema !== undefined) {
    return cachedSchema;
  }

  const resolvedSchema = normalizeSchemaDefinition(
    await schemaRegistry.getLatestSchema(subjectName),
    `subject '${subjectName}'`
  );

  schemasBySubject.set(subjectName, resolvedSchema);
  schemasById.set(resolvedSchema.schemaId, resolvedSchema);
  return resolvedSchema;
}

export async function getSchemaById(
  schemaId: number,
  schemaRegistry: SchemaRegistryRuntimeClient,
  schemasById: Map<number, CachedSchemaEntry>
): Promise<CachedSchemaEntry> {
  const cachedSchema = schemasById.get(schemaId);
  if (cachedSchema !== undefined) {
    return cachedSchema;
  }

  const resolvedSchema = normalizeSchemaDefinition(
    await schemaRegistry.getSchemaById(schemaId),
    `schema id '${schemaId}'`
  );

  schemasById.set(schemaId, resolvedSchema);
  return resolvedSchema;
}
