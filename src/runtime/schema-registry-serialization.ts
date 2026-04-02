import { Buffer } from 'node:buffer';

import avsc from 'avsc';
import type { Schema } from 'avsc';

import { decodeSchemaRegistryWireFormat, encodeSchemaRegistryWireFormat } from './schema-registry-wire-format.js';
import type {
  RuntimeEventMetadata,
  RuntimeIncomingMessage,
  RuntimeSerializationHooks,
  SchemaRegistryRuntimeClient,
  SchemaRegistryRuntimeSchema
} from './types.js';

const { Type } = avsc;

type AvroType = ReturnType<typeof Type.forSchema>;

interface CachedSchemaEntry {
  readonly avroType: AvroType;
  readonly schemaId: number;
}

function normalizeSchemaDefinition(
  schemaDefinition: SchemaRegistryRuntimeSchema,
  source: string
): CachedSchemaEntry {
  const rawSchema =
    typeof schemaDefinition.schema === 'string'
      ? JSON.parse(schemaDefinition.schema)
      : schemaDefinition.schema;

  if (rawSchema === null || typeof rawSchema !== 'object' || Array.isArray(rawSchema)) {
    throw new Error(`Schema Registry ${source} must resolve to a top-level Avro schema object.`);
  }

  return {
    avroType: Type.forSchema(rawSchema as Schema),
    schemaId: schemaDefinition.schemaId
  };
}

export function createSchemaRegistrySerialization(
  schemaRegistry: SchemaRegistryRuntimeClient
): RuntimeSerializationHooks {
  const schemasById = new Map<number, CachedSchemaEntry>();
  const schemasBySubject = new Map<string, CachedSchemaEntry>();

  async function getSchemaForSubject(subjectName: string): Promise<CachedSchemaEntry> {
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

  async function getSchemaById(schemaId: number): Promise<CachedSchemaEntry> {
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

  return {
    async deserialize<TPayload>(
      metadata: RuntimeEventMetadata,
      message: RuntimeIncomingMessage
    ): Promise<TPayload> {
      let decodedPayload;

      try {
        decodedPayload = decodeSchemaRegistryWireFormat(message.value);
      } catch (error) {
        throw new Error(
          `Failed to read Schema Registry payload for event '${metadata.eventName}': ${error instanceof Error ? error.message : 'Unknown error.'}`
        );
      }

      const schema = await getSchemaById(decodedPayload.schemaId);

      try {
        return schema.avroType.fromBuffer(Buffer.from(decodedPayload.payload)) as TPayload;
      } catch (error) {
        throw new Error(
          `Failed to deserialize event '${metadata.eventName}' with schema id '${decodedPayload.schemaId}': ${error instanceof Error ? error.message : 'Unknown error.'}`
        );
      }
    },
    async serialize(metadata: RuntimeEventMetadata, payload: unknown) {
      const schema = await getSchemaForSubject(metadata.subjectName);

      try {
        const encodedPayload = schema.avroType.toBuffer(payload);

        return {
          schemaId: schema.schemaId,
          value: encodeSchemaRegistryWireFormat(schema.schemaId, encodedPayload)
        };
      } catch (error) {
        throw new Error(
          `Failed to serialize event '${metadata.eventName}' for subject '${metadata.subjectName}': ${error instanceof Error ? error.message : 'Unknown error.'}`
        );
      }
    }
  };
}
