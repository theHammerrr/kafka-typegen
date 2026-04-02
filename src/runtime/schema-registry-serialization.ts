import { Buffer } from 'node:buffer';

import {
  type CachedSchemaEntry,
  getSchemaById,
  getSchemaForSubject
} from './schema-registry-schema-cache.js';
import {
  decodeSchemaRegistryWireFormat,
  encodeSchemaRegistryWireFormat
} from './schema-registry-wire-format.js';
import type {
  RuntimeEventMetadata,
  RuntimeIncomingMessage,
  RuntimeSerializationHooks,
  SchemaRegistryRuntimeClient
} from './types.js';

export function createSchemaRegistrySerialization(
  schemaRegistry: SchemaRegistryRuntimeClient
): RuntimeSerializationHooks {
  const schemasById = new Map<number, CachedSchemaEntry>();
  const schemasBySubject = new Map<string, CachedSchemaEntry>();

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

      const schema = await getSchemaById(
        decodedPayload.schemaId,
        schemaRegistry,
        schemasById
      );

      try {
        return schema.avroType.fromBuffer(Buffer.from(decodedPayload.payload)) as TPayload;
      } catch (error) {
        throw new Error(
          `Failed to deserialize event '${metadata.eventName}' with schema id '${decodedPayload.schemaId}': ${error instanceof Error ? error.message : 'Unknown error.'}`
        );
      }
    },
    async serialize(metadata: RuntimeEventMetadata, payload: unknown) {
      const schema = await getSchemaForSubject(
        metadata.subjectName,
        schemaRegistry,
        schemasById,
        schemasBySubject
      );

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
