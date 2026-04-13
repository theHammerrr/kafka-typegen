import { Buffer } from 'node:buffer';

import type { ResolvedKafkaTypegenObservability } from '../observability.js';
import {
  type CachedSchemaEntry,
  getSchemaById,
  getSchemaForSubject
} from './schema-registry-schema-cache.js';
import {
  decodeSchemaRegistryWireFormat,
  encodeSchemaRegistryWireFormat
} from './schema-registry-wire-format.js';
import {
  emitSchemaRegistryFailure,
  emitSchemaRegistryStart,
  emitSchemaRegistrySuccess
} from './schema-registry-observability.js';
import type {
  RuntimeEventMetadata,
  RuntimeIncomingMessage,
  RuntimeSerializationHooks,
  SchemaRegistryRuntimeClient
} from './types.js';

export function createSchemaRegistrySerialization(
  schemaRegistry: SchemaRegistryRuntimeClient,
  observability: ResolvedKafkaTypegenObservability
): RuntimeSerializationHooks {
  const schemasById = new Map<number, CachedSchemaEntry>();
  const schemasBySubject = new Map<string, CachedSchemaEntry>();

  return {
    async deserialize<TPayload>(
      metadata: RuntimeEventMetadata,
      message: RuntimeIncomingMessage
    ): Promise<TPayload> {
      await emitSchemaRegistryStart(observability, metadata, 'deserialize');
      let decodedPayload;

      try {
        decodedPayload = decodeSchemaRegistryWireFormat(message.value);
      } catch (error) {
        await emitSchemaRegistryFailure(observability, metadata, 'deserialize', error);
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
        const payload = schema.avroType.fromBuffer(Buffer.from(decodedPayload.payload)) as TPayload;
        await emitSchemaRegistrySuccess(observability, metadata, 'deserialize', decodedPayload.schemaId);
        return payload;
      } catch (error) {
        await emitSchemaRegistryFailure(
          observability,
          metadata,
          'deserialize',
          error,
          decodedPayload.schemaId
        );
        throw new Error(
          `Failed to deserialize event '${metadata.eventName}' with schema id '${decodedPayload.schemaId}': ${error instanceof Error ? error.message : 'Unknown error.'}`
        );
      }
    },
    async serialize(metadata: RuntimeEventMetadata, payload: unknown) {
      await emitSchemaRegistryStart(observability, metadata, 'serialize');
      const schema = await getSchemaForSubject(
        metadata.subjectName,
        schemaRegistry,
        schemasById,
        schemasBySubject
      );

      try {
        const encodedPayload = schema.avroType.toBuffer(payload);
        await emitSchemaRegistrySuccess(observability, metadata, 'serialize', schema.schemaId);

        return {
          schemaId: schema.schemaId,
          value: encodeSchemaRegistryWireFormat(schema.schemaId, encodedPayload)
        };
      } catch (error) {
        await emitSchemaRegistryFailure(observability, metadata, 'serialize', error, schema.schemaId);
        throw new Error(
          `Failed to serialize event '${metadata.eventName}' for subject '${metadata.subjectName}': ${error instanceof Error ? error.message : 'Unknown error.'}`
        );
      }
    }
  };
}
