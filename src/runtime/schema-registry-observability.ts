import { emitObservedEvent, toErrorString, type ResolvedKafkaTypegenObservability } from '../observability.js';
import type { RuntimeEventMetadata } from './types.js';

export async function emitSchemaRegistryStart(
  observability: ResolvedKafkaTypegenObservability,
  metadata: RuntimeEventMetadata,
  operation: 'deserialize' | 'serialize'
): Promise<void> {
  await emitObservedEvent(observability, {
    eventName: metadata.eventName,
    subjectName: metadata.subjectName,
    type: `runtime.schema-registry.${operation}.start`
  });
}

export async function emitSchemaRegistrySuccess(
  observability: ResolvedKafkaTypegenObservability,
  metadata: RuntimeEventMetadata,
  operation: 'deserialize' | 'serialize',
  schemaId?: number
): Promise<void> {
  await emitObservedEvent(observability, {
    eventName: metadata.eventName,
    ...(schemaId !== undefined ? { schemaId } : {}),
    subjectName: metadata.subjectName,
    type: `runtime.schema-registry.${operation}.success`
  });
}

export async function emitSchemaRegistryFailure(
  observability: ResolvedKafkaTypegenObservability,
  metadata: RuntimeEventMetadata,
  operation: 'deserialize' | 'serialize',
  error: unknown,
  schemaId?: number
): Promise<void> {
  const errorMessage = toErrorString(error);

  observability.logger.error(`Failed to ${operation} Schema Registry payload.`, {
    error: errorMessage,
    eventName: metadata.eventName,
    ...(schemaId !== undefined ? { schemaId } : {}),
    subjectName: metadata.subjectName
  });
  await emitObservedEvent(observability, {
    error: errorMessage,
    eventName: metadata.eventName,
    ...(schemaId !== undefined ? { schemaId } : {}),
    subjectName: metadata.subjectName,
    type: `runtime.schema-registry.${operation}.failure`
  });
}
