import type { ResolvedKafkaTypegenObservability } from '../observability.js';
import { createConfluentSchemaRegistryRuntimeClient } from './confluent-schema-registry-client.js';
import { createSchemaRegistrySerialization } from './schema-registry-serialization.js';
import type {
  RuntimeSchemaRegistry,
  RuntimeSerializationHooks,
  RuntimeSerializationOptions,
  SchemaRegistryRuntimeClient
} from './types.js';

function isSchemaRegistryRuntimeClient(
  schemaRegistry: RuntimeSchemaRegistry
): schemaRegistry is SchemaRegistryRuntimeClient {
  return (
    typeof schemaRegistry === 'object' &&
    schemaRegistry !== null &&
    'getLatestSchema' in schemaRegistry &&
    typeof schemaRegistry.getLatestSchema === 'function' &&
    'getSchemaById' in schemaRegistry &&
    typeof schemaRegistry.getSchemaById === 'function'
  );
}

export function resolveRuntimeSerialization(
  options: RuntimeSerializationOptions,
  observability: ResolvedKafkaTypegenObservability
): RuntimeSerializationHooks {
  const hasSchemaRegistry = 'schemaRegistry' in options && options.schemaRegistry !== undefined;
  const hasSerialization = 'serialization' in options && options.serialization !== undefined;

  if (hasSchemaRegistry === hasSerialization) {
    throw new Error(
      "Runtime helpers require exactly one of 'serialization' or 'schemaRegistry'."
    );
  }

  return hasSerialization
    ? options.serialization
    : createSchemaRegistrySerialization(
        isSchemaRegistryRuntimeClient(options.schemaRegistry)
          ? options.schemaRegistry
          : createConfluentSchemaRegistryRuntimeClient(options.schemaRegistry),
        observability
      );
}
