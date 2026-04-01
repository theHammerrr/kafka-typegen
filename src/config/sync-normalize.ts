import type { KafkaTypegenConfig, NormalizedSyncConfig } from './types.js';

export function normalizeSyncConfig(config: KafkaTypegenConfig): NormalizedSyncConfig | undefined {
  const normalizedSchemaRegistryUrl = config.sync?.schemaRegistry?.url ?? config.schemaRegistry?.url;

  return {
    ...(config.sync?.kafka !== undefined
      ? {
          kafka: {
            brokers: config.sync.kafka.brokers,
            clientId: config.sync.kafka.clientId ?? 'kafka-typegen-sync',
            failOnDrift: config.sync.kafka.failOnDrift ?? false,
            ssl: config.sync.kafka.ssl ?? false,
            ...(config.sync.kafka.sasl !== undefined ? { sasl: config.sync.kafka.sasl } : {})
          }
        }
      : {}),
    ...(normalizedSchemaRegistryUrl !== undefined
      ? {
          schemaRegistry: {
            failOnDrift: config.sync?.schemaRegistry?.failOnDrift ?? false,
            url: normalizedSchemaRegistryUrl,
            ...(config.sync?.schemaRegistry?.password !== undefined
              ? { password: config.sync.schemaRegistry.password }
              : {}),
            ...(config.sync?.schemaRegistry?.username !== undefined
              ? { username: config.sync.schemaRegistry.username }
              : {})
          }
        }
      : {})
  };
}
