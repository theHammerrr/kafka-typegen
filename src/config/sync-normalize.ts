import type { KafkaTypegenConfig, NormalizedSyncConfig } from './types.js';

function normalizeSchemaRegistryOnDrift(config: KafkaTypegenConfig): 'fail' | 'ignore' | 'register' {
  if (config.sync?.schemaRegistry?.onDrift !== undefined) {
    return config.sync.schemaRegistry.onDrift;
  }

  return config.sync?.schemaRegistry?.failOnDrift === true ? 'fail' : 'register';
}

export function normalizeSyncConfig(config: KafkaTypegenConfig): NormalizedSyncConfig | undefined {
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
    ...(config.schemaRegistry !== undefined
      ? {
          schemaRegistry: {
            onDrift: normalizeSchemaRegistryOnDrift(config),
            url: config.schemaRegistry.url,
            ...(config.sync?.schemaRegistry?.compatibility !== undefined
              ? { compatibility: config.sync.schemaRegistry.compatibility }
              : {}),
            ...(config.schemaRegistry.auth !== undefined
              ? { auth: config.schemaRegistry.auth }
              : {})
          }
        }
      : {})
  };
}
