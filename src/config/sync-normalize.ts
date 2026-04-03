import type { KafkaTypegenConfig, NormalizedSyncConfig } from './types.js';

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
            failOnDrift: config.sync?.schemaRegistry?.failOnDrift ?? false,
            url: config.schemaRegistry.url,
            ...(config.schemaRegistry.auth !== undefined
              ? { auth: config.schemaRegistry.auth }
              : {})
          }
        }
      : {})
  };
}
