import type { NormalizedKafkaTypegenConfig } from '../config/index.js';

import { KafkaJsAdminClient } from './kafka-admin-client.js';
import { HttpSchemaRegistryClient } from './schema-registry-client.js';
import type { SyncClients } from './types.js';

export function createSyncClients(config: NormalizedKafkaTypegenConfig): SyncClients {
  return {
    ...(config.sync?.kafka !== undefined ? { kafkaAdmin: new KafkaJsAdminClient(config.sync.kafka) } : {}),
    ...(config.sync?.schemaRegistry !== undefined
      ? { schemaRegistry: new HttpSchemaRegistryClient(config.sync.schemaRegistry) }
      : {})
  };
}
