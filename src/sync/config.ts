import type { NormalizedKafkaTypegenConfig } from '../config/index.js';

import type { SyncTarget } from './types.js';

export function isKafkaSyncEnabled(config: NormalizedKafkaTypegenConfig, target: SyncTarget): boolean {
  return (target === 'all' || target === 'kafka') && config.sync?.kafka !== undefined;
}

export function isRegistrySyncEnabled(config: NormalizedKafkaTypegenConfig, target: SyncTarget): boolean {
  return (target === 'all' || target === 'registry') && config.sync?.schemaRegistry !== undefined;
}

export function assertSyncTargetConfigured(config: NormalizedKafkaTypegenConfig, target: SyncTarget): void {
  if (target === 'kafka' && config.sync?.kafka === undefined) {
    throw new Error("Sync target 'kafka' requires sync.kafka configuration.");
  }

  if (target === 'registry' && config.sync?.schemaRegistry === undefined) {
    throw new Error("Sync target 'registry' requires a schema registry URL.");
  }

  if (target === 'all' && config.sync?.kafka === undefined && config.sync?.schemaRegistry === undefined) {
    throw new Error('Sync requires either sync.kafka or schemaRegistry configuration.');
  }
}
