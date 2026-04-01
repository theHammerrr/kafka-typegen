import { assertSyncTargetConfigured, isKafkaSyncEnabled, isRegistrySyncEnabled } from './config.js';
import { executeKafkaSync } from './kafka-sync.js';
import { executeSchemaRegistrySync } from './schema-registry-sync.js';
import type { SyncContext, SyncExecutionResult, SyncExecutorOptions } from './types.js';

export async function executeSync(context: SyncContext, options: SyncExecutorOptions): Promise<SyncExecutionResult> {
  assertSyncTargetConfigured(context.config, options.target);
  const operations = [];

  if (isKafkaSyncEnabled(context.config, options.target)) {
    if (options.clients.kafkaAdmin === undefined) {
      throw new Error('Kafka sync client is not configured.');
    }

    operations.push(...(await executeKafkaSync(context.config, options.clients.kafkaAdmin, options.apply)));
  }

  if (isRegistrySyncEnabled(context.config, options.target)) {
    if (options.clients.schemaRegistry === undefined) {
      throw new Error('Schema Registry sync client is not configured.');
    }

    operations.push(
      ...(await executeSchemaRegistrySync(context.catalog, context.config, options.clients.schemaRegistry, options.apply))
    );
  }

  return { applied: options.apply, operations };
}
