import { emitObservedEvent, resolveObservability, toErrorString } from '../observability.js';
import { assertSyncTargetConfigured, isKafkaSyncEnabled, isRegistrySyncEnabled } from './config.js';
import { executeKafkaSync } from './kafka-sync.js';
import { executeSchemaRegistrySync } from './schema-registry-sync.js';
import type { SyncContext, SyncExecutionOptions, SyncExecutionResult } from './types.js';

export async function executeSync(context: SyncContext, options: SyncExecutionOptions): Promise<SyncExecutionResult> {
  const observability = resolveObservability(options);

  await emitObservedEvent(observability, {
    apply: options.apply,
    target: options.target,
    type: 'sync.start'
  });

  try {
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

    for (const operation of operations) {
      await emitObservedEvent(observability, {
        action: operation.action,
        details: operation.details,
        resourceName: operation.resourceName,
        target: operation.target,
        type: 'sync.operation'
      });
    }

    await emitObservedEvent(observability, {
      apply: options.apply,
      operationCount: operations.length,
      target: options.target,
      type: 'sync.complete'
    });

    return { applied: options.apply, operations };
  } catch (error) {
    observability.logger.error('Sync execution failed.', {
      apply: options.apply,
      error: toErrorString(error),
      target: options.target
    });
    await emitObservedEvent(observability, {
      apply: options.apply,
      error: toErrorString(error),
      target: options.target,
      type: 'sync.failure'
    });
    throw error;
  }
}
