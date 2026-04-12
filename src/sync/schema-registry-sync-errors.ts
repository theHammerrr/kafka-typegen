import type { SyncOperation } from './types.js';

export function assertSchemaRegistryDriftAllowed(
  operations: readonly SyncOperation[],
  failOnDrift: boolean
): void {
  if (!failOnDrift) {
    return;
  }

  if (operations.some((operation) => operation.action === 'drift')) {
    throw new Error(
      'Schema Registry sync detected subject drift and sync.schemaRegistry.onDrift is set to fail.'
    );
  }
}
