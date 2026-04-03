import type { SyncExecutionResult } from './types.js';

export function formatSyncResult(result: SyncExecutionResult): string {
  const summary = result.operations.reduce(
    (counts, operation) => ({ ...counts, [operation.action]: counts[operation.action] + 1 }),
    { create: 0, drift: 0, noop: 0, update: 0 }
  );

  return [
    `${result.applied ? 'Applied' : 'Planned'} ${result.operations.length} sync operation(s).`,
    ...result.operations.map(
      (operation) => `[${operation.target}] ${operation.action.toUpperCase()} ${operation.resourceName}: ${operation.details}`
    ),
    `Summary: ${summary.create} create, ${summary.update} update, ${summary.noop} noop, ${summary.drift} drift.`
  ].join('\n');
}
