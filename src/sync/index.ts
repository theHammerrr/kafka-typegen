export type {
  KafkaTypegenLogger,
  KafkaTypegenObservedEvent,
  KafkaTypegenObservabilityOptions,
  KafkaTypegenObserver
} from '../observability.js';
export { createSyncClients } from './clients.js';
export { executeSync } from './executor.js';
export { formatSyncResult } from './format.js';
export { buildSchemaRegistryPlan } from './schema-registry-plan.js';
export { buildKafkaTopicPlan } from './topic-plan.js';
export type {
  DesiredKafkaTopic,
  DesiredSchemaRegistrySubject,
  KafkaAdminClient,
  RemoteKafkaTopic,
  RemoteSchemaRegistrySubject,
  SchemaRegistryClient,
  SyncCliOptions,
  SyncClients,
  SyncContext,
  SyncExecutionOptions,
  SyncExecutionResult,
  SyncOperation,
  SyncTarget
} from './types.js';
