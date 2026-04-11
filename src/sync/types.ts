import type { EventCatalog } from '../catalog/index.js';
import type { NormalizedKafkaTypegenConfig } from '../config/index.js';
import type { KafkaTypegenObservabilityOptions } from '../observability.js';

export type SyncTarget = 'all' | 'kafka' | 'registry';
export type SyncOperationAction = 'create' | 'drift' | 'noop';
export type SyncOperationTarget = 'kafka' | 'registry';

export interface DesiredKafkaTopic {
  readonly configEntries: Readonly<Record<string, string>>;
  readonly partitions: number;
  readonly replicationFactor: number;
  readonly topicName: string;
}

export interface DesiredSchemaRegistrySubject {
  readonly eventName: string;
  readonly schemaText: string;
  readonly subjectName: string;
  readonly topicName: string;
}

export interface SyncPlan {
  readonly kafkaTopics: readonly DesiredKafkaTopic[];
  readonly registrySubjects: readonly DesiredSchemaRegistrySubject[];
}

export interface RemoteKafkaTopic {
  readonly partitions: number;
  readonly replicationFactor: number;
  readonly topicName: string;
}

export interface RemoteSchemaRegistrySubject {
  readonly schemaText: string;
  readonly subjectName: string;
}

export interface KafkaAdminClient {
  createTopics(topics: readonly DesiredKafkaTopic[]): Promise<void>;
  listTopics(): Promise<readonly RemoteKafkaTopic[]>;
}

export interface SchemaRegistryClient {
  getLatestSubject(subjectName: string): Promise<RemoteSchemaRegistrySubject | undefined>;
  registerSubject(subject: DesiredSchemaRegistrySubject): Promise<void>;
}

export interface SyncClients {
  readonly kafkaAdmin?: KafkaAdminClient;
  readonly schemaRegistry?: SchemaRegistryClient;
}

export interface SyncExecutorOptions {
  readonly apply: boolean;
  readonly clients: SyncClients;
  readonly config: NormalizedKafkaTypegenConfig;
  readonly target: SyncTarget;
}

export interface SyncOperation {
  readonly action: SyncOperationAction;
  readonly details: string;
  readonly resourceName: string;
  readonly target: SyncOperationTarget;
}

export interface SyncExecutionResult {
  readonly applied: boolean;
  readonly operations: readonly SyncOperation[];
}

export interface SyncCliOptions {
  readonly apply: boolean;
  readonly json: boolean;
  readonly target: SyncTarget;
}

export interface SyncContext {
  readonly catalog: EventCatalog;
  readonly config: NormalizedKafkaTypegenConfig;
  readonly options: SyncCliOptions;
}

export type SyncExecutionOptions = SyncExecutorOptions & KafkaTypegenObservabilityOptions;
