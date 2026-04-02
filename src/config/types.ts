export type RuntimeTransport = '@platformatic/kafka' | 'kafkajs';

export type SubjectNameStrategy = 'event-name' | 'topic-name' | 'topic-event';

export interface KafkaTypegenSchemaRegistryAuthConfig {
  readonly password?: string;
  readonly token?: string;
  readonly username?: string;
}

export interface KafkaTypegenSchemaRegistryConfig {
  readonly auth?: KafkaTypegenSchemaRegistryAuthConfig;
  readonly url: string;
  readonly subjectStrategy?: SubjectNameStrategy;
}

export interface KafkaTypegenRuntimeConfig {
  readonly transport?: RuntimeTransport;
  readonly module?: string;
}

export interface KafkaTypegenSyncSaslConfig {
  readonly mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
  readonly password: string;
  readonly username: string;
}

export interface KafkaTypegenSyncKafkaConfig {
  readonly brokers: readonly string[];
  readonly clientId?: string;
  readonly failOnDrift?: boolean;
  readonly sasl?: KafkaTypegenSyncSaslConfig;
  readonly ssl?: boolean;
}

export interface KafkaTypegenSyncSchemaRegistryConfig {
  readonly failOnDrift?: boolean;
}

export interface KafkaTypegenSyncConfig {
  readonly kafka?: KafkaTypegenSyncKafkaConfig;
  readonly schemaRegistry?: KafkaTypegenSyncSchemaRegistryConfig;
}

export interface KafkaTypegenSourcesConfig {
  readonly rootDir?: string;
}

export interface KafkaTypegenGenerationConfig {
  readonly clientName?: string;
  readonly packageName?: string;
  readonly typesFileName?: string;
}

export interface KafkaTypegenNamingConfig {
  readonly eventTypeSuffix?: string;
  readonly topicTypeSuffix?: string;
}

export interface KafkaTypegenEventConfig {
  readonly name: string;
  readonly schemaPath: string;
  readonly keySchemaPath?: string;
  readonly subject?: string;
}

export interface KafkaTypegenTopicConfig {
  readonly name: string;
  readonly events: readonly KafkaTypegenEventConfig[];
  readonly keySchemaPath?: string;
  readonly subjectStrategy?: SubjectNameStrategy;
  readonly sync?: KafkaTypegenTopicSyncConfig;
}

export interface KafkaTypegenTopicSyncConfig {
  readonly cleanupPolicy?: 'compact' | 'compact,delete' | 'delete';
  readonly compressionType?:
    | 'gzip'
    | 'lz4'
    | 'producer'
    | 'snappy'
    | 'uncompressed'
    | 'zstd';
  readonly maxMessageBytes?: number;
  readonly minCompactionLagMs?: number;
  readonly partitions: number;
  readonly replicationFactor: number;
  readonly retentionBytes?: number;
  readonly retentionMs?: number;
}

export interface KafkaTypegenConfig {
  readonly outputDir: string;
  readonly schemaRegistry?: KafkaTypegenSchemaRegistryConfig;
  readonly runtime?: KafkaTypegenRuntimeConfig;
  readonly sync?: KafkaTypegenSyncConfig;
  readonly sources?: KafkaTypegenSourcesConfig;
  readonly generation?: KafkaTypegenGenerationConfig;
  readonly naming?: KafkaTypegenNamingConfig;
  readonly topics: readonly KafkaTypegenTopicConfig[];
}

export interface NormalizedSchemaRegistryConfig {
  readonly auth?: KafkaTypegenSchemaRegistryAuthConfig;
  readonly url: string;
  readonly subjectStrategy: SubjectNameStrategy;
}

export interface NormalizedRuntimeConfig {
  readonly transport: RuntimeTransport;
  readonly module: string;
}

export interface NormalizedSyncKafkaConfig {
  readonly brokers: readonly string[];
  readonly clientId: string;
  readonly failOnDrift: boolean;
  readonly sasl?: KafkaTypegenSyncSaslConfig;
  readonly ssl: boolean;
}

export interface NormalizedSyncSchemaRegistryConfig {
  readonly auth?: KafkaTypegenSchemaRegistryAuthConfig;
  readonly failOnDrift: boolean;
  readonly url: string;
}

export interface NormalizedSyncConfig {
  readonly kafka?: NormalizedSyncKafkaConfig;
  readonly schemaRegistry?: NormalizedSyncSchemaRegistryConfig;
}

export interface NormalizedSourcesConfig {
  readonly rootDir: string;
}

export interface NormalizedGenerationConfig {
  readonly clientName: string;
  readonly packageName?: string;
  readonly typesFileName: string;
}

export interface NormalizedNamingConfig {
  readonly eventTypeSuffix: string;
  readonly topicTypeSuffix: string;
}

export interface NormalizedEventConfig {
  readonly eventName: string;
  readonly topicName: string;
  readonly schemaPath: string;
  readonly resolvedSchemaPath: string;
  readonly keySchemaPath?: string;
  readonly resolvedKeySchemaPath?: string;
  readonly subjectName: string;
}

export interface NormalizedTopicConfig {
  readonly topicName: string;
  readonly keySchemaPath?: string;
  readonly resolvedKeySchemaPath?: string;
  readonly subjectStrategy: SubjectNameStrategy;
  readonly sync?: NormalizedTopicSyncConfig;
  readonly events: readonly NormalizedEventConfig[];
}

export interface NormalizedTopicSyncConfig {
  readonly cleanupPolicy?: 'compact' | 'compact,delete' | 'delete';
  readonly compressionType?:
    | 'gzip'
    | 'lz4'
    | 'producer'
    | 'snappy'
    | 'uncompressed'
    | 'zstd';
  readonly maxMessageBytes?: number;
  readonly minCompactionLagMs?: number;
  readonly partitions: number;
  readonly replicationFactor: number;
  readonly retentionBytes?: number;
  readonly retentionMs?: number;
}

export interface NormalizedKafkaTypegenConfig {
  readonly outputDir: string;
  readonly resolvedOutputDir: string;
  readonly schemaRegistry?: NormalizedSchemaRegistryConfig;
  readonly runtime: NormalizedRuntimeConfig;
  readonly sync?: NormalizedSyncConfig;
  readonly sources: NormalizedSourcesConfig;
  readonly generation: NormalizedGenerationConfig;
  readonly naming: NormalizedNamingConfig;
  readonly topics: readonly NormalizedTopicConfig[];
  readonly events: readonly NormalizedEventConfig[];
}

export interface ConfigValidationIssue {
  readonly path: string;
  readonly message: string;
}

export class ConfigValidationError extends Error {
  public readonly issues: readonly ConfigValidationIssue[];

  public constructor(issues: readonly ConfigValidationIssue[]) {
    super(
      `Invalid kafka-typegen config:\n${issues
        .map((issue) => `- ${issue.path}: ${issue.message}`)
        .join('\n')}`
    );

    this.name = 'ConfigValidationError';
    this.issues = issues;
  }
}
