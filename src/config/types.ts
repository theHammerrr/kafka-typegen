export type RuntimeTransport = '@platformatic/kafka' | 'kafkajs';

export type SchemaRegistryCompatibility =
  | 'BACKWARD'
  | 'BACKWARD_TRANSITIVE'
  | 'FORWARD'
  | 'FORWARD_TRANSITIVE'
  | 'FULL'
  | 'FULL_TRANSITIVE'
  | 'NONE';

export type SchemaRegistryDriftAction = 'fail' | 'ignore' | 'register';

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
  readonly compatibility?: SchemaRegistryCompatibility;
  readonly failOnDrift?: boolean;
  readonly onDrift?: SchemaRegistryDriftAction;
}

export interface KafkaTypegenSyncConfig {
  readonly kafka?: KafkaTypegenSyncKafkaConfig;
  readonly schemaRegistry?: KafkaTypegenSyncSchemaRegistryConfig;
}

export interface KafkaTypegenSourcesConfig {
  readonly rootDir?: string;
}

export interface KafkaTypegenGenerationConfig {
  readonly avroExternalTypes?: Readonly<Record<string, string>>;
  readonly avroSemanticMode?: 'default' | 'safe';
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
  readonly compressionGzipLevel?: number;
  readonly compressionLz4Level?: number;
  readonly compressionType?:
    | 'gzip'
    | 'lz4'
    | 'producer'
    | 'snappy'
    | 'uncompressed'
    | 'zstd';
  readonly compressionZstdLevel?: number;
  readonly deleteRetentionMs?: number;
  readonly fileDeleteDelayMs?: number;
  readonly flushMessages?: number;
  readonly flushMs?: number;
  readonly followerReplicationThrottledReplicas?: readonly string[] | string;
  readonly indexIntervalBytes?: number;
  readonly leaderReplicationThrottledReplicas?: readonly string[] | string;
  readonly localRetentionBytes?: number;
  readonly localRetentionMs?: number;
  readonly maxCompactionLagMs?: number;
  readonly maxMessageBytes?: number;
  readonly messageTimestampAfterMaxMs?: number;
  readonly messageTimestampBeforeMaxMs?: number;
  readonly messageTimestampType?: 'CreateTime' | 'LogAppendTime';
  readonly minCleanableDirtyRatio?: number;
  readonly minCompactionLagMs?: number;
  readonly minInSyncReplicas?: number;
  readonly partitions: number;
  readonly preallocate?: boolean;
  readonly remoteLogCopyDisable?: boolean;
  readonly remoteLogDeleteOnDisable?: boolean;
  readonly remoteStorageEnable?: boolean;
  readonly replicationFactor: number;
  readonly retentionBytes?: number;
  readonly retentionMs?: number;
  readonly segmentBytes?: number;
  readonly segmentIndexBytes?: number;
  readonly segmentJitterMs?: number;
  readonly segmentMs?: number;
  readonly uncleanLeaderElectionEnable?: boolean;
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
  readonly compatibility?: SchemaRegistryCompatibility;
  readonly onDrift: SchemaRegistryDriftAction;
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
  readonly avroExternalTypes: Readonly<Record<string, string>>;
  readonly avroSemanticMode: 'default' | 'safe';
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
  readonly compressionGzipLevel?: number;
  readonly compressionLz4Level?: number;
  readonly compressionType?:
    | 'gzip'
    | 'lz4'
    | 'producer'
    | 'snappy'
    | 'uncompressed'
    | 'zstd';
  readonly compressionZstdLevel?: number;
  readonly deleteRetentionMs?: number;
  readonly fileDeleteDelayMs?: number;
  readonly flushMessages?: number;
  readonly flushMs?: number;
  readonly followerReplicationThrottledReplicas?: readonly string[] | string;
  readonly indexIntervalBytes?: number;
  readonly leaderReplicationThrottledReplicas?: readonly string[] | string;
  readonly localRetentionBytes?: number;
  readonly localRetentionMs?: number;
  readonly maxCompactionLagMs?: number;
  readonly maxMessageBytes?: number;
  readonly messageTimestampAfterMaxMs?: number;
  readonly messageTimestampBeforeMaxMs?: number;
  readonly messageTimestampType?: 'CreateTime' | 'LogAppendTime';
  readonly minCleanableDirtyRatio?: number;
  readonly minCompactionLagMs?: number;
  readonly minInSyncReplicas?: number;
  readonly partitions: number;
  readonly preallocate?: boolean;
  readonly remoteLogCopyDisable?: boolean;
  readonly remoteLogDeleteOnDisable?: boolean;
  readonly remoteStorageEnable?: boolean;
  readonly replicationFactor: number;
  readonly retentionBytes?: number;
  readonly retentionMs?: number;
  readonly segmentBytes?: number;
  readonly segmentIndexBytes?: number;
  readonly segmentJitterMs?: number;
  readonly segmentMs?: number;
  readonly uncleanLeaderElectionEnable?: boolean;
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
