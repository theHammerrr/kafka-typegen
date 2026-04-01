export type RuntimeTransport = '@platformatic/kafka' | 'kafkajs';

export type SubjectNameStrategy = 'event-name' | 'topic-name' | 'topic-event';

export interface KafkaTypegenSchemaRegistryConfig {
  readonly url: string;
  readonly subjectStrategy?: SubjectNameStrategy;
}

export interface KafkaTypegenRuntimeConfig {
  readonly transport?: RuntimeTransport;
  readonly module?: string;
}

export interface KafkaTypegenSourcesConfig {
  readonly rootDir?: string;
}

export interface KafkaTypegenGenerationConfig {
  readonly clientName?: string;
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
}

export interface KafkaTypegenConfig {
  readonly outputDir: string;
  readonly schemaRegistry?: KafkaTypegenSchemaRegistryConfig;
  readonly runtime?: KafkaTypegenRuntimeConfig;
  readonly sources?: KafkaTypegenSourcesConfig;
  readonly generation?: KafkaTypegenGenerationConfig;
  readonly naming?: KafkaTypegenNamingConfig;
  readonly topics: readonly KafkaTypegenTopicConfig[];
}

export interface NormalizedSchemaRegistryConfig {
  readonly url: string;
  readonly subjectStrategy: SubjectNameStrategy;
}

export interface NormalizedRuntimeConfig {
  readonly transport: RuntimeTransport;
  readonly module: string;
}

export interface NormalizedSourcesConfig {
  readonly rootDir: string;
}

export interface NormalizedGenerationConfig {
  readonly clientName: string;
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
  readonly events: readonly NormalizedEventConfig[];
}

export interface NormalizedKafkaTypegenConfig {
  readonly outputDir: string;
  readonly resolvedOutputDir: string;
  readonly schemaRegistry?: NormalizedSchemaRegistryConfig;
  readonly runtime: NormalizedRuntimeConfig;
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
