export interface KafkaTypegenSchemaRegistryConfig {
  readonly url: string;
}

export interface KafkaTypegenRuntimeConfig {
  readonly clientModule?: string;
}

export interface KafkaTypegenEventConfig {
  readonly schemaPath: string;
  readonly keySchemaPath?: string;
}

export interface KafkaTypegenTopicConfig {
  readonly events: Record<string, KafkaTypegenEventConfig>;
}

export interface KafkaTypegenConfig {
  readonly outputDir: string;
  readonly schemaRegistry?: KafkaTypegenSchemaRegistryConfig;
  readonly runtime?: KafkaTypegenRuntimeConfig;
  readonly topics: Record<string, KafkaTypegenTopicConfig>;
}

export interface NormalizedEventConfig {
  readonly eventName: string;
  readonly topicName: string;
  readonly schemaPath: string;
  readonly keySchemaPath?: string;
}

export interface NormalizedTopicConfig {
  readonly topicName: string;
  readonly events: readonly NormalizedEventConfig[];
}

export interface NormalizedKafkaTypegenConfig {
  readonly outputDir: string;
  readonly schemaRegistry?: KafkaTypegenSchemaRegistryConfig;
  readonly runtime?: KafkaTypegenRuntimeConfig;
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
