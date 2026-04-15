import type { NormalizedKafkaTypegenConfig } from '../config/index.js';
import type { ParsedSchema } from '../schema/index.js';

export interface CatalogRuntimeEventMetadata {
  readonly eventName: string;
  readonly schemaFilePath: string;
  readonly subjectName: string;
  readonly topicName: string;
}

export interface CatalogEvent {
  readonly eventName: string;
  readonly payloadTypeName: string;
  readonly schema: ParsedSchema;
  readonly schemaName: string;
  readonly subjectName: string;
  readonly topicName: string;
  readonly topicTypeName: string;
  readonly runtime: CatalogRuntimeEventMetadata;
}

export interface CatalogTopic {
  readonly eventNames: readonly string[];
  readonly events: readonly CatalogEvent[];
  readonly propertyName: string;
  readonly subjectStrategy: string;
  readonly topicName: string;
  readonly topicTypeName: string;
}

export interface EventCatalog {
  readonly config: NormalizedKafkaTypegenConfig;
  readonly events: readonly CatalogEvent[];
  readonly topics: readonly CatalogTopic[];
}

export interface CatalogBuilder {
  build(config: NormalizedKafkaTypegenConfig): Promise<EventCatalog>;
}
