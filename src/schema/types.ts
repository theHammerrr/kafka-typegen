import type { Type } from 'avsc';

import type { NormalizedEventConfig } from '../config/index.js';

export interface SchemaSource {
  readonly filePath: string;
}

export interface SchemaLoadResult {
  readonly filePath: string;
  readonly rawSchema: string;
  readonly source: SchemaSource;
}

export interface SchemaLoader {
  load(source: SchemaSource): Promise<SchemaLoadResult>;
}

export interface ParsedSchemaField {
  readonly name: string;
  readonly path: string;
  readonly rawType: unknown;
  readonly type: string;
}

export interface ParsedSchema {
  readonly avroType: Type;
  readonly fields: readonly ParsedSchemaField[];
  readonly filePath: string;
  readonly name: string;
  readonly namespace?: string;
  readonly rawSchema: Record<string, unknown>;
}

export interface SchemaParser {
  parse(result: SchemaLoadResult): Promise<ParsedSchema>;
}

export interface EventSchemaDefinition {
  readonly eventName: string;
  readonly schema: ParsedSchema;
  readonly subjectName: string;
  readonly topicName: string;
}

export interface EventSchemaInput {
  readonly eventName: string;
  readonly filePath: string;
  readonly subjectName: string;
  readonly topicName: string;
}

export interface EventSchemaLoader {
  loadEventSchema(input: EventSchemaInput): Promise<EventSchemaDefinition>;
  loadEventSchemas(events: readonly EventSchemaInput[] | readonly NormalizedEventConfig[]): Promise<readonly EventSchemaDefinition[]>;
}
