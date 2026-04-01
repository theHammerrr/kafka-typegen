import { Type } from 'avsc';
import type { Schema } from 'avsc';

import type { NormalizedEventConfig } from '../config/index.js';

import { SchemaParseError } from './errors.js';
import { FileSystemSchemaLoader } from './loader.js';
import type {
  EventSchemaDefinition,
  EventSchemaInput,
  EventSchemaLoader,
  ParsedSchema,
  ParsedSchemaField,
  SchemaLoadResult,
  SchemaParser
} from './types.js';

function formatFieldType(fieldType: unknown): string {
  if (typeof fieldType === 'string') {
    return fieldType;
  }

  if (Array.isArray(fieldType)) {
    return fieldType.map((entry) => formatFieldType(entry)).join(' | ');
  }

  if (fieldType !== null && typeof fieldType === 'object') {
    const typeRecord = fieldType as Record<string, unknown>;

    if (typeof typeRecord.type === 'string') {
      return typeRecord.type;
    }
  }

  return JSON.stringify(fieldType);
}

function normalizeField(recordName: string, field: Record<string, unknown>): ParsedSchemaField {
  const fieldName = field.name;

  if (typeof fieldName !== 'string' || fieldName.length === 0) {
    throw new Error('Encountered an Avro record field without a valid name.');
  }

  return {
    name: fieldName,
    path: `${recordName}.${fieldName}`,
    rawType: field.type,
    type: formatFieldType(field.type)
  };
}

function toEventSchemaInput(event: EventSchemaInput | NormalizedEventConfig): EventSchemaInput {
  if ('filePath' in event) {
    return event;
  }

  return {
    eventName: event.eventName,
    filePath: event.resolvedSchemaPath,
    subjectName: event.subjectName,
    topicName: event.topicName
  };
}

export class AvroSchemaParser implements SchemaParser {
  public async parse(result: SchemaLoadResult): Promise<ParsedSchema> {
    let rawSchema: unknown;

    try {
      rawSchema = JSON.parse(result.rawSchema);
    } catch (error) {
      throw new SchemaParseError(
        result.filePath,
        `Schema file '${result.filePath}' does not contain valid JSON: ${error instanceof Error ? error.message : 'Unknown error.'}`,
        { cause: error }
      );
    }

    if (rawSchema === null || typeof rawSchema !== 'object' || Array.isArray(rawSchema)) {
      throw new SchemaParseError(
        result.filePath,
        `Schema file '${result.filePath}' must contain a top-level Avro record object.`
      );
    }

    const rawSchemaRecord = rawSchema as Record<string, unknown>;

    if (rawSchemaRecord.type !== 'record') {
      throw new SchemaParseError(
        result.filePath,
        `Schema file '${result.filePath}' must define a top-level Avro record.`
      );
    }

    if (typeof rawSchemaRecord.name !== 'string' || rawSchemaRecord.name.length === 0) {
      throw new SchemaParseError(
        result.filePath,
        `Schema file '${result.filePath}' must define a non-empty record name.`
      );
    }

    if (!Array.isArray(rawSchemaRecord.fields)) {
      throw new SchemaParseError(
        result.filePath,
        `Schema file '${result.filePath}' must define a 'fields' array for the record.`
      );
    }

    try {
      const avroType = Type.forSchema(rawSchemaRecord as Schema);
      const fields = rawSchemaRecord.fields.map((field) =>
        normalizeField(rawSchemaRecord.name as string, field as Record<string, unknown>)
      );

      return {
        avroType,
        fields,
        filePath: result.filePath,
        name: rawSchemaRecord.name,
        ...(typeof rawSchemaRecord.namespace === 'string'
          ? { namespace: rawSchemaRecord.namespace }
          : {}),
        rawSchema: rawSchemaRecord
      };
    } catch (error) {
      throw new SchemaParseError(
        result.filePath,
        `Failed to parse Avro schema '${result.filePath}': ${error instanceof Error ? error.message : 'Unknown error.'}`,
        { cause: error }
      );
    }
  }
}

class DefaultEventSchemaLoader implements EventSchemaLoader {
  private readonly loader = new FileSystemSchemaLoader();
  private readonly parser = new AvroSchemaParser();

  public async loadEventSchema(input: EventSchemaInput): Promise<EventSchemaDefinition> {
    const loadedSchema = await this.loader.load({ filePath: input.filePath });
    const parsedSchema = await this.parser.parse(loadedSchema);

    return {
      eventName: input.eventName,
      schema: parsedSchema,
      subjectName: input.subjectName,
      topicName: input.topicName
    };
  }

  public async loadEventSchemas(
    events: readonly EventSchemaInput[] | readonly NormalizedEventConfig[]
  ): Promise<readonly EventSchemaDefinition[]> {
    const definitions = await Promise.all(
      events.map((event) => this.loadEventSchema(toEventSchemaInput(event)))
    );

    return definitions.sort((leftDefinition, rightDefinition) => {
      const topicComparison = leftDefinition.topicName.localeCompare(rightDefinition.topicName);

      if (topicComparison !== 0) {
        return topicComparison;
      }

      return leftDefinition.eventName.localeCompare(rightDefinition.eventName);
    });
  }
}

export function createSchemaParser(): SchemaParser {
  return new AvroSchemaParser();
}

export function createEventSchemaLoader(): EventSchemaLoader {
  return new DefaultEventSchemaLoader();
}
