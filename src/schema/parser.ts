import avsc from 'avsc';
import type { Schema } from 'avsc';

import type { NormalizedEventConfig } from '../config/index.js';

import { SchemaParseError } from './errors.js';
import { normalizeField } from './field-format.js';
import { FileSystemSchemaLoader } from './loader.js';
import { toEventSchemaInput } from './event-input.js';
import type { EventSchemaDefinition, EventSchemaInput, EventSchemaLoader, ParsedSchema, SchemaLoadResult, SchemaParser } from './types.js';

const { Type } = avsc;

function parseRecordSchema(result: SchemaLoadResult): Record<string, unknown> {
  let rawSchema: unknown;

  try {
    rawSchema = JSON.parse(result.rawSchema);
  } catch (error) {
    throw new SchemaParseError(result.filePath, `Schema file '${result.filePath}' does not contain valid JSON: ${error instanceof Error ? error.message : 'Unknown error.'}`, { cause: error });
  }

  if (rawSchema === null || typeof rawSchema !== 'object' || Array.isArray(rawSchema)) {
    throw new SchemaParseError(result.filePath, `Schema file '${result.filePath}' must contain a top-level Avro record object.`);
  }

  return rawSchema as Record<string, unknown>;
}

function validateRecordSchema(rawSchemaRecord: Record<string, unknown>, filePath: string): void {
  if (rawSchemaRecord.type !== 'record') {
    throw new SchemaParseError(filePath, `Schema file '${filePath}' must define a top-level Avro record.`);
  }
  if (typeof rawSchemaRecord.name !== 'string' || rawSchemaRecord.name.length === 0) {
    throw new SchemaParseError(filePath, `Schema file '${filePath}' must define a non-empty record name.`);
  }
  if (!Array.isArray(rawSchemaRecord.fields)) {
    throw new SchemaParseError(filePath, `Schema file '${filePath}' must define a 'fields' array for the record.`);
  }
}

export class AvroSchemaParser implements SchemaParser {
  public async parse(result: SchemaLoadResult): Promise<ParsedSchema> {
    const rawSchemaRecord = parseRecordSchema(result);
    validateRecordSchema(rawSchemaRecord, result.filePath);

    try {
      const avroType = Type.forSchema(rawSchemaRecord as Schema);
      const recordName = rawSchemaRecord.name as string;
      const fields = (rawSchemaRecord.fields as unknown[]).map((field) =>
        normalizeField(recordName, field as Record<string, unknown>)
      );

      return {
        avroType,
        fields,
        filePath: result.filePath,
        name: recordName,
        ...(typeof rawSchemaRecord.namespace === 'string' ? { namespace: rawSchemaRecord.namespace } : {}),
        rawSchema: rawSchemaRecord
      };
    } catch (error) {
      throw new SchemaParseError(result.filePath, `Failed to parse Avro schema '${result.filePath}': ${error instanceof Error ? error.message : 'Unknown error.'}`, { cause: error });
    }
  }
}

class DefaultEventSchemaLoader implements EventSchemaLoader {
  private readonly loader = new FileSystemSchemaLoader();
  private readonly parser = new AvroSchemaParser();

  public async loadEventSchema(input: EventSchemaInput): Promise<EventSchemaDefinition> {
    const loadedSchema = await this.loader.load({ filePath: input.filePath });
    const parsedSchema = await this.parser.parse(loadedSchema);

    return { eventName: input.eventName, schema: parsedSchema, subjectName: input.subjectName, topicName: input.topicName };
  }

  public async loadEventSchemas(events: readonly EventSchemaInput[] | readonly NormalizedEventConfig[]): Promise<readonly EventSchemaDefinition[]> {
    const definitions = await Promise.all(events.map((event) => this.loadEventSchema(toEventSchemaInput(event))));

    return definitions.sort((leftDefinition, rightDefinition) => {
      const topicComparison = leftDefinition.topicName.localeCompare(rightDefinition.topicName);
      return topicComparison !== 0 ? topicComparison : leftDefinition.eventName.localeCompare(rightDefinition.eventName);
    });
  }
}

export function createSchemaParser(): SchemaParser {
  return new AvroSchemaParser();
}

export function createEventSchemaLoader(): EventSchemaLoader {
  return new DefaultEventSchemaLoader();
}
