import type { NormalizedEventConfig } from '../config/index.js';

import { FileSystemSchemaLoader } from './loader.js';
import { toEventSchemaInput } from './event-input.js';
import {
  parseSchemasWithSharedRegistry,
  parseSchemaWithRegistry
} from './parser-registry.js';
import type {
  EventSchemaDefinition,
  EventSchemaInput,
  EventSchemaLoader,
  EventSchemaLoadOptions,
  ParsedSchema,
  SchemaLoadResult,
  SchemaParser
} from './types.js';

export class AvroSchemaParser implements SchemaParser {
  public async parse(result: SchemaLoadResult): Promise<ParsedSchema> {
    return parseSchemaWithRegistry(result);
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

  public async loadEventSchemas(
    events: readonly EventSchemaInput[] | readonly NormalizedEventConfig[],
    options: EventSchemaLoadOptions = {}
  ): Promise<readonly EventSchemaDefinition[]> {
    const schemaInputs = events.map((event) => toEventSchemaInput(event));
    const uniqueFilePaths = [...new Set(schemaInputs.map((input) => input.filePath))];
    const loadedSchemas = await Promise.all(
      uniqueFilePaths.map((filePath) => this.loader.load({ filePath }))
    );
    const parsedSchemas = parseSchemasWithSharedRegistry(
      loadedSchemas,
      options.externalTypeMappings
    );
    const schemasByPath = new Map(
      parsedSchemas.map((schema) => [schema.filePath, schema])
    );
    const definitions = schemaInputs.map((input) => ({
      eventName: input.eventName,
      schema: schemasByPath.get(input.filePath) as ParsedSchema,
      subjectName: input.subjectName,
      topicName: input.topicName
    }));

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
