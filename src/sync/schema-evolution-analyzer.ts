import { analyzeEnumChanges } from './schema-evolution-enums.js';
import { analyzeRecordFields } from './schema-evolution-records.js';

interface AvroRecordSchema {
  readonly fields?: unknown;
  readonly type?: unknown;
}

function parseSchema(schemaText: string): unknown {
  try {
    return JSON.parse(schemaText);
  } catch {
    return undefined;
  }
}

function isRecordSchema(schema: unknown): schema is AvroRecordSchema {
  return typeof schema === 'object' && schema !== null && (schema as { type?: unknown }).type === 'record';
}

export function analyzeSchemaEvolution(
  previousSchemaText: string,
  nextSchemaText: string
): readonly string[] {
  const previousSchema = parseSchema(previousSchemaText);
  const nextSchema = parseSchema(nextSchemaText);

  if (!isRecordSchema(previousSchema) || !isRecordSchema(nextSchema)) {
    return [];
  }

  return [
    ...analyzeRecordFields(previousSchema, nextSchema),
    ...analyzeEnumChanges(previousSchema, nextSchema)
  ];
}
