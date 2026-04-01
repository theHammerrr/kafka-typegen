import type { ParsedSchemaField } from './types.js';

export function formatFieldType(fieldType: unknown): string {
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

export function normalizeField(recordName: string, field: Record<string, unknown>): ParsedSchemaField {
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
