import { formatLiteral, formatPropertyName, indent } from './render-utils.js';

export function toTypeScriptType(avroType: unknown): string {
  if (typeof avroType === 'string') {
    return renderPrimitiveType(avroType);
  }

  if (Array.isArray(avroType)) {
    return avroType.map((memberType) => toTypeScriptType(memberType)).join(' | ');
  }

  if (avroType !== null && typeof avroType === 'object') {
    return renderComplexType(avroType as Record<string, unknown>);
  }

  return 'unknown';
}

function renderPrimitiveType(avroType: string): string {
  switch (avroType) {
    case 'boolean':
      return 'boolean';
    case 'bytes':
      return 'Uint8Array';
    case 'double':
    case 'float':
    case 'int':
    case 'long':
      return 'number';
    case 'null':
      return 'null';
    case 'string':
      return 'string';
    default:
      return 'unknown';
  }
}

function renderComplexType(typeRecord: Record<string, unknown>): string {
  if (typeof typeRecord.logicalType === 'string') {
    return typeRecord.logicalType === 'uuid' ? 'string' : toTypeScriptType(typeRecord.type);
  }

  switch (typeRecord.type) {
    case 'array':
      return `${toTypeScriptType(typeRecord.items)}[]`;
    case 'enum':
      return Array.isArray(typeRecord.symbols)
        ? typeRecord.symbols.map((symbol) => formatLiteral(String(symbol))).join(' | ')
        : 'string';
    case 'fixed':
      return 'Uint8Array';
    case 'map':
      return `Record<string, ${toTypeScriptType(typeRecord.values)}>`;
    case 'record':
      return renderInlineRecord(typeRecord);
    default:
      return renderPrimitiveType(String(typeRecord.type));
  }
}

function renderInlineRecord(typeRecord: Record<string, unknown>): string {
  const fields = Array.isArray(typeRecord.fields)
    ? typeRecord.fields.map((field) => {
        const fieldRecord = field as Record<string, unknown>;
        const propertyName =
          typeof fieldRecord.name === 'string' ? formatPropertyName(fieldRecord.name) : "'unknown'";

        return `${propertyName}: ${toTypeScriptType(fieldRecord.type)};`;
      })
    : [];

  return `{\n${indent(fields.join('\n'))}\n}`;
}
