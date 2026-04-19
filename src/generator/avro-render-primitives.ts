import type { AvroTypeRenderContext } from './avro-render-helpers.js';
import { isNamedTypeReference, renderNamedTypeReference } from './avro-render-helpers.js';

const AVRO_PRIMITIVE_TYPES = new Set([
  'boolean',
  'bytes',
  'double',
  'float',
  'int',
  'long',
  'null',
  'string'
]);

export function renderLogicalType(logicalType: string, fallbackType: () => string): string {
  switch (logicalType) {
    case 'date': return 'AvroDate';
    case 'time-millis': return 'AvroTimeMillis';
    case 'timestamp-micros': return 'AvroTimestampMicros';
    case 'timestamp-millis': return 'AvroTimestampMillis';
    case 'decimal': return 'AvroDecimal';
    case 'uuid': return 'string';
    default: return fallbackType();
  }
}

export function renderPrimitiveType(avroType: string, context: AvroTypeRenderContext): string {
  switch (avroType) {
    case 'boolean': return 'boolean';
    case 'bytes': return 'Uint8Array';
    case 'double':
    case 'float':
    case 'int': return 'number';
    case 'long': return context.semanticMode === 'safe' ? 'bigint' : 'number';
    case 'null': return 'null';
    case 'string': return 'string';
    default:
      if (isNamedTypeReference(avroType) && !AVRO_PRIMITIVE_TYPES.has(avroType)) {
        return renderNamedTypeReference(avroType, context.references);
      }

      throw new Error(`Unsupported Avro type '${avroType}' at '${context.path}'.`);
  }
}
