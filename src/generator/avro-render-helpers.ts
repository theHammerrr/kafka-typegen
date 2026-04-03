export interface AvroTypeRenderContext {
  readonly path: string;
  readonly references?: Readonly<Record<string, string>>;
}

export function createChildRenderContext(
  parentContext: AvroTypeRenderContext,
  path: string
): AvroTypeRenderContext {
  return parentContext.references === undefined
    ? { path }
    : { path, references: parentContext.references };
}

export function shouldRenderNamedReference(
  typeRecord: Record<string, unknown>,
  references: Readonly<Record<string, string>> | undefined
): typeRecord is { name: string } {
  return typeof typeRecord.name === 'string'
    && references?.[typeRecord.name] !== undefined
    && (typeRecord.type === 'record'
      || typeRecord.type === 'enum'
      || typeRecord.type === 'fixed');
}

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

export function isNamedTypeReference(value: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/u.test(value);
}

export function renderNamedTypeReference(
  typeName: string,
  references: Readonly<Record<string, string>> | undefined
): string {
  return references?.[typeName] ?? typeName.split('.').at(-1) ?? typeName;
}

export function renderLogicalType(
  logicalType: string,
  fallbackType: () => string
): string {
  switch (logicalType) {
    case 'date':
    case 'time-millis':
    case 'timestamp-micros':
    case 'timestamp-millis':
      return 'number';
    case 'decimal':
      return 'Uint8Array';
    case 'uuid':
      return 'string';
    default:
      return fallbackType();
  }
}

export function renderPrimitiveType(
  avroType: string,
  context: AvroTypeRenderContext
): string {
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
      if (isNamedTypeReference(avroType) && !AVRO_PRIMITIVE_TYPES.has(avroType)) {
        return renderNamedTypeReference(avroType, context.references);
      }

      throw new Error(
        `Unsupported Avro type '${avroType}' at '${context.path}'.`
      );
  }
}
