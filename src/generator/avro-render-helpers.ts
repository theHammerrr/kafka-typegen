export interface AvroTypeRenderContext {
  readonly path: string;
  readonly references?: Readonly<Record<string, string>>;
  readonly semanticMode?: 'default' | 'safe';
}

export function createChildRenderContext(
  parentContext: AvroTypeRenderContext,
  path: string
): AvroTypeRenderContext {
  return parentContext.references === undefined
    ? {
        path,
        ...(parentContext.semanticMode !== undefined
          ? { semanticMode: parentContext.semanticMode }
          : {})
      }
    : {
        path,
        references: parentContext.references,
        ...(parentContext.semanticMode !== undefined
          ? { semanticMode: parentContext.semanticMode }
          : {})
      };
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
