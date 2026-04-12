import { formatLiteral, formatPropertyName, indent } from './render-utils.js';
import {
  type AvroTypeRenderContext,
  createChildRenderContext,
  renderLogicalType,
  renderNamedTypeReference,
  renderPrimitiveType,
  shouldRenderNamedReference
} from './avro-render-helpers.js';

export function toTypeScriptType(
  avroType: unknown,
  context: AvroTypeRenderContext = { path: 'schema' }
): string {
  if (typeof avroType === 'string') {
    return renderPrimitiveType(avroType, context);
  }

  if (Array.isArray(avroType)) {
    return avroType
      .map((memberType, index) =>
        toTypeScriptType(
          memberType,
          createChildRenderContext(context, `${context.path}[${index}]`)
        )
      )
      .join(' | ');
  }

  if (avroType !== null && typeof avroType === 'object') {
    return renderComplexType(avroType as Record<string, unknown>, context);
  }

  throw new Error(
    `Unsupported Avro type definition at '${context.path}': ${JSON.stringify(avroType)}.`
  );
}

function renderComplexType(
  typeRecord: Record<string, unknown>,
  context: AvroTypeRenderContext
): string {
  if (typeof typeRecord.logicalType === 'string') {
    return renderLogicalType(typeRecord.logicalType, () =>
      toTypeScriptType(
        typeRecord.type,
        createChildRenderContext(context, `${context.path}.type`)
      )
    );
  }

  if (shouldRenderNamedReference(typeRecord, context.references)) {
    return renderNamedTypeReference(typeRecord.name, context.references);
  }

  switch (typeRecord.type) {
    case 'array':
      return `${toTypeScriptType(
        typeRecord.items,
        createChildRenderContext(context, `${context.path}.items`)
      )}[]`;
    case 'enum':
      return Array.isArray(typeRecord.symbols)
        ? typeRecord.symbols.map((symbol) => formatLiteral(String(symbol))).join(' | ')
        : 'string';
    case 'fixed':
      return 'Uint8Array';
    case 'map':
      return `Record<string, ${toTypeScriptType(
        typeRecord.values,
        createChildRenderContext(context, `${context.path}.values`)
      )}>`;
    case 'record':
      return renderInlineRecord(typeRecord, context);
    default:
      if (typeof typeRecord.type === 'string') {
        return renderPrimitiveType(typeRecord.type, {
          ...createChildRenderContext(context, `${context.path}.type`)
        });
      }

      throw new Error(
        `Unsupported Avro complex type at '${context.path}': ${JSON.stringify(typeRecord)}.`
      );
  }
}

function renderInlineRecord(
  typeRecord: Record<string, unknown>,
  context: AvroTypeRenderContext
): string {
  const fields = Array.isArray(typeRecord.fields)
    ? typeRecord.fields.map((field, index) => {
        const fieldRecord = field as Record<string, unknown>;
        const propertyName =
          typeof fieldRecord.name === 'string' ? formatPropertyName(fieldRecord.name) : "'unknown'";

        return `${propertyName}: ${toTypeScriptType(fieldRecord.type, {
          ...createChildRenderContext(
            context,
            `${context.path}.fields[${index}].type`
          )
        })};`;
      })
    : [];

  return `{\n${indent(fields.join('\n'))}\n}`;
}
