import { toTypeScriptType } from './avro-type-renderer.js';
import {
  emitEnumOrFixedDeclaration,
  getNamedTypeEntries,
  registerNamedTypeReferences
} from './avro-declaration-helpers.js';
import { formatLiteral, formatPropertyName, indent } from './render-utils.js';

export interface AvroDeclarationOutput {
  readonly declarations: readonly string[];
  readonly references: Readonly<Record<string, string>>;
}

function collectType(
  avroType: unknown,
  path: string,
  references: Map<string, string>,
  declarationsByName: Map<string, string>
): void {
  if (Array.isArray(avroType)) {
    avroType.forEach((memberType, index) =>
      collectType(memberType, `${path}[${index}]`, references, declarationsByName)
    );
    return;
  }

  if (avroType === null || typeof avroType !== 'object') {
    return;
  }

  const typeRecord = avroType as Record<string, unknown>;
  registerNamedTypeReferences(typeRecord, references);

  if (Array.isArray(typeRecord.fields)) {
    collectNamedRecord(typeRecord, path, references, declarationsByName);
  }
  emitEnumOrFixedDeclaration(typeRecord, declarationsByName);
  collectType(typeRecord.type, `${path}.type`, references, declarationsByName);
  collectType(typeRecord.items, `${path}.items`, references, declarationsByName);
  collectType(typeRecord.values, `${path}.values`, references, declarationsByName);
}

function collectNamedRecord(
  typeRecord: Record<string, unknown>,
  path: string,
  references: Map<string, string>,
  declarationsByName: Map<string, string>
): void {
  if (typeof typeRecord.name !== 'string' || !Array.isArray(typeRecord.fields)) {
    return;
  }

  const declaration = typeRecord.fields
    .map((field, index) => {
      const fieldRecord = field as Record<string, unknown>;
      const fieldName =
        typeof fieldRecord.name === 'string' ? formatPropertyName(fieldRecord.name) : "'unknown'";
      collectType(
        fieldRecord.type,
        `${path}.fields[${index}].type`,
        references,
        declarationsByName
      );
      return `${fieldName}: ${toTypeScriptType(fieldRecord.type, {
        path: `${path}.fields[${index}].type`,
        references: Object.fromEntries(references)
      })};`;
    })
    .join('\n');

  declarationsByName.set(
    typeRecord.name,
    `export interface ${typeRecord.name} {\n${indent(declaration)}\n}`
  );
}

export function collectAvroDeclarations(
  rootSchema: Record<string, unknown>,
  rootTypeName: string
): AvroDeclarationOutput {
  const references = new Map<string, string>(
    getNamedTypeEntries(
      String(rootSchema.name),
      typeof rootSchema.namespace === 'string' ? rootSchema.namespace : undefined,
      rootTypeName
    )
  );
  const declarationsByName = new Map<string, string>();

  collectType(rootSchema, String(rootSchema.name), references, declarationsByName);
  declarationsByName.delete(String(rootSchema.name));

  return {
    declarations: [...declarationsByName.values()],
    references: Object.fromEntries(references)
  };
}
