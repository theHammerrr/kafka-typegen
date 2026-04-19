import { toTypeScriptType } from './avro-type-renderer.js';
import {
  emitEnumOrFixedDeclaration,
  registerNamedTypeReferences
} from './avro-declaration-helpers.js';
import { formatPropertyName, indent } from './render-utils.js';

function collectNamedRecord(
  typeRecord: Record<string, unknown>,
  path: string,
  references: Map<string, string>,
  declarationsByName: Map<string, string>,
  semanticMode: 'default' | 'safe'
): void {
  if (typeof typeRecord.name !== 'string' || !Array.isArray(typeRecord.fields)) {
    return;
  }

  const declaration = typeRecord.fields.map((field, index) => {
    const fieldRecord = field as Record<string, unknown>;
    const fieldName = typeof fieldRecord.name === 'string'
      ? formatPropertyName(fieldRecord.name)
      : "'unknown'";
    const fieldPath = `${path}.fields[${index}].type`;

    collectNestedDeclarations(
      fieldRecord.type,
      fieldPath,
      references,
      declarationsByName,
      semanticMode
    );

    return `${fieldName}: ${toTypeScriptType(fieldRecord.type, {
      path: fieldPath,
      references: Object.fromEntries(references),
      semanticMode
    })};`;
  }).join('\n');

  declarationsByName.set(
    typeRecord.name,
    `export interface ${typeRecord.name} {\n${indent(declaration)}\n}`
  );
}

export function collectNestedDeclarations(
  avroType: unknown,
  path: string,
  references: Map<string, string>,
  declarationsByName: Map<string, string>,
  semanticMode: 'default' | 'safe'
): void {
  if (Array.isArray(avroType)) {
    avroType.forEach((memberType, index) =>
      collectNestedDeclarations(
        memberType,
        `${path}[${index}]`,
        references,
        declarationsByName,
        semanticMode
      )
    );
    return;
  }

  if (avroType === null || typeof avroType !== 'object') {
    return;
  }

  const typeRecord = avroType as Record<string, unknown>;
  registerNamedTypeReferences(typeRecord, references);

  if (Array.isArray(typeRecord.fields)) {
    collectNamedRecord(typeRecord, path, references, declarationsByName, semanticMode);
  }

  emitEnumOrFixedDeclaration(typeRecord, declarationsByName);
  collectNestedDeclarations(typeRecord.type, `${path}.type`, references, declarationsByName, semanticMode);
  collectNestedDeclarations(typeRecord.items, `${path}.items`, references, declarationsByName, semanticMode);
  collectNestedDeclarations(typeRecord.values, `${path}.values`, references, declarationsByName, semanticMode);
}
