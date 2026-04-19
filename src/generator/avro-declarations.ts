import {
  emitEnumOrFixedDeclaration,
  getNamedTypeEntries
} from './avro-declaration-helpers.js';
import { collectNestedDeclarations } from './avro-declaration-collector.js';

export interface AvroDeclarationOutput {
  readonly declarations: readonly string[];
  readonly references: Readonly<Record<string, string>>;
}

export function collectAvroDeclarations(
  rootSchema: Record<string, unknown>,
  rootTypeName: string,
  sharedReferences: Readonly<Record<string, string>> = {},
  semanticMode: 'default' | 'safe' = 'default'
): AvroDeclarationOutput {
  const references = new Map<string, string>(
    Object.entries(sharedReferences)
  );
  for (const [referenceName, targetName] of getNamedTypeEntries(
    String(rootSchema.name),
    typeof rootSchema.namespace === 'string' ? rootSchema.namespace : undefined,
    rootTypeName
  )) {
    references.set(referenceName, targetName);
  }
  const declarationsByName = new Map<string, string>();
  emitEnumOrFixedDeclaration(rootSchema, declarationsByName);

  const rootFields = Array.isArray(rootSchema.fields) ? rootSchema.fields : [];
  rootFields.forEach((field, index) => {
    const fieldRecord = field as Record<string, unknown>;
    collectNestedDeclarations(
      fieldRecord.type,
      `${String(rootSchema.name)}.fields[${index}].type`,
      references,
      declarationsByName,
      semanticMode
    );
  });

  return {
    declarations: [...declarationsByName.values()],
    references: Object.fromEntries(references)
  };
}
