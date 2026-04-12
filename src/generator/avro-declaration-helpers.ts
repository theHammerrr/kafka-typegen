import { formatLiteral } from './render-utils.js';

export function getNamedTypeEntries(
  typeName: string,
  namespace: string | undefined,
  targetName: string
): readonly [string, string][] {
  const entries: Array<[string, string]> = [
    [typeName, targetName],
  ];

  if (namespace !== undefined) {
    entries.push([`${namespace}.${typeName}`, targetName]);
  }

  return entries;
}

export function registerNamedTypeReferences(
  typeRecord: Record<string, unknown>,
  references: Map<string, string>
): void {
  if (typeof typeRecord.name !== 'string') {
    return;
  }

  for (const [referenceName, targetName] of getNamedTypeEntries(
    typeRecord.name,
    typeof typeRecord.namespace === 'string' ? typeRecord.namespace : undefined,
    typeRecord.name
  )) {
    const existingTargetName = references.get(referenceName);
    if (
      existingTargetName !== undefined &&
      existingTargetName !== targetName
    ) {
      throw new Error(
        `Conflicting Avro named type reference '${referenceName}' maps to both '${existingTargetName}' and '${targetName}'.`
      );
    }

    references.set(referenceName, targetName);
  }
}

function setNamedDeclaration(
  declarationsByName: Map<string, string>,
  typeName: string,
  declaration: string
): void {
  const existingDeclaration = declarationsByName.get(typeName);

  if (
    existingDeclaration !== undefined &&
    existingDeclaration !== declaration
  ) {
    throw new Error(
      `Conflicting Avro named type declaration '${typeName}' was generated with incompatible definitions.`
    );
  }

  declarationsByName.set(typeName, declaration);
}

export function emitEnumOrFixedDeclaration(
  typeRecord: Record<string, unknown>,
  declarationsByName: Map<string, string>
): void {
  if (Array.isArray(typeRecord.symbols) && typeof typeRecord.name === 'string') {
    setNamedDeclaration(
      declarationsByName,
      typeRecord.name,
      `export type ${typeRecord.name} = ${typeRecord.symbols
        .map((symbol) => formatLiteral(String(symbol)))
        .join(' | ')};`
    );
  }

  if (typeRecord.type === 'fixed' && typeof typeRecord.name === 'string') {
    setNamedDeclaration(
      declarationsByName,
      typeRecord.name,
      `export type ${typeRecord.name} = Uint8Array;`
    );
  }
}
