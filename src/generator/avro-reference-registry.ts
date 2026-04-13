import type { EventCatalog } from '../catalog/index.js';

import { getNamedTypeEntries, registerNamedTypeReferences } from './avro-declaration-helpers.js';

function collectTypeReferences(
  avroType: unknown,
  references: Map<string, string>,
  generatedTypeNames: Set<string>
): void {
  if (Array.isArray(avroType)) {
    avroType.forEach((memberType) =>
      collectTypeReferences(memberType, references, generatedTypeNames)
    );
    return;
  }

  if (avroType === null || typeof avroType !== 'object') {
    return;
  }

  const typeRecord = avroType as Record<string, unknown>;
  if (typeof typeRecord.name === 'string') {
    generatedTypeNames.add(typeRecord.name);
  }
  registerNamedTypeReferences(typeRecord, references);
  collectTypeReferences(typeRecord.type, references, generatedTypeNames);
  collectTypeReferences(typeRecord.items, references, generatedTypeNames);
  collectTypeReferences(typeRecord.values, references, generatedTypeNames);

  if (Array.isArray(typeRecord.fields)) {
    for (const field of typeRecord.fields) {
      collectTypeReferences(
        (field as Record<string, unknown>).type,
        references,
        generatedTypeNames
      );
    }
  }
}

export function collectCatalogAvroReferences(
  catalog: EventCatalog
): Readonly<Record<string, string>> {
  const references = new Map<string, string>();
  const generatedTypeNames = new Set<string>(
    catalog.events.map((event) => event.payloadTypeName)
  );

  for (const [avroFullName, typeScriptType] of Object.entries(
    catalog.config.generation.avroExternalTypes
  )) {
    const segments = avroFullName.split('.');
    const shortName = segments.at(-1) ?? avroFullName;
    const namespace =
      segments.length > 1 ? segments.slice(0, -1).join('.') : undefined;

    for (const [referenceName, targetName] of getNamedTypeEntries(
      shortName,
      namespace,
      typeScriptType
    )) {
      setReference(references, referenceName, targetName);
    }
  }

  for (const event of catalog.events) {
    generatedTypeNames.add(event.schema.name);

    for (const [referenceName, targetName] of getNamedTypeEntries(
      event.schema.name,
      event.schema.namespace,
      event.payloadTypeName
    )) {
      setReference(references, referenceName, targetName);
    }

    if (Array.isArray(event.schema.rawSchema.fields)) {
      for (const field of event.schema.rawSchema.fields as unknown[]) {
        collectTypeReferences(
          (field as Record<string, unknown>).type,
          references,
          generatedTypeNames
        );
      }
    }
  }

  assertNoExternalTypeNameCollisions(
    catalog.config.generation.avroExternalTypes,
    generatedTypeNames
  );

  return Object.fromEntries(references);
}

function setReference(
  references: Map<string, string>,
  referenceName: string,
  targetName: string
): void {
  const existingTargetName = references.get(referenceName);

  if (existingTargetName !== undefined && existingTargetName !== targetName) {
    throw new Error(
      `Conflicting Avro named type reference '${referenceName}' maps to both '${existingTargetName}' and '${targetName}'.`
    );
  }

  references.set(referenceName, targetName);
}

function assertNoExternalTypeNameCollisions(
  externalTypes: Readonly<Record<string, string>>,
  generatedTypeNames: ReadonlySet<string>
): void {
  for (const [avroFullName, typeScriptType] of Object.entries(externalTypes)) {
    if (generatedTypeNames.has(typeScriptType)) {
      throw new Error(
        `Configured external Avro type '${avroFullName}' collides with generated type '${typeScriptType}'.`
      );
    }
  }
}
