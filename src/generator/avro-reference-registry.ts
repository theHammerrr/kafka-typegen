import type { EventCatalog } from '../catalog/index.js';

import { getNamedTypeEntries, registerNamedTypeReferences } from './avro-declaration-helpers.js';

function collectTypeReferences(
  avroType: unknown,
  references: Map<string, string>
): void {
  if (Array.isArray(avroType)) {
    avroType.forEach((memberType) => collectTypeReferences(memberType, references));
    return;
  }

  if (avroType === null || typeof avroType !== 'object') {
    return;
  }

  const typeRecord = avroType as Record<string, unknown>;
  registerNamedTypeReferences(typeRecord, references);
  collectTypeReferences(typeRecord.type, references);
  collectTypeReferences(typeRecord.items, references);
  collectTypeReferences(typeRecord.values, references);

  if (Array.isArray(typeRecord.fields)) {
    for (const field of typeRecord.fields) {
      collectTypeReferences((field as Record<string, unknown>).type, references);
    }
  }
}

export function collectCatalogAvroReferences(
  catalog: EventCatalog
): Readonly<Record<string, string>> {
  const references = new Map<string, string>();

  for (const event of catalog.events) {
    for (const [referenceName, targetName] of getNamedTypeEntries(
      event.schema.name,
      event.schema.namespace,
      event.payloadTypeName
    )) {
      references.set(referenceName, targetName);
    }

    for (const field of event.schema.fields) {
      collectTypeReferences(field.rawType, references);
    }
  }

  return Object.fromEntries(references);
}
