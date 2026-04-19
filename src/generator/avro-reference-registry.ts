import type { EventCatalog } from '../catalog/index.js';

import { getNamedTypeEntries } from './avro-declaration-helpers.js';
import {
  assertNoExternalTypeNameCollisions,
  collectTypeReferences,
  registerReference,
  registerExternalTypeMappings
} from './avro-reference-collector.js';

export function collectCatalogAvroReferences(
  catalog: EventCatalog
): Readonly<Record<string, string>> {
  const references = new Map<string, string>();
  const generatedTypeNames = new Set<string>(
    catalog.events.map((event) => event.payloadTypeName)
  );

  registerExternalTypeMappings(
    catalog.config.generation.avroExternalTypes,
    references
  );

  for (const event of catalog.events) {
    generatedTypeNames.add(event.schema.name);

    for (const [referenceName, targetName] of getNamedTypeEntries(
      event.schema.name,
      event.schema.namespace,
      event.payloadTypeName
    )) {
      registerReference(references, referenceName, targetName);
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
