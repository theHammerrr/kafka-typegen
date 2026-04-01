import type { NormalizedKafkaTypegenConfig } from '../config/index.js';
import { createEventSchemaLoader } from '../schema/index.js';

import { buildCatalogEvents, buildCatalogTopics } from './builders.js';
import type { CatalogBuilder, EventCatalog } from './types.js';
import { validateIdentifierCollisions } from './validation.js';

export class DefaultCatalogBuilder implements CatalogBuilder {
  public async build(config: NormalizedKafkaTypegenConfig): Promise<EventCatalog> {
    const schemaDefinitions = await createEventSchemaLoader().loadEventSchemas(config.events);
    const events = buildCatalogEvents(config, schemaDefinitions);
    const topics = buildCatalogTopics(config, events);

    validateIdentifierCollisions(events, topics);

    return {
      config,
      events,
      topics
    };
  }
}

export function createCatalogBuilder(): CatalogBuilder {
  return new DefaultCatalogBuilder();
}
