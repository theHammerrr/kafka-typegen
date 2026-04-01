import type { EventCatalog } from '../catalog/index.js';

import { stableStringify } from './stable-stringify.js';
import type { DesiredSchemaRegistrySubject } from './types.js';

export function buildSchemaRegistryPlan(catalog: EventCatalog): readonly DesiredSchemaRegistrySubject[] {
  return catalog.events.map((event) => ({
    eventName: event.eventName,
    schemaText: stableStringify(event.schema.rawSchema),
    subjectName: event.subjectName,
    topicName: event.topicName
  }));
}
