import type { EventCatalog } from '../catalog/index.js';

import { formatLiteral, indent, toCamelCase, toPascalCase } from './render-utils.js';

export function emitEventNameConstants(catalog: EventCatalog): string {
  const entries = catalog.events.map(
    (event) => `${toPascalCase(toCamelCase(event.eventName))}: ${formatLiteral(event.eventName)},`
  );

  return [
    'export const EventNames = {',
    indent(entries.join('\n')),
    '} as const;'
  ].join('\n');
}

export function emitTopicNameConstants(catalog: EventCatalog): string {
  const entries = catalog.topics.map(
    (topic) => `${toPascalCase(toCamelCase(topic.topicName))}: ${formatLiteral(topic.topicName)},`
  );

  return [
    'export const TopicNames = {',
    indent(entries.join('\n')),
    '} as const;'
  ].join('\n');
}
