import type { CatalogEvent, CatalogTopic, EventCatalog } from '../catalog/index.js';

import { formatLiteral, indent } from './render-utils.js';

function emitEventMessage(event: CatalogEvent): string {
  const lines = [
    `event: ${formatLiteral(event.eventName)};`,
    `topic: ${formatLiteral(event.topicName)};`,
    `payload: ${event.payloadTypeName};`,
    'key?: unknown;',
    'headers?: Readonly<Record<string, string>>;',
    'partition?: number;',
    'offset?: string;',
    'timestamp?: string;',
    'schemaId?: string | number;'
  ];

  return `export interface ${event.payloadTypeName}Message {\n${indent(lines.join('\n'))}\n}`;
}

function emitTopicMessage(topic: CatalogTopic): string | undefined {
  if (topic.events.length < 2) {
    return undefined;
  }

  const union = topic.events.map((event) => `${event.payloadTypeName}Message`).join(' | ');
  return `export type ${topic.topicTypeName}Message = ${union};`;
}

export function emitConsumerMessageTypes(catalog: EventCatalog): string {
  const sections = [
    ...catalog.events.map(emitEventMessage),
    ...catalog.topics
      .map(emitTopicMessage)
      .filter((section): section is string => section !== undefined)
  ];

  return sections.join('\n\n');
}
