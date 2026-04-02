import type { EventCatalog } from '../catalog/index.js';

import { formatLiteral, indent, toGeneratedSchemaPath } from './render-utils.js';

export function emitEventMetadataMap(catalog: EventCatalog): string {
  const entries = catalog.events.map((event) => {
    const metadataLines = [
      `event: ${formatLiteral(event.eventName)};`,
      `topic: ${formatLiteral(event.topicName)};`,
      `subject: ${formatLiteral(event.subjectName)};`,
      `schemaFilePath: ${formatLiteral(toGeneratedSchemaPath(catalog, event.runtime.schemaFilePath))};`,
      `schemaName: ${formatLiteral(event.schemaName)};`,
      `payloadType: ${formatLiteral(event.payloadTypeName)};`
    ];

    return `${formatLiteral(event.eventName)}: {\n${indent(metadataLines.join('\n'))}\n};`;
  });

  return `export interface EventMetadataByName {\n${indent(entries.join('\n'))}\n}`;
}

export function emitTopicMetadataMap(catalog: EventCatalog): string {
  const entries = catalog.topics.map((topic) => {
    const metadataLines = [
      `topic: ${formatLiteral(topic.topicName)};`,
      `topicType: ${formatLiteral(topic.topicTypeName)};`,
      `events: ${topic.events.map((event) => formatLiteral(event.eventName)).join(' | ')};`
    ];

    return `${formatLiteral(topic.topicName)}: {\n${indent(metadataLines.join('\n'))}\n};`;
  });

  return `export interface TopicMetadataByName {\n${indent(entries.join('\n'))}\n}`;
}

export function emitProducerMetadataConstant(catalog: EventCatalog): string {
  const entries = catalog.events.map((event) => {
    const lines = [
      `eventName: ${formatLiteral(event.eventName)},`,
      `topicName: ${formatLiteral(event.topicName)},`,
      `subjectName: ${formatLiteral(event.subjectName)},`,
      `schemaFilePath: ${formatLiteral(toGeneratedSchemaPath(catalog, event.runtime.schemaFilePath))},`,
      `schemaName: ${formatLiteral(event.schemaName)},`,
      `payloadTypeName: ${formatLiteral(event.payloadTypeName)}`
    ];

    return `${formatLiteral(event.eventName)}: {\n${indent(lines.join('\n'))}\n}`;
  });

  return `export const producerEventMetadata: { readonly [K in EventName]: RuntimeEventMetadata } = {\n${indent(entries.join(',\n'))}\n};`;
}

export function emitTopicMetadataConstant(catalog: EventCatalog): string {
  const entries = catalog.topics.map((topic) => {
    const eventEntries = topic.events.map(
      (event) => `${formatLiteral(event.eventName)}: producerEventMetadata[${formatLiteral(event.eventName)}]`
    );

    return `${formatLiteral(topic.topicName)}: {\n${indent(eventEntries.join(',\n'))}\n}`;
  });

  return `export const topicEventMetadata: { readonly [K in TopicName]: Readonly<Record<string, RuntimeEventMetadata>> } = {\n${indent(entries.join(',\n'))}\n};`;
}

export function emitSchemaRegistryConfigConstant(
  catalog: EventCatalog
): string | undefined {
  if (catalog.config.schemaRegistry === undefined) {
    return undefined;
  }

  return `export const SchemaRegistryConfig = {\n${indent(
    `url: ${formatLiteral(catalog.config.schemaRegistry.url)}`
  )}\n} as const;`;
}
