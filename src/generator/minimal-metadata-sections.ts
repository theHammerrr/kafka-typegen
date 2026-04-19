import type { CatalogTopic, EventCatalog } from '../catalog/index.js';

import { formatLiteral, toCamelCase, toGeneratedSchemaPath, indent } from './render-utils.js';

function emitProducerMetadata(catalog: EventCatalog, topic: CatalogTopic): string {
  const eventEntries = topic.events.map((event) => {
    const propertyName = toCamelCase(event.eventName);
    const lines = [
      `eventName: ${formatLiteral(event.eventName)},`,
      `topicName: ${formatLiteral(event.topicName)},`,
      `subjectName: ${formatLiteral(event.subjectName)},`,
      `schemaFilePath: ${formatLiteral(toGeneratedSchemaPath(catalog, event.runtime.schemaFilePath))},`,
      `schemaName: ${formatLiteral(event.schemaName)},`,
      `payloadTypeName: ${formatLiteral(event.payloadTypeName)}`
    ];

    return `${propertyName}: {\n${indent(lines.join('\n'))}\n}`;
  });

  return `${topic.propertyName}: {\n${indent(eventEntries.join(',\n'))}\n}`;
}

function emitConsumerTopicMetadata(topic: CatalogTopic): string {
  const eventEntries = topic.events.map((event) => {
    const propertyName = toCamelCase(event.eventName);
    return `${formatLiteral(event.eventName)}: producerMetadataByTopic.${topic.propertyName}.${propertyName}`;
  });

  return `${topic.propertyName}: {\n${indent(
    [
      `topicName: ${formatLiteral(topic.topicName)},`,
      'metadataByEvent: {',
      indent(eventEntries.join(',\n')),
      '}'
    ].join('\n')
  )}\n}`;
}

export function emitMinimalProducerMetadata(catalog: EventCatalog): string {
  return [
    'const producerMetadataByTopic: Readonly<Record<string, Readonly<Record<string, RuntimeEventMetadata>>>> = {',
    indent(catalog.topics.map((topic) => emitProducerMetadata(catalog, topic)).join(',\n')),
    '};'
  ].join('\n');
}

export function emitMinimalConsumerMetadata(catalog: EventCatalog): string {
  return [
    'const consumerMetadataByTopic = {',
    indent(catalog.topics.map(emitConsumerTopicMetadata).join(',\n')),
    '} as const;'
  ].join('\n');
}
