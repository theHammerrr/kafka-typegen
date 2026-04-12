import type { EventCatalog } from '../catalog/index.js';

import { collectAvroDeclarations } from './avro-declarations.js';
import { AVRO_LOGICAL_TYPE_DECLARATIONS } from './avro-logical-types.js';
import { collectCatalogAvroReferences } from './avro-reference-registry.js';
import { toTypeScriptType } from './avro-type-renderer.js';
import { formatLiteral, formatPropertyName, indent } from './render-utils.js';

export function emitPayloadInterfaces(catalog: EventCatalog): string {
  const sharedReferences = collectCatalogAvroReferences(catalog);
  const declarationsByBody = new Map<string, string>();
  const payloadInterfaces = catalog.events.map((event) => {
    const { declarations, references } = collectAvroDeclarations(
      event.schema.rawSchema,
      event.payloadTypeName,
      sharedReferences
    );

    for (const declaration of declarations) {
      declarationsByBody.set(declaration, declaration);
    }

    const fields = event.schema.fields.map(
      (field) => `${formatPropertyName(field.name)}: ${toTypeScriptType(field.rawType, {
        path: field.path,
        references
      })};`
    );

    return `export interface ${event.payloadTypeName} {\n${indent(fields.join('\n'))}\n}`;
  });

  return [
    ...AVRO_LOGICAL_TYPE_DECLARATIONS,
    ...declarationsByBody.values(),
    ...payloadInterfaces
  ].join('\n\n');
}

export function emitEventUnion(catalog: EventCatalog): string {
  const eventNames = catalog.events.map((event) => formatLiteral(event.eventName)).join(' | ');

  return `export type EventName = ${eventNames};`;
}

export function emitTopicUnion(catalog: EventCatalog): string {
  const topicNames = catalog.topics.map((topic) => formatLiteral(topic.topicName)).join(' | ');

  return `export type TopicName = ${topicNames};`;
}

export function emitEventPayloadMap(catalog: EventCatalog): string {
  const entries = catalog.events.map((event) => `${formatLiteral(event.eventName)}: ${event.payloadTypeName};`);

  return `export interface EventPayloadByName {\n${indent(entries.join('\n'))}\n}`;
}

export function emitTopicEventsMap(catalog: EventCatalog): string {
  const entries = catalog.topics.map((topic) => {
    const eventUnion = topic.events.map((event) => formatLiteral(event.eventName)).join(' | ');

    return `${formatLiteral(topic.topicName)}: ${eventUnion};`;
  });

  return `export interface TopicEventByName {\n${indent(entries.join('\n'))}\n}`;
}
