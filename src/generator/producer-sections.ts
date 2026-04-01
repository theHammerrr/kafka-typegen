import type { EventCatalog } from '../catalog/index.js';

import { formatLiteral, indent, toCamelCase } from './render-utils.js';

export function emitProducerTypes(catalog: EventCatalog): string {
  const groupedEntries = catalog.events.map((event) => {
    const helperName = toCamelCase(event.eventName);

    return `${helperName}: {\n${indent(`send(payload: ${event.payloadTypeName}): Promise<void>;`)}\n};`;
  });

  return [
    'export interface GeneratedProducerEvents {',
    indent(groupedEntries.join('\n')),
    '}',
    '',
    'export interface GeneratedProducer {',
    indent(
      [
        'send<E extends EventName>(event: E, payload: EventPayloadByName[E]): Promise<void>;',
        'events: GeneratedProducerEvents;'
      ].join('\n')
    ),
    '}'
  ].join('\n');
}

export function emitProducerFactory(catalog: EventCatalog): string {
  const groupedEntries = catalog.events.map((event) => {
    const helperName = toCamelCase(event.eventName);
    const helperBody = [
      `send(payload: ${event.payloadTypeName}) {`,
      indent(
        `return runtimeProducer.send(producerEventMetadata[${formatLiteral(event.eventName)}], payload);`
      ),
      '}'
    ].join('\n');

    return `${helperName}: {\n${indent(helperBody)}\n}`;
  });

  const body = [
    'return {',
    indent(
      [
        'send(event, payload) {',
        indent('return runtimeProducer.send(producerEventMetadata[event], payload);'),
        '},',
        'events: {',
        indent(groupedEntries.join(',\n')),
        '}'
      ].join('\n')
    ),
    '};'
  ].join('\n');

  return `export function createProducer(runtimeProducer: RuntimeProducer): GeneratedProducer {\n${indent(body)}\n}`;
}
