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
    'export type GeneratedProducer<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> = TRuntimeProducer & {',
    indent(
      [
        'send<E extends EventName>(event: E, payload: EventPayloadByName[E]): Promise<void>;',
        'events: GeneratedProducerEvents;'
      ].join('\n')
    ),
    '};'
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
    'const producer = Object.create(runtimeProducer) as GeneratedProducer<TRuntimeProducer>;',
    '',
    'producer.send = ((eventOrMetadata: unknown, payload: unknown) => {',
    indent(
      [
        'if (typeof eventOrMetadata === \'string\' && Object.hasOwn(producerEventMetadata, eventOrMetadata)) {',
        indent(
          'return runtimeProducer.send(producerEventMetadata[eventOrMetadata as EventName], payload);'
        ),
        '}',
        '',
        'return runtimeProducer.send(eventOrMetadata as never, payload as never);'
      ].join('\n')
    ),
    '}) as GeneratedProducer<TRuntimeProducer>[\'send\'];',
    '',
    'producer.events = {',
    indent(
      groupedEntries.join(',\n')
    ),
    '};',
    '',
    'return producer;'
  ].join('\n');

  return `export function createProducer<TRuntimeProducer extends RuntimeProducer>(runtimeProducer: TRuntimeProducer): GeneratedProducer<TRuntimeProducer> {\n${indent(body)}\n}`;
}
