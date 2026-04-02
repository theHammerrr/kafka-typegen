import type { EventCatalog } from '../catalog/index.js';

import { formatLiteral, indent, toCamelCase } from './render-utils.js';

export function emitProducerTypes(catalog: EventCatalog): string {
  const groupedEntries = catalog.events.map((event) => {
    const helperName = toCamelCase(event.eventName);

    return `${helperName}: {\n${indent(`send(payload: ${event.payloadTypeName}, options?: GeneratedProducerSendOptions<TRuntimeProducer>): Promise<void>;`)}\n};`;
  });

  return [
    'export type GeneratedProducerSendOptions<TRuntimeProducer extends RuntimeProducer> = TRuntimeProducer extends RuntimeProducer<infer TSendOptions> ? TSendOptions : never;',
    '',
    'export interface GeneratedProducerEvents<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> {',
    indent(groupedEntries.join('\n')),
    '}',
    '',
    'export type GeneratedProducer<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> = TRuntimeProducer & {',
    indent(
      [
        'send<E extends EventName>(',
        indent('event: E,'),
        indent('payload: EventPayloadByName[E],'),
        indent('options?: GeneratedProducerSendOptions<TRuntimeProducer>'),
        '): Promise<void>;',
        'events: GeneratedProducerEvents<TRuntimeProducer>;'
      ].join('\n')
    ),
    '};'
  ].join('\n');
}

export function emitProducerFactory(catalog: EventCatalog): string {
  const groupedEntries = catalog.events.map((event) => {
    const helperName = toCamelCase(event.eventName);
    const helperBody = [
      `send(payload: ${event.payloadTypeName}, options?: GeneratedProducerSendOptions<TRuntimeProducer>) {`,
      indent(
        `return runtimeProducer.send(producerEventMetadata[${formatLiteral(event.eventName)}], payload, options);`
      ),
      '}'
    ].join('\n');

    return `${helperName}: {\n${indent(helperBody)}\n}`;
  });

  const body = [
    'const producer = Object.create(runtimeProducer) as GeneratedProducer<TRuntimeProducer>;',
    '',
    'producer.send = ((eventOrMetadata: unknown, payload: unknown, options?: unknown) => {',
    indent(
      [
        'if (typeof eventOrMetadata === \'string\' && Object.hasOwn(producerEventMetadata, eventOrMetadata)) {',
        indent(
          'return runtimeProducer.send(producerEventMetadata[eventOrMetadata as EventName], payload, options as never);'
        ),
        '}',
        '',
        'return runtimeProducer.send(eventOrMetadata as never, payload as never, options as never);'
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
