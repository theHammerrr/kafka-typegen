import type { EventCatalog } from '../catalog/index.js';

import { formatLiteral, indent, toCamelCase } from './render-utils.js';

export function emitConsumerTypes(catalog: EventCatalog): string {
  return [
    ...catalog.events.map(emitEventMessage),
    ...catalog.topics.map(emitTopicMessage),
    '',
    'export interface GeneratedConsumerEvents {',
    indent(catalog.events.map(emitConsumerEventHelper).join('\n')),
    '}',
    '',
    emitGeneratedConsumerInterface(),
    '',
    emitConsumerMessageByEvent(catalog),
    '',
    emitConsumerMessageByTopic(catalog)
  ].join('\n');
}

function emitEventMessage(event: EventCatalog['events'][number]): string {
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

function emitTopicMessage(topic: EventCatalog['topics'][number]): string {
  const union = topic.events.map((event) => `${event.payloadTypeName}Message`).join(' | ');

  return `export type ${topic.topicTypeName}Message = ${union};`;
}

function emitConsumerEventHelper(event: EventCatalog['events'][number]): string {
  const helperName = toCamelCase(event.eventName);

  return `${helperName}: {\n${indent(`on(handler: (message: ${event.payloadTypeName}Message) => Promise<void> | void): Promise<void>;`)}\n};`;
}

function emitGeneratedConsumerInterface(): string {
  return [
    'export type GeneratedConsumer<TRuntimeConsumer extends RuntimeConsumer = RuntimeConsumer> = TRuntimeConsumer & {',
    indent(
      [
        'on<E extends EventName>(',
        indent('event: E,'),
        indent('handler: (message: ConsumerMessageByEvent[E]) => Promise<void> | void'),
        '): Promise<void>;',
        'onTopic<T extends TopicName>(',
        indent('topic: T,'),
        indent('handler: (message: ConsumerMessageByTopic[T]) => Promise<void> | void'),
        '): Promise<void>;',
        'events: GeneratedConsumerEvents;'
      ].join('\n')
    ),
    '};'
  ].join('\n');
}

function emitConsumerHelper(event: EventCatalog['events'][number]): string {
  const helperName = toCamelCase(event.eventName);
  const body = [
    'on(handler) {',
    indent(`return runtimeConsumer.on(producerEventMetadata[${formatLiteral(event.eventName)}], handler);`),
    '}'
  ].join('\n');

  return `${helperName}: {\n${indent(body)}\n}`;
}

function emitConsumerMessageByEvent(catalog: EventCatalog): string {
  const entries = catalog.events.map((event) => `${formatLiteral(event.eventName)}: ${event.payloadTypeName}Message;`);

  return `export interface ConsumerMessageByEvent {\n${indent(entries.join('\n'))}\n}`;
}

function emitConsumerMessageByTopic(catalog: EventCatalog): string {
  const entries = catalog.topics.map((topic) => `${formatLiteral(topic.topicName)}: ${topic.topicTypeName}Message;`);

  return `export interface ConsumerMessageByTopic {\n${indent(entries.join('\n'))}\n}`;
}

export function emitConsumerFactory(catalog: EventCatalog): string {
  const eventHelpers = catalog.events.map(emitConsumerHelper);

  const factoryBody = [
    'const consumer = Object.create(runtimeConsumer) as GeneratedConsumer<TRuntimeConsumer>;',
    '',
    'consumer.on = ((eventOrMetadata: unknown, handler: unknown) => {',
    indent(
      [
        'if (typeof eventOrMetadata === \'string\' && Object.hasOwn(producerEventMetadata, eventOrMetadata)) {',
        indent(
          'return runtimeConsumer.on(producerEventMetadata[eventOrMetadata as EventName], handler as never);'
        ),
        '}',
        '',
        'return runtimeConsumer.on(eventOrMetadata as never, handler as never);'
      ].join('\n')
    ),
    '}) as GeneratedConsumer<TRuntimeConsumer>[\'on\'];',
    '',
    'consumer.onTopic = ((topicOrName: unknown, handlerOrMetadata: unknown, maybeHandler?: unknown) => {',
    indent(
      [
        'if (typeof topicOrName === \'string\' && Object.hasOwn(topicEventMetadata, topicOrName)) {',
        indent(
          'return runtimeConsumer.onTopic(topicOrName as TopicName, topicEventMetadata[topicOrName as TopicName], handlerOrMetadata as never);'
        ),
        '}',
        '',
        'return runtimeConsumer.onTopic(topicOrName as never, handlerOrMetadata as never, maybeHandler as never);'
      ].join('\n')
    ),
    '}) as GeneratedConsumer<TRuntimeConsumer>[\'onTopic\'];',
    '',
    'consumer.events = {',
    indent(eventHelpers.join(',\n')),
    '};',
    '',
    'return consumer;'
  ].join('\n');

  return `export function createConsumer<TRuntimeConsumer extends RuntimeConsumer>(runtimeConsumer: TRuntimeConsumer): GeneratedConsumer<TRuntimeConsumer> {\n${indent(factoryBody)}\n}`;
}
