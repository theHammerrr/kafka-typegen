import type { EventCatalog } from '../catalog/index.js';

export { emitConsumerFactory } from './consumer-factory.js';
import { formatLiteral, indent, toCamelCase } from './render-utils.js';

export function emitConsumerTypes(catalog: EventCatalog): string {
  return [
    ...catalog.events.map(emitEventMessage),
    ...catalog.topics.map(emitTopicMessage),
    '',
    emitGeneratedConsumerInterface(catalog),
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

  return `${helperName}: {\n${indent(`on(handler: (message: ${event.payloadTypeName}Message) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;`)}\n};`;
}

function emitGeneratedConsumerInterface(catalog: EventCatalog): string {
  return [
    'export type GeneratedConsumerSubscribeOptions<TRuntimeConsumer extends RuntimeConsumer> = TRuntimeConsumer extends RuntimeConsumer<infer TSubscriptionOptions> ? TSubscriptionOptions : never;',
    '',
    'export interface GeneratedConsumerEvents<TRuntimeConsumer extends RuntimeConsumer = RuntimeConsumer> {',
    indent(catalog.events.map(emitConsumerEventHelper).join('\n')),
    '}',
    '',
    'export type GeneratedConsumer<TRuntimeConsumer extends RuntimeConsumer = RuntimeConsumer> = TRuntimeConsumer & {',
    indent(
      [
        'on<E extends EventName>(',
        indent('event: E,'),
        indent('handler: (message: ConsumerMessageByEvent[E]) => Promise<void> | void,'),
        indent('options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>'),
        '): Promise<void>;',
        'onTopic<T extends TopicName>(',
        indent('topic: T,'),
        indent('handler: (message: ConsumerMessageByTopic[T]) => Promise<void> | void,'),
        indent('options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>'),
        '): Promise<void>;',
        'events: GeneratedConsumerEvents<TRuntimeConsumer>;'
      ].join('\n')
    ),
    '};'
  ].join('\n');
}

function emitConsumerMessageByEvent(catalog: EventCatalog): string {
  const entries = catalog.events.map((event) => `${formatLiteral(event.eventName)}: ${event.payloadTypeName}Message;`);

  return `export interface ConsumerMessageByEvent {\n${indent(entries.join('\n'))}\n}`;
}

function emitConsumerMessageByTopic(catalog: EventCatalog): string {
  const entries = catalog.topics.map((topic) => `${formatLiteral(topic.topicName)}: ${topic.topicTypeName}Message;`);

  return `export interface ConsumerMessageByTopic {\n${indent(entries.join('\n'))}\n}`;
}
