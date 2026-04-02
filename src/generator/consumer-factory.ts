import type { EventCatalog } from '../catalog/index.js';

import { formatLiteral, indent, toCamelCase } from './render-utils.js';

function emitConsumerHelper(event: EventCatalog['events'][number]): string {
  const helperName = toCamelCase(event.eventName);
  const body = [
    'on(handler, options) {',
    indent(
      `return runtimeConsumer.on(producerEventMetadata[${formatLiteral(event.eventName)}], handler, options);`
    ),
    '}'
  ].join('\n');

  return `${helperName}: {\n${indent(body)}\n}`;
}

export function emitConsumerFactory(catalog: EventCatalog): string {
  const eventHelpers = catalog.events.map(emitConsumerHelper);
  const factoryBody = [
    'const consumer = Object.create(runtimeConsumer) as GeneratedConsumer<TRuntimeConsumer>;',
    '',
    'consumer.on = ((eventOrMetadata: unknown, handler: unknown, options?: unknown) => {',
    indent(
      [
        'if (typeof eventOrMetadata === \'string\' && Object.hasOwn(producerEventMetadata, eventOrMetadata)) {',
        indent('return runtimeConsumer.on(producerEventMetadata[eventOrMetadata as EventName], handler as never, options as never);'),
        '}',
        '',
        'return runtimeConsumer.on(eventOrMetadata as never, handler as never, options as never);'
      ].join('\n')
    ),
    '}) as GeneratedConsumer<TRuntimeConsumer>[\'on\'];',
    '',
    'consumer.onTopic = ((topicOrName: unknown, handlerOrMetadata: unknown, maybeHandler?: unknown, maybeOptions?: unknown) => {',
    indent(
      [
        'if (typeof topicOrName === \'string\' && Object.hasOwn(topicEventMetadata, topicOrName)) {',
        indent('return runtimeConsumer.onTopic(topicOrName as TopicName, topicEventMetadata[topicOrName as TopicName], handlerOrMetadata as never, maybeHandler as never);'),
        '}',
        '',
        'return runtimeConsumer.onTopic(topicOrName as never, handlerOrMetadata as never, maybeHandler as never, maybeOptions as never);'
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
