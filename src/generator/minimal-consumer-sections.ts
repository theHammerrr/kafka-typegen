import type { CatalogTopic, EventCatalog } from '../catalog/index.js';

import { toCamelCase, toPascalCase, indent } from './render-utils.js';

function getConsumerTopicInterfaceName(topic: CatalogTopic): string {
  return `Generated${toPascalCase(toCamelCase(topic.topicName))}ConsumerTopic`;
}

function emitConsumerTopicInterface(topic: CatalogTopic): string {
  const members = topic.events.map((event) => {
    const propertyName = toCamelCase(event.eventName);
    const signature =
      'on(handler: '
      + `(message: ${event.payloadTypeName}Message) => Promise<void> | void, `
      + 'options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;';

    return `${propertyName}: {\n${indent(signature)}\n};`;
  });

  if (topic.events.length > 1) {
    members.unshift(
      'on(handler: '
      + `(message: ${topic.topicTypeName}Message) => Promise<void> | void, `
      + 'options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;'
    );
  }

  return [
    `export interface ${getConsumerTopicInterfaceName(topic)}<TRuntimeConsumer extends RuntimeConsumer = RuntimeConsumer> {`,
    indent(members.join('\n')),
    '}'
  ].join('\n');
}

export function emitConsumerTypes(catalog: EventCatalog): string {
  const topicInterfaces = catalog.topics.map(emitConsumerTopicInterface);
  const topicMembers = catalog.topics.map(
    (topic) => `${topic.propertyName}: ${getConsumerTopicInterfaceName(topic)}<TRuntimeConsumer>;`
  );

  return [
    'export type GeneratedConsumerSubscribeOptions<TRuntimeConsumer extends RuntimeConsumer> = TRuntimeConsumer extends RuntimeConsumer<infer TSubscriptionOptions> ? TSubscriptionOptions : never;',
    '',
    ...topicInterfaces,
    '',
    'export interface GeneratedConsumerTopics<TRuntimeConsumer extends RuntimeConsumer = RuntimeConsumer> {',
    indent(topicMembers.join('\n')),
    '}',
    '',
    'export type GeneratedConsumer<TRuntimeConsumer extends RuntimeConsumer = RuntimeConsumer> = TRuntimeConsumer & GeneratedConsumerTopics<TRuntimeConsumer>;'
  ].join('\n');
}

export function emitConsumerFactory(catalog: EventCatalog): string {
  const topicEntries = catalog.topics.map((topic) => {
    const members: string[] = [];

    if (topic.events.length > 1) {
      members.push(
        [
          `on(handler: (message: ${topic.topicTypeName}Message) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>) {`,
          indent(
            `return runtimeOnTopic(consumerMetadataByTopic.${topic.propertyName}.topicName, consumerMetadataByTopic.${topic.propertyName}.metadataByEvent, handler as never, options);`
          ),
          '}'
        ].join('\n')
      );
    }

    members.push(
      ...topic.events.map((event) => {
        const eventPropertyName = toCamelCase(event.eventName);
        const body = [
          `on(handler: (message: ${event.payloadTypeName}Message) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>) {`,
          indent(
            `return runtimeOn(producerMetadataByTopic.${topic.propertyName}.${eventPropertyName}, handler as never, options);`
          ),
          '}'
        ].join('\n');

        return `${eventPropertyName}: {\n${indent(body)}\n}`;
      })
    );

    return `${topic.propertyName}: {\n${indent(members.join(',\n'))}\n}`;
  });

  return [
    'export function createConsumer<TRuntimeConsumer extends RuntimeConsumer>(runtimeConsumer: TRuntimeConsumer): GeneratedConsumer<TRuntimeConsumer> {',
    indent(
      [
        'const runtimeOn = runtimeConsumer.on.bind(runtimeConsumer);',
        'const runtimeOnTopic = runtimeConsumer.onTopic.bind(runtimeConsumer);',
        '',
        'return Object.assign(Object.create(runtimeConsumer), {',
        indent(topicEntries.join(',\n')),
        '}) as GeneratedConsumer<TRuntimeConsumer>;'
      ].join('\n')
    ),
    '}'
  ].join('\n');
}
