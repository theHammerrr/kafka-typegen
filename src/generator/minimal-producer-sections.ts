import type { CatalogTopic, EventCatalog } from '../catalog/index.js';

import { toCamelCase, toPascalCase, indent } from './render-utils.js';

function getProducerTopicInterfaceName(topic: CatalogTopic): string {
  return `Generated${toPascalCase(toCamelCase(topic.topicName))}ProducerTopic`;
}

function emitProducerTopicInterface(topic: CatalogTopic): string {
  const members = topic.events.map((event) => {
    const propertyName = toCamelCase(event.eventName);
    const signature =
      'send(payload: '
      + `${event.payloadTypeName}, `
      + 'options?: GeneratedProducerSendOptions<TRuntimeProducer>): Promise<void>;';

    return `${propertyName}: {\n${indent(signature)}\n};`;
  });

  return [
    `export interface ${getProducerTopicInterfaceName(topic)}<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> {`,
    indent(members.join('\n')),
    '}'
  ].join('\n');
}

export function emitProducerTypes(catalog: EventCatalog): string {
  const topicInterfaces = catalog.topics.map(emitProducerTopicInterface);
  const topicMembers = catalog.topics.map((topic) => {
    return `${topic.propertyName}: ${getProducerTopicInterfaceName(topic)}<TRuntimeProducer>;`;
  });

  return [
    'export type GeneratedProducerSendOptions<TRuntimeProducer extends RuntimeProducer> = TRuntimeProducer extends RuntimeProducer<infer TSendOptions> ? TSendOptions : never;',
    '',
    ...topicInterfaces,
    '',
    'export interface GeneratedProducerTopics<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> {',
    indent(topicMembers.join('\n')),
    '}',
    '',
    'export type GeneratedProducer<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> = TRuntimeProducer & GeneratedProducerTopics<TRuntimeProducer>;'
  ].join('\n');
}

export function emitProducerFactory(catalog: EventCatalog): string {
  const topicEntries = catalog.topics.map((topic) => {
    const eventEntries = topic.events.map((event) => {
      const eventPropertyName = toCamelCase(event.eventName);
      const body = [
        `send(payload: ${event.payloadTypeName}, options?: GeneratedProducerSendOptions<TRuntimeProducer>) {`,
        indent(
          `return runtimeSend(producerMetadataByTopic.${topic.propertyName}.${eventPropertyName}, payload, options);`
        ),
        '}'
      ].join('\n');

      return `${eventPropertyName}: {\n${indent(body)}\n}`;
    });

    return `${topic.propertyName}: {\n${indent(eventEntries.join(',\n'))}\n}`;
  });

  return [
    'export function createProducer<TRuntimeProducer extends RuntimeProducer>(runtimeProducer: TRuntimeProducer): GeneratedProducer<TRuntimeProducer> {',
    indent(
      [
        'const runtimeSend = runtimeProducer.send.bind(runtimeProducer);',
        '',
        'return Object.assign(Object.create(runtimeProducer), {',
        indent(topicEntries.join(',\n')),
        '}) as GeneratedProducer<TRuntimeProducer>;'
      ].join('\n')
    ),
    '}'
  ].join('\n');
}
