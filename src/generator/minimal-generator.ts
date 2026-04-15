import type { CatalogEvent, CatalogTopic, EventCatalog } from '../catalog/index.js';

import { emitPayloadInterfaces } from './catalog-sections.js';
import { emitSchemaRegistryConfigConstant } from './metadata-sections.js';
import {
  formatLiteral,
  indent,
  toCamelCase,
  toGeneratedSchemaPath,
  toPascalCase
} from './render-utils.js';
import type { GeneratedFile } from './types.js';

function emitEventMessage(event: CatalogEvent): string {
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

function emitTopicMessage(topic: CatalogTopic): string | undefined {
  if (topic.events.length < 2) {
    return undefined;
  }

  const union = topic.events.map((event) => `${event.payloadTypeName}Message`).join(' | ');
  return `export type ${topic.topicTypeName}Message = ${union};`;
}

function emitConsumerMessageTypes(catalog: EventCatalog): string {
  const sections = [
    ...catalog.events.map(emitEventMessage),
    ...catalog.topics
      .map(emitTopicMessage)
      .filter((section): section is string => section !== undefined)
  ];

  return sections.join('\n\n');
}

function getProducerTopicInterfaceName(topic: CatalogTopic): string {
  return `Generated${toPascalCase(toCamelCase(topic.topicName))}ProducerTopic`;
}

function getConsumerTopicInterfaceName(topic: CatalogTopic): string {
  return `Generated${toPascalCase(toCamelCase(topic.topicName))}ConsumerTopic`;
}

function emitProducerTopicInterface(topic: CatalogTopic): string {
  const members = topic.events.map((event) => {
    const propertyName = toCamelCase(event.eventName);
    return `${propertyName}: {\n${indent(`send(payload: ${event.payloadTypeName}, options?: GeneratedProducerSendOptions<TRuntimeProducer>): Promise<void>;`)}\n};`;
  });

  return [
    `export interface ${getProducerTopicInterfaceName(topic)}<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> {`,
    indent(members.join('\n')),
    '}'
  ].join('\n');
}

function emitProducerTypes(catalog: EventCatalog): string {
  const topicInterfaces = catalog.topics.map(emitProducerTopicInterface);
  const topicMembers = catalog.topics.map((topic) => {
    const propertyName = topic.propertyName;
    return `${propertyName}: ${getProducerTopicInterfaceName(topic)}<TRuntimeProducer>;`;
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

function emitMinimalProducerMetadata(catalog: EventCatalog): string {
  return [
    'const producerMetadataByTopic: Readonly<Record<string, Readonly<Record<string, RuntimeEventMetadata>>>> = {',
    indent(catalog.topics.map((topic) => emitProducerMetadata(catalog, topic)).join(',\n')),
    '};'
  ].join('\n');
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

function emitMinimalConsumerMetadata(catalog: EventCatalog): string {
  return [
    'const consumerMetadataByTopic = {',
    indent(catalog.topics.map(emitConsumerTopicMetadata).join(',\n')),
    '} as const;'
  ].join('\n');
}

function emitProducerFactory(catalog: EventCatalog): string {
  const topicEntries = catalog.topics.map((topic) => {
    const topicPropertyName = topic.propertyName;
    const eventEntries = topic.events.map((event) => {
      const eventPropertyName = toCamelCase(event.eventName);
      const body = [
        `send(payload: ${event.payloadTypeName}, options?: GeneratedProducerSendOptions<TRuntimeProducer>) {`,
        indent(`return runtimeSend(producerMetadataByTopic.${topicPropertyName}.${eventPropertyName}, payload, options);`),
        '}'
      ].join('\n');

      return `${eventPropertyName}: {\n${indent(body)}\n}`;
    });

    return `${topicPropertyName}: {\n${indent(eventEntries.join(',\n'))}\n}`;
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

function emitConsumerTopicInterface(topic: CatalogTopic): string {
  const members = topic.events.map((event) => {
    const propertyName = toCamelCase(event.eventName);
    return `${propertyName}: {\n${indent(`on(handler: (message: ${event.payloadTypeName}Message) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;`)}\n};`;
  });

  if (topic.events.length > 1) {
    members.unshift(`on(handler: (message: ${topic.topicTypeName}Message) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;`);
  }

  return [
    `export interface ${getConsumerTopicInterfaceName(topic)}<TRuntimeConsumer extends RuntimeConsumer = RuntimeConsumer> {`,
    indent(members.join('\n')),
    '}'
  ].join('\n');
}

function emitConsumerTypes(catalog: EventCatalog): string {
  const topicInterfaces = catalog.topics.map(emitConsumerTopicInterface);
  const topicMembers = catalog.topics.map((topic) => {
    const propertyName = topic.propertyName;
    return `${propertyName}: ${getConsumerTopicInterfaceName(topic)}<TRuntimeConsumer>;`;
  });

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

function emitConsumerFactory(catalog: EventCatalog): string {
  const topicEntries = catalog.topics.map((topic) => {
    const topicPropertyName = topic.propertyName;
    const members: string[] = [];

    if (topic.events.length > 1) {
      members.push(
        [
          `on(handler: (message: ${topic.topicTypeName}Message) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>) {`,
          indent(`return runtimeOnTopic(consumerMetadataByTopic.${topicPropertyName}.topicName, consumerMetadataByTopic.${topicPropertyName}.metadataByEvent, handler as never, options);`),
          '}'
        ].join('\n')
      );
    }

    members.push(
      ...topic.events.map((event) => {
        const eventPropertyName = toCamelCase(event.eventName);
        const body = [
          `on(handler: (message: ${event.payloadTypeName}Message) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>) {`,
          indent(`return runtimeOn(producerMetadataByTopic.${topicPropertyName}.${eventPropertyName}, handler as never, options);`),
          '}'
        ].join('\n');

        return `${eventPropertyName}: {\n${indent(body)}\n}`;
      })
    );

    return `${topicPropertyName}: {\n${indent(members.join(',\n'))}\n}`;
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

function emitClientTypes(): string {
  return [
    'export type GeneratedClient<TRuntimeClient extends RuntimeClient = RuntimeClient> = Omit<TRuntimeClient, \'producer\' | \'consumer\'> & {',
    indent(
      [
        'producer: GeneratedProducer<TRuntimeClient[\'producer\']>;',
        'consumer: GeneratedConsumer<TRuntimeClient[\'consumer\']>;'
      ].join('\n')
    ),
    '};'
  ].join('\n');
}

function emitClientFactory(): string {
  return [
    'export function createClient<TRuntimeClient extends RuntimeClient>(runtime: TRuntimeClient): GeneratedClient<TRuntimeClient> {',
    indent(
      [
        'return Object.assign(Object.create(runtime), {',
        indent(
          [
            'producer: createProducer(runtime.producer),',
            'consumer: createConsumer(runtime.consumer)'
          ].join('\n')
        ),
        '}) as GeneratedClient<TRuntimeClient>;'
      ].join('\n')
    ),
    '}'
  ].join('\n');
}

export function emitMinimalGeneratedFile(catalog: EventCatalog): GeneratedFile {
  const sections = [
    '// Generated by kafka-typegen. Do not edit manually.',
    `import type { RuntimeClient, RuntimeConsumer, RuntimeEventMetadata, RuntimeProducer } from ${formatLiteral(catalog.config.runtime.module)};`,
    emitPayloadInterfaces(catalog),
    ...(catalog.config.schemaRegistry !== undefined
      ? [emitSchemaRegistryConfigConstant(catalog) ?? '']
      : []),
    emitConsumerMessageTypes(catalog),
    emitMinimalProducerMetadata(catalog),
    emitMinimalConsumerMetadata(catalog),
    emitProducerTypes(catalog),
    emitProducerFactory(catalog),
    emitConsumerTypes(catalog),
    emitConsumerFactory(catalog),
    emitClientTypes(),
    emitClientFactory()
  ];

  return {
    contents: `${sections.join('\n\n')}\n`,
    filePath: catalog.config.generation.typesFileName
  };
}
