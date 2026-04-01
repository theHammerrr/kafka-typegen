import { relative as relativePath } from 'node:path';

import type { EventCatalog } from '../catalog/index.js';

import type { GeneratedFile, GeneratorOutput, TypeGenerator } from './types.js';

function indent(value: string, depth = 1): string {
  const prefix = '  '.repeat(depth);

  return value
    .split('\n')
    .map((line) => (line.length > 0 ? `${prefix}${line}` : line))
    .join('\n');
}

function formatPropertyName(name: string): string {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/u.test(name) ? name : `'${name}'`;
}

function formatLiteral(value: string): string {
  return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`;
}

function toGeneratedSchemaPath(catalog: EventCatalog, schemaFilePath: string): string {
  return relativePath(catalog.config.sources.rootDir, schemaFilePath);
}

function toCamelCase(value: string): string {
  const pascalCaseValue = value
    .split(/[^a-zA-Z0-9]+/u)
    .flatMap((segment) => segment.split(/(?=[A-Z])/u))
    .filter((segment) => segment.length > 0)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1).toLowerCase())
    .join('');

  return pascalCaseValue.length > 0
    ? `${pascalCaseValue.charAt(0).toLowerCase()}${pascalCaseValue.slice(1)}`
    : 'event';
}

function toTypeScriptType(avroType: unknown): string {
  if (typeof avroType === 'string') {
    switch (avroType) {
      case 'boolean':
        return 'boolean';
      case 'bytes':
        return 'Uint8Array';
      case 'double':
      case 'float':
      case 'int':
      case 'long':
        return 'number';
      case 'null':
        return 'null';
      case 'string':
        return 'string';
      default:
        return 'unknown';
    }
  }

  if (Array.isArray(avroType)) {
    return avroType.map((memberType) => toTypeScriptType(memberType)).join(' | ');
  }

  if (avroType !== null && typeof avroType === 'object') {
    const typeRecord = avroType as Record<string, unknown>;

    if (typeof typeRecord.logicalType === 'string') {
      return typeRecord.logicalType === 'uuid' ? 'string' : toTypeScriptType(typeRecord.type);
    }

    switch (typeRecord.type) {
      case 'array':
        return `${toTypeScriptType(typeRecord.items)}[]`;
      case 'boolean':
        return 'boolean';
      case 'bytes':
        return 'Uint8Array';
      case 'double':
      case 'float':
      case 'int':
      case 'long':
        return 'number';
      case 'enum':
        return Array.isArray(typeRecord.symbols)
          ? typeRecord.symbols.map((symbol) => formatLiteral(String(symbol))).join(' | ')
          : 'string';
      case 'fixed':
        return 'Uint8Array';
      case 'map':
        return `Record<string, ${toTypeScriptType(typeRecord.values)}>`;
      case 'null':
        return 'null';
      case 'record': {
        const fields = Array.isArray(typeRecord.fields)
          ? typeRecord.fields.map((field) => {
              const fieldRecord = field as Record<string, unknown>;
              const propertyName =
                typeof fieldRecord.name === 'string' ? formatPropertyName(fieldRecord.name) : "'unknown'";

              return `${propertyName}: ${toTypeScriptType(fieldRecord.type)};`;
            })
          : [];

        return `{\n${indent(fields.join('\n'))}\n}`;
      }
      case 'string':
        return 'string';
      default:
        return 'unknown';
    }
  }

  return 'unknown';
}

function emitPayloadInterfaces(catalog: EventCatalog): string {
  return catalog.events
    .map((event) => {
      const fields = event.schema.fields.map(
        (field) => `${formatPropertyName(field.name)}: ${toTypeScriptType(field.rawType)};`
      );

      return `export interface ${event.payloadTypeName} {\n${indent(fields.join('\n'))}\n}`;
    })
    .join('\n\n');
}

function emitEventUnion(catalog: EventCatalog): string {
  const eventNames = catalog.events.map((event) => formatLiteral(event.eventName)).join(' | ');

  return `export type EventName = ${eventNames};`;
}

function emitTopicUnion(catalog: EventCatalog): string {
  const topicNames = catalog.topics.map((topic) => formatLiteral(topic.topicName)).join(' | ');

  return `export type TopicName = ${topicNames};`;
}

function emitEventPayloadMap(catalog: EventCatalog): string {
  const entries = catalog.events.map(
    (event) => `${formatLiteral(event.eventName)}: ${event.payloadTypeName};`
  );

  return `export interface EventPayloadByName {\n${indent(entries.join('\n'))}\n}`;
}

function emitTopicEventsMap(catalog: EventCatalog): string {
  const entries = catalog.topics.map((topic) => {
    const eventUnion = topic.events.map((event) => formatLiteral(event.eventName)).join(' | ');

    return `${formatLiteral(topic.topicName)}: ${eventUnion};`;
  });

  return `export interface TopicEventByName {\n${indent(entries.join('\n'))}\n}`;
}

function emitEventMetadataMap(catalog: EventCatalog): string {
  const entries = catalog.events.map((event) => {
    const metadataLines = [
      `event: ${formatLiteral(event.eventName)};`,
      `topic: ${formatLiteral(event.topicName)};`,
      `subject: ${formatLiteral(event.subjectName)};`,
      `schemaFilePath: ${formatLiteral(toGeneratedSchemaPath(catalog, event.runtime.schemaFilePath))};`,
      `schemaName: ${formatLiteral(event.schemaName)};`,
      `payloadType: ${formatLiteral(event.payloadTypeName)};`
    ];

    return `${formatLiteral(event.eventName)}: {\n${indent(metadataLines.join('\n'))}\n};`;
  });

  return `export interface EventMetadataByName {\n${indent(entries.join('\n'))}\n}`;
}

function emitTopicMetadataMap(catalog: EventCatalog): string {
  const entries = catalog.topics.map((topic) => {
    const metadataLines = [
      `topic: ${formatLiteral(topic.topicName)};`,
      `topicType: ${formatLiteral(topic.topicTypeName)};`,
      `events: ${topic.events.map((event) => formatLiteral(event.eventName)).join(' | ')};`
    ];

    return `${formatLiteral(topic.topicName)}: {\n${indent(metadataLines.join('\n'))}\n};`;
  });

  return `export interface TopicMetadataByName {\n${indent(entries.join('\n'))}\n}`;
}

function emitProducerMetadataConstant(catalog: EventCatalog): string {
  const entries = catalog.events.map((event) => {
    const lines = [
      `eventName: ${formatLiteral(event.eventName)},`,
      `topicName: ${formatLiteral(event.topicName)},`,
      `subjectName: ${formatLiteral(event.subjectName)},`,
      `schemaFilePath: ${formatLiteral(toGeneratedSchemaPath(catalog, event.runtime.schemaFilePath))},`,
      `schemaName: ${formatLiteral(event.schemaName)},`,
      `payloadTypeName: ${formatLiteral(event.payloadTypeName)}`
    ];

    return `${formatLiteral(event.eventName)}: {\n${indent(lines.join('\n'))}\n}`;
  });

  return `export const producerEventMetadata: { readonly [K in EventName]: RuntimeEventMetadata } = {\n${indent(
    entries.join(',\n')
  )}\n};`;
}

function emitProducerTypes(catalog: EventCatalog): string {
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

function emitProducerFactory(catalog: EventCatalog): string {
  const groupedEntries = catalog.events.map((event) => {
    const helperName = toCamelCase(event.eventName);
    const helperBody = [
      `send(payload: ${event.payloadTypeName}) {`,
      indent(
        `return runtimeProducer.send(producerEventMetadata[${formatLiteral(event.eventName)}], payload);`,
        1
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
        indent('return runtimeProducer.send(producerEventMetadata[event], payload);', 1),
        '},',
        'events: {',
        indent(groupedEntries.join(',\n')),
        '}'
      ].join('\n')
    ),
    '};'
  ].join('\n');

  return `export function createProducer(runtimeProducer: RuntimeProducer): GeneratedProducer {\n${indent(
    body
  )}\n}`;
}

function emitConsumerTypes(catalog: EventCatalog): string {
  const eventMessages = catalog.events.map((event) => {
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
  });

  const topicMessages = catalog.topics.map((topic) => {
    const union = topic.events.map((event) => `${event.payloadTypeName}Message`).join(' | ');

    return `export type ${topic.topicTypeName}Message = ${union};`;
  });

  const consumerEvents = catalog.events.map((event) => {
    const helperName = toCamelCase(event.eventName);

    return `${helperName}: {\n${indent(
      `on(handler: (message: ${event.payloadTypeName}Message) => Promise<void> | void): Promise<void>;`
    )}\n};`;
  });

  return [
    ...eventMessages,
    ...topicMessages,
    '',
    'export interface GeneratedConsumerEvents {',
    indent(consumerEvents.join('\n')),
    '}',
    '',
    'export interface GeneratedConsumer {',
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
    '}',
    '',
    'export interface ConsumerMessageByEvent {',
    indent(
      catalog.events
        .map((event) => `${formatLiteral(event.eventName)}: ${event.payloadTypeName}Message;`)
        .join('\n')
    ),
    '}',
    '',
    'export interface ConsumerMessageByTopic {',
    indent(
      catalog.topics
        .map((topic) => `${formatLiteral(topic.topicName)}: ${topic.topicTypeName}Message;`)
        .join('\n')
    ),
    '}'
  ].join('\n');
}

function emitConsumerFactory(catalog: EventCatalog): string {
  const eventHelpers = catalog.events.map((event) => {
    const helperName = toCamelCase(event.eventName);
    const body = [
      'on(handler) {',
      indent(`return runtimeConsumer.on(producerEventMetadata[${formatLiteral(event.eventName)}], handler);`),
      '}'
    ].join('\n');

    return `${helperName}: {\n${indent(body)}\n}`;
  });

  const factoryBody = [
    'return {',
    indent(
      [
        'on(event, handler) {',
        indent('return runtimeConsumer.on(producerEventMetadata[event], handler);'),
        '},',
        'onTopic(topic, handler) {',
        indent('return runtimeConsumer.onTopic(topic, handler);'),
        '},',
        'events: {',
        indent(eventHelpers.join(',\n')),
        '}'
      ].join('\n')
    ),
    '};'
  ].join('\n');

  return `export function createConsumer(runtimeConsumer: RuntimeConsumer): GeneratedConsumer {\n${indent(
    factoryBody
  )}\n}`;
}

function emitGeneratedFile(catalog: EventCatalog): GeneratedFile {
  const sections = [
    '// Generated by kafka-typegen. Do not edit manually.',
    "import type { RuntimeConsumer, RuntimeEventMetadata, RuntimeProducer } from 'kafka-typegen';",
    emitPayloadInterfaces(catalog),
    emitEventUnion(catalog),
    emitTopicUnion(catalog),
    emitEventPayloadMap(catalog),
    emitTopicEventsMap(catalog),
    emitEventMetadataMap(catalog),
    emitTopicMetadataMap(catalog),
    emitProducerMetadataConstant(catalog),
    emitProducerTypes(catalog),
    emitProducerFactory(catalog),
    emitConsumerTypes(catalog),
    emitConsumerFactory(catalog)
  ];

  return {
    contents: `${sections.join('\n\n')}\n`,
    filePath: catalog.config.generation.typesFileName
  };
}

export class DefaultTypeGenerator implements TypeGenerator {
  public async generate(catalog: EventCatalog): Promise<GeneratorOutput> {
    return {
      files: [emitGeneratedFile(catalog)]
    };
  }
}

export function createTypeGenerator(): TypeGenerator {
  return new DefaultTypeGenerator();
}
