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

function emitGeneratedFile(catalog: EventCatalog): GeneratedFile {
  const sections = [
    '// Generated by kafka-typegen. Do not edit manually.',
    emitPayloadInterfaces(catalog),
    emitEventUnion(catalog),
    emitTopicUnion(catalog),
    emitEventPayloadMap(catalog),
    emitTopicEventsMap(catalog),
    emitEventMetadataMap(catalog),
    emitTopicMetadataMap(catalog)
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
