import type { NormalizedKafkaTypegenConfig, NormalizedTopicConfig } from '../config/index.js';
import { createEventSchemaLoader } from '../schema/index.js';
import type { EventSchemaDefinition } from '../schema/index.js';

import { CatalogValidationError } from './errors.js';
import type { CatalogBuilder, CatalogEvent, CatalogTopic, EventCatalog } from './types.js';

function toPascalCase(value: string): string {
  const segments = value
    .split(/[^a-zA-Z0-9]+/u)
    .flatMap((segment) => segment.split(/(?=[A-Z])/u))
    .filter((segment) => segment.length > 0);

  return segments
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1).toLowerCase())
    .join('');
}

function buildPayloadTypeName(eventName: string, suffix: string): string {
  return `${toPascalCase(eventName)}${suffix}`;
}

function buildTopicTypeName(topicName: string, suffix: string): string {
  return `${toPascalCase(topicName)}${suffix}`;
}

function validateIdentifierCollisions(events: readonly CatalogEvent[], topics: readonly CatalogTopic[]): void {
  const issues: string[] = [];
  const payloadTypeNames = new Map<string, string>();
  const topicTypeNames = new Map<string, string>();

  for (const event of events) {
    const existingEvent = payloadTypeNames.get(event.payloadTypeName);

    if (existingEvent !== undefined) {
      issues.push(
        `Generated payload type '${event.payloadTypeName}' collides between events '${existingEvent}' and '${event.eventName}'.`
      );
    } else {
      payloadTypeNames.set(event.payloadTypeName, event.eventName);
    }
  }

  for (const topic of topics) {
    const existingTopic = topicTypeNames.get(topic.topicTypeName);

    if (existingTopic !== undefined) {
      issues.push(
        `Generated topic type '${topic.topicTypeName}' collides between topics '${existingTopic}' and '${topic.topicName}'.`
      );
    } else {
      topicTypeNames.set(topic.topicTypeName, topic.topicName);
    }
  }

  if (issues.length > 0) {
    throw new CatalogValidationError(issues);
  }
}

function buildCatalogEvents(
  config: NormalizedKafkaTypegenConfig,
  schemaDefinitions: readonly EventSchemaDefinition[]
): readonly CatalogEvent[] {
  const topicTypeNamesByTopic = new Map<string, string>(
    config.topics.map((topic) => [
      topic.topicName,
      buildTopicTypeName(topic.topicName, config.naming.topicTypeSuffix)
    ])
  );

  return schemaDefinitions.map((definition) => {
    const topicTypeName = topicTypeNamesByTopic.get(definition.topicName);

    if (topicTypeName === undefined) {
      throw new CatalogValidationError([
        `Schema definition for event '${definition.eventName}' references unknown topic '${definition.topicName}'.`
      ]);
    }

    return {
      eventName: definition.eventName,
      payloadTypeName: buildPayloadTypeName(definition.eventName, config.naming.eventTypeSuffix),
      runtime: {
        eventName: definition.eventName,
        schemaFilePath: definition.schema.filePath,
        subjectName: definition.subjectName,
        topicName: definition.topicName
      },
      schema: definition.schema,
      schemaName: definition.schema.name,
      subjectName: definition.subjectName,
      topicName: definition.topicName,
      topicTypeName
    };
  });
}

function buildCatalogTopics(
  config: NormalizedKafkaTypegenConfig,
  events: readonly CatalogEvent[]
): readonly CatalogTopic[] {
  const eventsByTopic = new Map<string, CatalogEvent[]>();

  for (const event of events) {
    const topicEvents = eventsByTopic.get(event.topicName);

    if (topicEvents === undefined) {
      eventsByTopic.set(event.topicName, [event]);
    } else {
      topicEvents.push(event);
    }
  }

  return config.topics.map((topic: NormalizedTopicConfig) => {
    const topicEvents = [...(eventsByTopic.get(topic.topicName) ?? [])].sort((leftEvent, rightEvent) =>
      leftEvent.eventName.localeCompare(rightEvent.eventName)
    );

    return {
      eventNames: topicEvents.map((event) => event.eventName),
      events: topicEvents,
      subjectStrategy: topic.subjectStrategy,
      topicName: topic.topicName,
      topicTypeName: buildTopicTypeName(topic.topicName, config.naming.topicTypeSuffix)
    };
  });
}

export class DefaultCatalogBuilder implements CatalogBuilder {
  public async build(config: NormalizedKafkaTypegenConfig): Promise<EventCatalog> {
    const schemaDefinitions = await createEventSchemaLoader().loadEventSchemas(config.events);
    const events = buildCatalogEvents(config, schemaDefinitions);
    const topics = buildCatalogTopics(config, events);

    validateIdentifierCollisions(events, topics);

    return {
      config,
      events,
      topics
    };
  }
}

export function createCatalogBuilder(): CatalogBuilder {
  return new DefaultCatalogBuilder();
}
