import type { NormalizedKafkaTypegenConfig, NormalizedTopicConfig } from '../config/index.js';
import type { EventSchemaDefinition } from '../schema/index.js';

import { CatalogValidationError } from './errors.js';
import { buildPayloadTypeName, buildTopicPropertyName, buildTopicTypeName } from './naming.js';
import type { CatalogEvent, CatalogTopic } from './types.js';

export function buildCatalogEvents(
  config: NormalizedKafkaTypegenConfig,
  schemaDefinitions: readonly EventSchemaDefinition[]
): readonly CatalogEvent[] {
  const topicTypeNamesByTopic = new Map<string, string>(
    config.topics.map((topic) => [topic.topicName, buildTopicTypeName(topic.topicName, config.naming.topicTypeSuffix)])
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

export function buildCatalogTopics(
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
      propertyName: buildTopicPropertyName(topic.topicName),
      subjectStrategy: topic.subjectStrategy,
      topicName: topic.topicName,
      topicTypeName: buildTopicTypeName(topic.topicName, config.naming.topicTypeSuffix)
    };
  });
}
