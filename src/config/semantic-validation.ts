import type { ConfigValidationIssue, KafkaTypegenConfig } from './types.js';
import { ConfigValidationError } from './types.js';
import { buildValidationIssue } from './issues.js';

export function validateSemanticConfig(config: KafkaTypegenConfig): void {
  const issues: ConfigValidationIssue[] = [];
  const topicNameIndexes = new Map<string, number>();
  const eventNameIndexes = new Map<string, { topicIndex: number; eventIndex: number }>();

  config.topics.forEach((topic, topicIndex) => {
    const existingTopicIndex = topicNameIndexes.get(topic.name);

    if (existingTopicIndex !== undefined) {
      issues.push(
        buildValidationIssue(
          ['topics', topicIndex, 'name'],
          `Duplicate topic name '${topic.name}'. First defined at topics[${existingTopicIndex}].name.`
        )
      );
    } else {
      topicNameIndexes.set(topic.name, topicIndex);
    }

    if (config.sync?.kafka !== undefined && topic.sync === undefined) {
      issues.push(
        buildValidationIssue(
          ['topics', topicIndex, 'sync'],
          'Topic sync config is required when sync.kafka is enabled.'
        )
      );
    }

    topic.events.forEach((event, eventIndex) => {
      const existingEvent = eventNameIndexes.get(event.name);

      if (existingEvent !== undefined) {
        issues.push(
          buildValidationIssue(
            ['topics', topicIndex, 'events', eventIndex, 'name'],
            `Duplicate event name '${event.name}'. First defined at topics[${existingEvent.topicIndex}].events[${existingEvent.eventIndex}].name.`
          )
        );
      } else {
        eventNameIndexes.set(event.name, { eventIndex, topicIndex });
      }
    });
  });

  if (
    config.sync?.schemaRegistry !== undefined &&
    config.schemaRegistry === undefined
  ) {
    issues.push(
      buildValidationIssue(
        ['schemaRegistry'],
        'Top-level schemaRegistry config is required when sync.schemaRegistry is enabled.'
      )
    );
  }

  if (
    config.sync?.schemaRegistry?.failOnDrift !== undefined &&
    config.sync.schemaRegistry.onDrift !== undefined
  ) {
    issues.push(
      buildValidationIssue(
        ['sync', 'schemaRegistry', 'onDrift'],
        'Use either sync.schemaRegistry.onDrift or sync.schemaRegistry.failOnDrift, not both.'
      )
    );
  }

  const configuredExternalTypes = config.generation?.avroExternalTypes;
  if (configuredExternalTypes !== undefined) {
    const seenTypeScriptTargets = new Map<string, string>();

    for (const [avroFullName, typeScriptType] of Object.entries(configuredExternalTypes)) {
      const existingAvroFullName = seenTypeScriptTargets.get(typeScriptType);

      if (existingAvroFullName !== undefined && existingAvroFullName !== avroFullName) {
        issues.push(
          buildValidationIssue(
            ['generation', 'avroExternalTypes'],
            `TypeScript type '${typeScriptType}' is mapped from both '${existingAvroFullName}' and '${avroFullName}'.`
          )
        );
      } else {
        seenTypeScriptTargets.set(typeScriptType, avroFullName);
      }
    }
  }

  if (issues.length > 0) {
    throw new ConfigValidationError(issues);
  }
}
