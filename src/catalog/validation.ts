import { CatalogValidationError } from './errors.js';
import type { CatalogEvent, CatalogTopic } from './types.js';

export function validateIdentifierCollisions(events: readonly CatalogEvent[], topics: readonly CatalogTopic[]): void {
  const issues: string[] = [];
  const payloadTypeNames = new Map<string, string>();
  const topicTypeNames = new Map<string, string>();

  for (const event of events) {
    const existingEvent = payloadTypeNames.get(event.payloadTypeName);
    if (existingEvent !== undefined) {
      issues.push(`Generated payload type '${event.payloadTypeName}' collides between events '${existingEvent}' and '${event.eventName}'.`);
    } else {
      payloadTypeNames.set(event.payloadTypeName, event.eventName);
    }
  }

  for (const topic of topics) {
    const existingTopic = topicTypeNames.get(topic.topicTypeName);
    if (existingTopic !== undefined) {
      issues.push(`Generated topic type '${topic.topicTypeName}' collides between topics '${existingTopic}' and '${topic.topicName}'.`);
    } else {
      topicTypeNames.set(topic.topicTypeName, topic.topicName);
    }
  }

  if (issues.length > 0) {
    throw new CatalogValidationError(issues);
  }
}
