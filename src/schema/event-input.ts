import type { NormalizedEventConfig } from '../config/index.js';

import type { EventSchemaInput } from './types.js';

export function toEventSchemaInput(event: EventSchemaInput | NormalizedEventConfig): EventSchemaInput {
  if ('filePath' in event) {
    return event;
  }

  return {
    eventName: event.eventName,
    filePath: event.resolvedSchemaPath,
    subjectName: event.subjectName,
    topicName: event.topicName
  };
}
