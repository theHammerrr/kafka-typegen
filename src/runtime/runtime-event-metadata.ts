import type { RuntimeEventMetadata } from './types.js';

export function isRuntimeEventMetadata(value: unknown): value is RuntimeEventMetadata {
  return (
    typeof value === 'object' &&
    value !== null &&
    'eventName' in value &&
    'payloadTypeName' in value &&
    'schemaFilePath' in value &&
    'schemaName' in value &&
    'subjectName' in value &&
    'topicName' in value
  );
}
