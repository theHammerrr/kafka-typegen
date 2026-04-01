import type { RuntimeIncomingMessage } from './types.js';
import { toRuntimeHeaders } from './platformatic-headers.js';
import type { PlatformaticMessage } from './platformatic-types.js';

export function toRuntimeIncomingMessage<TKey>(message: PlatformaticMessage<TKey>): RuntimeIncomingMessage {
  const headers = message.headers.size > 0 ? toRuntimeHeaders(message.headers) : undefined;

  return {
    topicName: message.topic,
    value: message.value,
    ...(headers !== undefined ? { headers } : {}),
    ...(message.key !== undefined ? { key: message.key } : {}),
    ...(message.offset !== undefined ? { offset: message.offset.toString() } : {}),
    ...(message.partition !== undefined ? { partition: message.partition } : {}),
    ...(message.schemaId !== undefined ? { schemaId: message.schemaId } : {}),
    ...(message.timestamp !== undefined ? { timestamp: message.timestamp.toString() } : {})
  };
}
