import type { RuntimeConsumerMessage, RuntimeEventMetadata, RuntimeIncomingMessage } from './types.js';

export function toConsumerMessage<TPayload>(
  metadata: RuntimeEventMetadata,
  message: RuntimeIncomingMessage,
  payload: TPayload
): RuntimeConsumerMessage<TPayload> {
  return {
    event: metadata.eventName,
    eventName: metadata.eventName,
    payload,
    topic: metadata.topicName,
    topicName: metadata.topicName,
    ...(message.headers !== undefined ? { headers: message.headers } : {}),
    ...(message.key !== undefined ? { key: message.key } : {}),
    ...(message.offset !== undefined ? { offset: message.offset } : {}),
    ...(message.partition !== undefined ? { partition: message.partition } : {}),
    ...(message.schemaId !== undefined ? { schemaId: message.schemaId } : {}),
    ...(message.timestamp !== undefined ? { timestamp: message.timestamp } : {})
  };
}
