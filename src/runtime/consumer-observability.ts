import { emitObservedEvent, toErrorString } from '../observability.js';
import { toConsumerMessage } from './consumer-message.js';
import type {
  ResolvedRuntimeClientOptions,
  RuntimeConsumerMessage,
  RuntimeEventMetadata,
  RuntimeIncomingMessage
} from './types.js';

export async function handleObservedConsumerMessage<TPayload>(
  options: ResolvedRuntimeClientOptions<unknown, unknown>,
  metadata: RuntimeEventMetadata,
  message: RuntimeIncomingMessage,
  handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void
): Promise<void> {
  await emitObservedEvent(options.observability, {
    eventName: metadata.eventName,
    topicName: metadata.topicName,
    type: 'runtime.consumer.handle.start'
  });

  try {
    const payload = await options.serialization.deserialize<TPayload>(metadata, message);
    await handler(toConsumerMessage(metadata, message, payload));
    await emitObservedEvent(options.observability, {
      eventName: metadata.eventName,
      topicName: metadata.topicName,
      type: 'runtime.consumer.handle.success'
    });
  } catch (error) {
    options.observability.logger.error('Runtime consumer handler failed.', {
      error: toErrorString(error),
      eventName: metadata.eventName,
      topicName: metadata.topicName
    });
    await emitObservedEvent(options.observability, {
      error: toErrorString(error),
      eventName: metadata.eventName,
      topicName: metadata.topicName,
      type: 'runtime.consumer.handle.failure'
    });
    throw error;
  }
}
