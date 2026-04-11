import { emitObservedEvent, type ResolvedKafkaTypegenObservability, toErrorString } from '../observability.js';
import type { RuntimeIncomingMessage, RuntimeTransportConsumer } from './types.js';
import { toRuntimeIncomingMessage } from './platformatic-message.js';
import type {
  PlatformaticConsumerLike,
  PlatformaticConsumerSubscribeOptions,
  PlatformaticConsumerTransportOptions,
  PlatformaticMessage,
  PlatformaticMessagesStream
} from './platformatic-types.js';

export function createPlatformaticConsumerTransport<TKey = Buffer>(
  consumer: PlatformaticConsumerLike<TKey>,
  options: PlatformaticConsumerTransportOptions<TKey> = {},
  observability?: ResolvedKafkaTypegenObservability
): RuntimeTransportConsumer<PlatformaticConsumerSubscribeOptions<TKey>> {
  const handlersByTopic = new Map<string, Set<(message: RuntimeIncomingMessage) => Promise<void> | void>>();
  const streamsByTopic = new Map<string, PlatformaticMessagesStream<TKey>>();

  return {
    async onTopic(topicName, handler, subscribeOptions) {
      const existingHandlers = handlersByTopic.get(topicName);
      if (existingHandlers !== undefined) {
        existingHandlers.add(handler);
        return;
      }

      const topicHandlers = new Set<(message: RuntimeIncomingMessage) => Promise<void> | void>([handler]);
      handlersByTopic.set(topicName, topicHandlers);

      const stream = await consumer.consume({
        ...(options.consumeOptions ?? {}),
        ...(subscribeOptions ?? {}),
        topics: [topicName]
      });
      streamsByTopic.set(topicName, stream);
      stream.on('data', (message) => {
        const runtimeMessage = toRuntimeIncomingMessage(message as PlatformaticMessage<TKey>);
        for (const topicHandler of topicHandlers) {
          void Promise.resolve(topicHandler(runtimeMessage)).catch(async (error) => {
            observability?.logger.error('Platformatic topic handler failed.', {
              error: toErrorString(error),
              topicName
            });
            if (observability !== undefined) {
              await emitObservedEvent(observability, {
                error: toErrorString(error),
                source: 'platformatic-handler',
                topicName,
                type: 'runtime.consumer.background-error'
              });
            }
          });
        }
      });
      stream.on('error', (error) => {
        observability?.logger.error('Platformatic consumer stream error.', {
          error: toErrorString(error),
          topicName
        });
        if (observability !== undefined) {
          void emitObservedEvent(observability, {
            error: toErrorString(error),
            source: 'platformatic-stream',
            topicName,
            type: 'runtime.consumer.background-error'
          });
        }
      });
    }
  };
}
