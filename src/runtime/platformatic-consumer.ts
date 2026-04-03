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
  options: PlatformaticConsumerTransportOptions<TKey> = {}
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
          void Promise.resolve(topicHandler(runtimeMessage));
        }
      });
      stream.on('error', () => {});
    }
  };
}
