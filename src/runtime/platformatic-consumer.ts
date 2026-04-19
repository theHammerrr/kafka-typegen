import type { ResolvedKafkaTypegenObservability } from '../observability.js';
import {
  type PlatformaticTopicSubscription,
  closePlatformaticConsumer,
  stopPlatformaticTopicStreams
} from './platformatic-consumer-lifecycle.js';
import {
  buildPlatformaticConsumerOptionsSignature,
  reportPlatformaticConsumerError
} from './platformatic-consumer-options.js';
import { attachPlatformaticConsumerStream } from './platformatic-consumer-stream.js';
import type { RuntimeIncomingMessage, RuntimeTransportConsumer } from './types.js';
import type {
  PlatformaticConsumerLike,
  PlatformaticConsumerSubscribeOptions,
  PlatformaticConsumerTransportOptions
} from './platformatic-types.js';

type PlatformaticTopicHandler = (
  message: RuntimeIncomingMessage
) => Promise<void> | void;

interface PlatformaticTopicSubscriptionState<TKey>
  extends PlatformaticTopicSubscription<TKey> {
  readonly handlers: Set<PlatformaticTopicHandler>;
  readonly optionsSignature: string;
}

export function createPlatformaticConsumerTransport<TKey = Buffer>(
  consumer: PlatformaticConsumerLike<TKey>,
  options: PlatformaticConsumerTransportOptions<TKey> = {},
  observability?: ResolvedKafkaTypegenObservability
): RuntimeTransportConsumer<PlatformaticConsumerSubscribeOptions<TKey>> {
  const subscriptionsByTopic = new Map<
    string,
    PlatformaticTopicSubscriptionState<TKey>
  >();

  return {
    async close(closeOptions) {
      await closePlatformaticConsumer(consumer, subscriptionsByTopic, closeOptions);
    },
    async onTopic(topicName, handler, subscribeOptions) {
      const optionsSignature = buildPlatformaticConsumerOptionsSignature(
        options,
        subscribeOptions
      );
      const existingSubscription = subscriptionsByTopic.get(topicName);

      if (existingSubscription !== undefined) {
        if (existingSubscription.optionsSignature !== optionsSignature) {
          throw new Error(
            `Topic '${topicName}' is already subscribed with different consume options.`
          );
        }

        existingSubscription.handlers.add(handler);
        return;
      }

      const topicHandlers = new Set<PlatformaticTopicHandler>([handler]);
      const stream = await consumer.consume({
        ...(options.consumeOptions ?? {}),
        ...(subscribeOptions ?? {}),
        topics: [topicName]
      });

      subscriptionsByTopic.set(topicName, {
        handlers: topicHandlers,
        optionsSignature,
        stream
      });
      attachPlatformaticConsumerStream(
        stream,
        topicName,
        topicHandlers,
        options.onError,
        observability
      );
    },
    async stop() {
      await stopPlatformaticTopicStreams(subscriptionsByTopic);
    }
  };
}
