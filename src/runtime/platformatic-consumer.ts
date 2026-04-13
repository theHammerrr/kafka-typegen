import {
  emitObservedEvent,
  type ResolvedKafkaTypegenObservability,
  toErrorString
} from '../observability.js';
import {
  type PlatformaticTopicSubscription,
  closePlatformaticConsumer,
  stopPlatformaticTopicStreams
} from './platformatic-consumer-lifecycle.js';
import {
  buildPlatformaticConsumerOptionsSignature,
  reportPlatformaticConsumerError
} from './platformatic-consumer-options.js';
import { toRuntimeIncomingMessage } from './platformatic-message.js';
import type { RuntimeIncomingMessage, RuntimeTransportConsumer } from './types.js';
import type {
  PlatformaticConsumerLike,
  PlatformaticConsumerSubscribeOptions,
  PlatformaticConsumerTransportOptions,
  PlatformaticMessage
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
            reportPlatformaticConsumerError(error, options.onError);
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
        reportPlatformaticConsumerError(error, options.onError);
      });
    },
    async stop() {
      await stopPlatformaticTopicStreams(subscriptionsByTopic);
    }
  };
}
