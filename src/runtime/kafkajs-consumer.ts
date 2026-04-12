import type {
  KafkaJsConsumerLike,
  KafkaJsConsumerRunOptions,
  KafkaJsConsumerSubscribeOptions,
  KafkaJsConsumerTransportOptions
} from './kafkajs-types.js';
import {
  buildKafkaJsConsumerOptionsSignature,
  reportKafkaJsConsumerError
} from './kafkajs-consumer-options.js';
import { toRuntimeIncomingMessage } from './kafkajs-message.js';
import type { RuntimeIncomingMessage, RuntimeTransportConsumer } from './types.js';

type KafkaJsTopicHandler = (
  message: RuntimeIncomingMessage
) => Promise<void> | void;

export type KafkaJsConsumerTransport = RuntimeTransportConsumer<
  KafkaJsConsumerSubscribeOptions
> & {
  run(options?: KafkaJsConsumerRunOptions): Promise<void>;
  stop(): Promise<void>;
};

interface KafkaJsTopicSubscription {
  readonly handlers: Set<KafkaJsTopicHandler>;
  readonly optionsSignature: string;
}

interface KafkaJsConsumerEventSource {
  readonly events?: {
    readonly CRASH?: string;
  };
  on?: (
    eventName: string,
    listener: (event: { payload?: { error?: unknown } }) => void
  ) => unknown;
}

export function createKafkaJsConsumerTransport(
  consumer: KafkaJsConsumerLike,
  options: KafkaJsConsumerTransportOptions = {}
): KafkaJsConsumerTransport {
  const subscriptionsByTopic = new Map<string, KafkaJsTopicSubscription>();
  const crashEvent = (consumer as KafkaJsConsumerEventSource).events?.CRASH;
  const nativeOn = (consumer as KafkaJsConsumerEventSource).on?.bind(consumer);
  let isRunning = false;

  if (crashEvent !== undefined && nativeOn !== undefined) {
    nativeOn(crashEvent, (event) => {
      reportKafkaJsConsumerError(event.payload?.error ?? event, options.onError);
    });
  }

  return {
    async onTopic(topicName, handler, subscribeOptions) {
      const optionsSignature = buildKafkaJsConsumerOptionsSignature(subscribeOptions);
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

      if (isRunning) {
        throw new Error(
          `Topic '${topicName}' cannot be subscribed after the KafkaJS consumer has started.`
        );
      }

      await consumer.subscribe({
        ...(subscribeOptions ?? {}),
        topic: topicName
      });

      subscriptionsByTopic.set(topicName, {
        handlers: new Set<KafkaJsTopicHandler>([handler]),
        optionsSignature
      });
    },
    async run(runOptions) {
      isRunning = true;

      await consumer.run({
        ...(options.runOptions ?? {}),
        ...(runOptions ?? {}),
        eachMessage: async (payload) => {
          const subscription = subscriptionsByTopic.get(payload.topic);
          if (subscription === undefined) {
            return;
          }

          const runtimeMessage = toRuntimeIncomingMessage(payload);

          for (const topicHandler of subscription.handlers) {
            await Promise.resolve(topicHandler(runtimeMessage)).catch((error) => {
              reportKafkaJsConsumerError(error, options.onError);
            });
          }
        }
      });
    },
    async stop() {
      await consumer.stop();
      isRunning = false;
    }
  };
}
