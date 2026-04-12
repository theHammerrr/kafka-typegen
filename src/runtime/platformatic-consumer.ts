import type { RuntimeIncomingMessage, RuntimeTransportConsumer } from './types.js';
import { toRuntimeIncomingMessage } from './platformatic-message.js';
import type {
  PlatformaticConsumerLike,
  PlatformaticConsumerSubscribeOptions,
  PlatformaticConsumerTransportOptions,
  PlatformaticMessage,
  PlatformaticMessagesStream
} from './platformatic-types.js';

type PlatformaticTopicHandler = (
  message: RuntimeIncomingMessage
) => Promise<void> | void;

interface PlatformaticTopicSubscription<TKey> {
  readonly handlers: Set<PlatformaticTopicHandler>;
  readonly optionsSignature: string;
  readonly stream: PlatformaticMessagesStream<TKey>;
}

function reportConsumerError(
  error: unknown,
  onError: ((error: unknown) => void) | undefined
): void {
  if (onError !== undefined) {
    onError(error);
    return;
  }

  queueMicrotask(() => {
    throw error;
  });
}

function stringifyOptionValue(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stringifyOptionValue(item)).join(',')}]`;
  }

  return `{${Object.entries(value as Record<string, unknown>)
    .sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey))
    .map(([key, entryValue]) => `${JSON.stringify(key)}:${stringifyOptionValue(entryValue)}`)
    .join(',')}}`;
}

function buildOptionsSignature<TKey>(
  options: PlatformaticConsumerTransportOptions<TKey>,
  subscribeOptions: PlatformaticConsumerSubscribeOptions<TKey> | undefined
): string {
  return stringifyOptionValue({
    ...(options.consumeOptions ?? {}),
    ...(subscribeOptions ?? {})
  });
}

export function createPlatformaticConsumerTransport<TKey = Buffer>(
  consumer: PlatformaticConsumerLike<TKey>,
  options: PlatformaticConsumerTransportOptions<TKey> = {}
): RuntimeTransportConsumer<PlatformaticConsumerSubscribeOptions<TKey>> {
  const subscriptionsByTopic = new Map<
    string,
    PlatformaticTopicSubscription<TKey>
  >();

  return {
    async onTopic(topicName, handler, subscribeOptions) {
      const optionsSignature = buildOptionsSignature(options, subscribeOptions);
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
          void Promise.resolve(topicHandler(runtimeMessage)).catch((error) => {
            reportConsumerError(error, options.onError);
          });
        }
      });

      stream.on('error', (error) => {
        reportConsumerError(error, options.onError);
      });
    }
  };
}
