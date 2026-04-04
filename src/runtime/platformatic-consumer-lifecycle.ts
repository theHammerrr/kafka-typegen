import type { RuntimeConsumerCloseOptions } from './types.js';
import { ACTIVE_STREAM_CLOSE_ERROR_MESSAGE } from './platformatic-consumer-options.js';
import type {
  PlatformaticConsumerLike,
  PlatformaticMessagesStream
} from './platformatic-types.js';

export interface PlatformaticTopicSubscription<TKey> {
  readonly stream: PlatformaticMessagesStream<TKey>;
}

export async function stopPlatformaticTopicStreams<TKey>(
  subscriptionsByTopic: Map<string, PlatformaticTopicSubscription<TKey>>
): Promise<void> {
  await Promise.all(
    [...subscriptionsByTopic.values()].map((subscription) =>
      subscription.stream.close()
    )
  );
  subscriptionsByTopic.clear();
}

export async function closePlatformaticConsumer<TKey>(
  consumer: PlatformaticConsumerLike<TKey>,
  subscriptionsByTopic: Map<string, PlatformaticTopicSubscription<TKey>>,
  closeOptions?: RuntimeConsumerCloseOptions
): Promise<void> {
  if (closeOptions?.force === true) {
    await consumer.close?.(true);
    subscriptionsByTopic.clear();
    return;
  }

  await stopPlatformaticTopicStreams(subscriptionsByTopic);

  try {
    await consumer.close?.(false);
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.includes(ACTIVE_STREAM_CLOSE_ERROR_MESSAGE)
    ) {
      await consumer.close?.(true);
      return;
    }

    throw error;
  }
}
