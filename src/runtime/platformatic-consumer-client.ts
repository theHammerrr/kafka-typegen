import { createRuntimeConsumer } from './consumer-client.js';
import { createPlatformaticConsumerTransport } from './platformatic-consumer.js';
import type { PlatformaticConsumerLike, PlatformaticConsumerTransportOptions } from './platformatic-types.js';
import type { RuntimeConsumer, RuntimeSerializationOptions } from './types.js';

export type PlatformaticRuntimeConsumerOptions<TKey = unknown> = PlatformaticConsumerTransportOptions<TKey> &
  RuntimeSerializationOptions & {
  readonly consumer: PlatformaticConsumerLike<TKey>;
};

export function createPlatformaticRuntimeConsumer<TKey = unknown>(
  options: PlatformaticRuntimeConsumerOptions<TKey>
): RuntimeConsumer {
  return createRuntimeConsumer({
    consumerTransport: createPlatformaticConsumerTransport(options.consumer, {
      ...(options.consumeOptions !== undefined ? { consumeOptions: options.consumeOptions } : {})
    }),
    ...(options.schemaRegistry !== undefined
      ? { schemaRegistry: options.schemaRegistry }
      : { serialization: options.serialization })
  });
}
