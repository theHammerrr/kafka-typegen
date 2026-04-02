import { createRuntimeConsumer } from './consumer-client.js';
import { createPlatformaticConsumerTransport } from './platformatic-consumer.js';
import type { PlatformaticConsumerLike, PlatformaticConsumerTransportOptions } from './platformatic-types.js';
import type { RuntimeConsumer, RuntimeSerializationHooks } from './types.js';

export interface PlatformaticRuntimeConsumerOptions<TKey = unknown>
  extends PlatformaticConsumerTransportOptions<TKey> {
  readonly consumer: PlatformaticConsumerLike<TKey>;
  readonly serialization: RuntimeSerializationHooks;
}

export function createPlatformaticRuntimeConsumer<TKey = unknown>(
  options: PlatformaticRuntimeConsumerOptions<TKey>
): RuntimeConsumer {
  return createRuntimeConsumer({
    consumerTransport: createPlatformaticConsumerTransport(options.consumer, {
      ...(options.consumeOptions !== undefined ? { consumeOptions: options.consumeOptions } : {})
    }),
    serialization: options.serialization
  });
}
