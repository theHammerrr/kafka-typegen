import { createRuntimeProducer } from './producer-client.js';
import { createPlatformaticProducerTransport } from './platformatic-producer.js';
import type { PlatformaticProducerLike } from './platformatic-types.js';
import type { RuntimeProducer, RuntimeSerializationOptions } from './types.js';

export type PlatformaticRuntimeProducerOptions<TKey = unknown> = RuntimeSerializationOptions & {
  readonly producer: PlatformaticProducerLike<TKey>;
};

export function createPlatformaticRuntimeProducer<TKey = unknown>(
  options: PlatformaticRuntimeProducerOptions<TKey>
): RuntimeProducer {
  return createRuntimeProducer({
    producerTransport: createPlatformaticProducerTransport(options.producer),
    ...(options.schemaRegistry !== undefined
      ? { schemaRegistry: options.schemaRegistry }
      : { serialization: options.serialization })
  });
}
