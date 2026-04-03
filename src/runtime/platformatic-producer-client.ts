import { createRuntimeProducer } from './producer-client.js';
import { createPlatformaticProducerTransport } from './platformatic-producer.js';
import type { PlatformaticProducerLike } from './platformatic-types.js';
import type { RuntimeProducer, RuntimeSerializationHooks } from './types.js';

export interface PlatformaticRuntimeProducerOptions<TKey = unknown> {
  readonly producer: PlatformaticProducerLike<TKey>;
  readonly serialization: RuntimeSerializationHooks;
}

export function createPlatformaticRuntimeProducer<TKey = unknown>(
  options: PlatformaticRuntimeProducerOptions<TKey>
): RuntimeProducer {
  return createRuntimeProducer({
    producerTransport: createPlatformaticProducerTransport(options.producer),
    serialization: options.serialization
  });
}
