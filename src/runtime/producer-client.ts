import { DefaultRuntimeProducer } from './producer-runtime.js';
import { resolveRuntimeSerialization } from './runtime-serialization.js';
import type { RuntimeProducer, RuntimeProducerOptions } from './types.js';

export function createRuntimeProducer(options: RuntimeProducerOptions): RuntimeProducer {
  return new DefaultRuntimeProducer({
    consumerTransport: {
      async onTopic() {}
    },
    producerTransport: options.producerTransport,
    serialization: resolveRuntimeSerialization(options)
  });
}
