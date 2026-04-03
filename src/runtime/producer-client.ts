import { DefaultRuntimeProducer } from './producer-runtime.js';
import type { RuntimeClientOptions, RuntimeProducer } from './types.js';

export function createRuntimeProducer(options: Pick<RuntimeClientOptions, 'producerTransport' | 'serialization'>): RuntimeProducer {
  return new DefaultRuntimeProducer({
    consumerTransport: {
      async onTopic() {}
    },
    producerTransport: options.producerTransport,
    serialization: options.serialization
  });
}
