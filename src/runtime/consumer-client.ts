import { DefaultRuntimeConsumer } from './consumer-runtime.js';
import type { RuntimeClientOptions, RuntimeConsumer } from './types.js';

export function createRuntimeConsumer(options: Pick<RuntimeClientOptions, 'consumerTransport' | 'serialization'>): RuntimeConsumer {
  return new DefaultRuntimeConsumer({
    consumerTransport: options.consumerTransport,
    producerTransport: {
      async send() {}
    },
    serialization: options.serialization
  });
}
