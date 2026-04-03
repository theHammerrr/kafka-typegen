import { DefaultRuntimeConsumer } from './consumer-runtime.js';
import { resolveRuntimeSerialization } from './runtime-serialization.js';
import type { RuntimeConsumer, RuntimeConsumerOptions } from './types.js';

export function createRuntimeConsumer(options: RuntimeConsumerOptions): RuntimeConsumer {
  return new DefaultRuntimeConsumer({
    consumerTransport: options.consumerTransport,
    producerTransport: {
      async send() {}
    },
    serialization: resolveRuntimeSerialization(options)
  });
}
