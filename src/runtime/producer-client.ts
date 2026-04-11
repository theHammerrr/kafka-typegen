import { resolveObservability } from '../observability.js';
import { DefaultRuntimeProducer } from './producer-runtime.js';
import { resolveRuntimeSerialization } from './runtime-serialization.js';
import type { RuntimeProducer, RuntimeProducerOptions } from './types.js';

export function createRuntimeProducer<TSendOptions = unknown>(
  options: RuntimeProducerOptions<TSendOptions>
): RuntimeProducer<TSendOptions> {
  const observability = resolveObservability(options);
  return new DefaultRuntimeProducer<TSendOptions>({
    consumerTransport: {
      async onTopic() {}
    },
    observability,
    producerTransport: options.producerTransport,
    serialization: resolveRuntimeSerialization(options, observability)
  });
}
