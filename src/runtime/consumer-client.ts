import { resolveObservability } from '../observability.js';
import { DefaultRuntimeConsumer } from './consumer-runtime.js';
import { resolveRuntimeSerialization } from './runtime-serialization.js';
import type { RuntimeConsumer, RuntimeConsumerOptions } from './types.js';

export function createRuntimeConsumer<TSubscriptionOptions = unknown>(
  options: RuntimeConsumerOptions<TSubscriptionOptions>
): RuntimeConsumer<TSubscriptionOptions> {
  const observability = resolveObservability(options);
  return new DefaultRuntimeConsumer<TSubscriptionOptions>({
    consumerTransport: options.consumerTransport,
    observability,
    producerTransport: {
      async send() {}
    },
    serialization: resolveRuntimeSerialization(options, observability)
  });
}
