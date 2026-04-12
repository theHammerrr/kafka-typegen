import { resolveObservability } from '../observability.js';
import { DefaultRuntimeConsumer } from './consumer-runtime.js';
import { DefaultRuntimeProducer } from './producer-runtime.js';
import { resolveRuntimeSerialization } from './runtime-serialization.js';
import type { ResolvedRuntimeClientOptions, RuntimeClient, RuntimeClientOptions } from './types.js';

export function createRuntimeClient<
  TSendOptions = unknown,
  TSubscriptionOptions = unknown
>(
  options: RuntimeClientOptions<TSendOptions, TSubscriptionOptions>
): RuntimeClient<TSendOptions, TSubscriptionOptions> {
  const observability = resolveObservability(options);
  const resolvedOptions: ResolvedRuntimeClientOptions<
    TSendOptions,
    TSubscriptionOptions
  > = {
    consumerTransport: options.consumerTransport,
    observability,
    producerTransport: options.producerTransport,
    serialization: resolveRuntimeSerialization(options, observability)
  };

  return {
    consumer: new DefaultRuntimeConsumer<TSubscriptionOptions>(resolvedOptions),
    producer: new DefaultRuntimeProducer<TSendOptions>(resolvedOptions)
  };
}
