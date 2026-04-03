import { DefaultRuntimeConsumer } from './consumer-runtime.js';
import { DefaultRuntimeProducer } from './producer-runtime.js';
import { resolveRuntimeSerialization } from './runtime-serialization.js';
import type { ResolvedRuntimeClientOptions, RuntimeClient, RuntimeClientOptions } from './types.js';

export function createRuntimeClient(options: RuntimeClientOptions): RuntimeClient {
  const resolvedOptions: ResolvedRuntimeClientOptions = {
    consumerTransport: options.consumerTransport,
    producerTransport: options.producerTransport,
    serialization: resolveRuntimeSerialization(options)
  };

  return {
    consumer: new DefaultRuntimeConsumer(resolvedOptions),
    producer: new DefaultRuntimeProducer(resolvedOptions)
  };
}
