import { DefaultRuntimeConsumer } from './consumer-runtime.js';
import { DefaultRuntimeProducer } from './producer-runtime.js';
import type { RuntimeClient, RuntimeClientOptions } from './types.js';

export function createRuntimeClient(options: RuntimeClientOptions): RuntimeClient {
  return {
    consumer: new DefaultRuntimeConsumer(options),
    producer: new DefaultRuntimeProducer(options)
  };
}
