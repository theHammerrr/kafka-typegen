import { createRuntimeClient } from './client.js';
export type {
  RuntimeClient,
  RuntimeClientOptions,
  RuntimeConsumer,
  RuntimeConsumerMessage,
  RuntimeEventMetadata,
  RuntimeIncomingMessage,
  RuntimeOutgoingMessage,
  RuntimeProducer,
  RuntimeSerializationHooks,
  RuntimeSerializationResult,
  RuntimeTransportConsumer,
  RuntimeTransportProducer
} from './types.js';
import type { RuntimeClient, RuntimeClientOptions } from './types.js';
import { createPlatformaticConsumerTransport } from './platformatic-consumer.js';
import { createPlatformaticProducerTransport } from './platformatic-producer.js';
export type { PlatformaticConsumerTransportOptions, PlatformaticRuntimeClientOptions } from './platformatic-types.js';
import type { PlatformaticRuntimeClientOptions } from './platformatic-types.js';

export { createPlatformaticConsumerTransport } from './platformatic-consumer.js';
export { createPlatformaticProducerTransport } from './platformatic-producer.js';

export function createPlatformaticRuntimeClient<TKey = unknown>(
  options: PlatformaticRuntimeClientOptions<TKey>
): RuntimeClient {
  const runtimeOptions: RuntimeClientOptions = {
    consumerTransport: createPlatformaticConsumerTransport(options.consumer, {
      ...(options.consumeOptions !== undefined ? { consumeOptions: options.consumeOptions } : {})
    }),
    producerTransport: createPlatformaticProducerTransport(options.producer),
    serialization: options.serialization
  };

  return createRuntimeClient(runtimeOptions);
}
