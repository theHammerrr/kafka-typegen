import { createRuntimeClient } from './client.js';
export type {
  SchemaRegistryRuntimeClient,
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
import { createPlatformaticRuntimeConsumer } from './platformatic-consumer-client.js';
import { createPlatformaticProducerTransport } from './platformatic-producer.js';
import { createPlatformaticRuntimeProducer } from './platformatic-producer-client.js';
export type { PlatformaticConsumerTransportOptions, PlatformaticRuntimeClientOptions } from './platformatic-types.js';
import type { PlatformaticRuntimeClientOptions } from './platformatic-types.js';

export { createPlatformaticConsumerTransport } from './platformatic-consumer.js';
export { createPlatformaticProducerTransport } from './platformatic-producer.js';
export { createPlatformaticRuntimeConsumer } from './platformatic-consumer-client.js';
export { createPlatformaticRuntimeProducer } from './platformatic-producer-client.js';

export function createPlatformaticRuntimeClient<TKey = unknown>(
  options: PlatformaticRuntimeClientOptions<TKey>
): RuntimeClient {
  const runtimeOptions: RuntimeClientOptions = {
    consumerTransport: createPlatformaticConsumerTransport(options.consumer, {
      ...(options.consumeOptions !== undefined ? { consumeOptions: options.consumeOptions } : {})
    }),
    producerTransport: createPlatformaticProducerTransport(options.producer),
    ...(options.schemaRegistry !== undefined
      ? { schemaRegistry: options.schemaRegistry }
      : { serialization: options.serialization })
  };

  return createRuntimeClient(runtimeOptions);
}
