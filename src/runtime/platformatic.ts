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
import {
  createPlatformaticRuntimeConsumer,
  toPlatformaticRuntimeConsumer,
  type PlatformaticRuntimeConsumer
} from './platformatic-consumer-client.js';
import { createPlatformaticProducerTransport } from './platformatic-producer.js';
import {
  createPlatformaticRuntimeProducer,
  toPlatformaticRuntimeProducer,
  type PlatformaticRuntimeProducer
} from './platformatic-producer-client.js';
export type { PlatformaticConsumerTransportOptions, PlatformaticRuntimeClientOptions } from './platformatic-types.js';
import type {
  PlatformaticConsumerLike,
  PlatformaticProducerLike,
  PlatformaticRuntimeClientOptions
} from './platformatic-types.js';

export { createPlatformaticConsumerTransport } from './platformatic-consumer.js';
export { createPlatformaticProducerTransport } from './platformatic-producer.js';
export { createPlatformaticRuntimeConsumer } from './platformatic-consumer-client.js';
export { createPlatformaticRuntimeProducer } from './platformatic-producer-client.js';

export type PlatformaticRuntimeClient<
  TProducer = PlatformaticProducerLike,
  TConsumer = PlatformaticConsumerLike
> = RuntimeClient & {
  readonly consumer: PlatformaticRuntimeConsumer<TConsumer>;
  readonly producer: PlatformaticRuntimeProducer<TProducer>;
};

export function createPlatformaticRuntimeClient<
  TKey = Buffer,
  TProducer extends PlatformaticProducerLike<TKey> = PlatformaticProducerLike<TKey>,
  TConsumer extends PlatformaticConsumerLike<TKey> = PlatformaticConsumerLike<TKey>
>(
  options: PlatformaticRuntimeClientOptions<TKey, TProducer, TConsumer>
): PlatformaticRuntimeClient<TProducer, TConsumer> {
  const runtime = createRuntimeClient({
    consumerTransport: createPlatformaticConsumerTransport(options.consumer, {
      ...(options.consumeOptions !== undefined
        ? { consumeOptions: options.consumeOptions }
        : {})
    }),
    producerTransport: createPlatformaticProducerTransport(options.producer),
    ...(options.schemaRegistry !== undefined
      ? { schemaRegistry: options.schemaRegistry }
      : { serialization: options.serialization })
  } satisfies RuntimeClientOptions);

  return Object.assign(
    Object.create(runtime),
    {
      consumer: toPlatformaticRuntimeConsumer<TKey, TConsumer>(
        options.consumer,
        runtime.consumer
      ),
      producer: toPlatformaticRuntimeProducer<TKey, TProducer>(
        options.producer,
        runtime.producer
      )
    }
  ) as PlatformaticRuntimeClient<TProducer, TConsumer>;
}
