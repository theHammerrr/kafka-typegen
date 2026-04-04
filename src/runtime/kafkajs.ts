import { createRuntimeClient } from './client.js';
import {
  createKafkaJsRuntimeConsumer,
  toKafkaJsRuntimeConsumer,
  type KafkaJsRuntimeConsumer
} from './kafkajs-consumer-client.js';
import { createKafkaJsConsumerTransport } from './kafkajs-consumer.js';
import {
  createKafkaJsRuntimeProducer,
  toKafkaJsRuntimeProducer,
  type KafkaJsRuntimeProducer
} from './kafkajs-producer-client.js';
import { createKafkaJsProducerTransport } from './kafkajs-producer.js';
import type {
  KafkaJsConsumerLike,
  KafkaJsProducerLike,
  KafkaJsRuntimeClientOptions
} from './kafkajs-types.js';
import type { RuntimeClient, RuntimeClientOptions } from './types.js';

export { createKafkaJsRuntimeConsumer } from './kafkajs-consumer-client.js';
export { createKafkaJsRuntimeProducer } from './kafkajs-producer-client.js';
export type {
  RuntimeClient,
  RuntimeClientOptions,
  RuntimeConsumer,
  RuntimeConsumerMessage,
  RuntimeEventMetadata,
  RuntimeIncomingMessage,
  RuntimeProducer,
  RuntimeSerializationHooks,
  RuntimeSerializationResult
} from './types.js';
export type { KafkaJsRuntimeClientOptions } from './kafkajs-types.js';

export type KafkaJsRuntimeClient<
  TProducer = KafkaJsProducerLike,
  TConsumer = KafkaJsConsumerLike
> = RuntimeClient & {
  readonly consumer: KafkaJsRuntimeConsumer<TConsumer>;
  readonly producer: KafkaJsRuntimeProducer<TProducer>;
};

export function createKafkaJsRuntimeClient<
  TProducer extends KafkaJsProducerLike = KafkaJsProducerLike,
  TConsumer extends KafkaJsConsumerLike = KafkaJsConsumerLike
>(
  options: KafkaJsRuntimeClientOptions<TProducer, TConsumer>
): KafkaJsRuntimeClient<TProducer, TConsumer> {
  const consumerTransport = createKafkaJsConsumerTransport(options.consumer, {
    ...(options.onError !== undefined ? { onError: options.onError } : {}),
    ...(options.runOptions !== undefined ? { runOptions: options.runOptions } : {})
  });
  const runtime = createRuntimeClient({
    consumerTransport,
    producerTransport: createKafkaJsProducerTransport(options.producer),
    ...(options.schemaRegistry !== undefined
      ? { schemaRegistry: options.schemaRegistry }
      : { serialization: options.serialization })
  } satisfies RuntimeClientOptions);

  return Object.assign(
    Object.create(runtime),
    {
      consumer: toKafkaJsRuntimeConsumer(options.consumer, consumerTransport, runtime.consumer),
      producer: toKafkaJsRuntimeProducer(options.producer, runtime.producer)
    }
  ) as KafkaJsRuntimeClient<TProducer, TConsumer>;
}
