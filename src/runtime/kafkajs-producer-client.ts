import { createRuntimeClientProxy } from './client-proxy.js';
import {
  createKafkaJsProducerTransport
} from './kafkajs-producer.js';
import type {
  KafkaJsProducerLike,
  KafkaJsProducerSendOptions
} from './kafkajs-types.js';
import { createRuntimeProducer } from './producer-client.js';
import type {
  RuntimeEventMetadata,
  RuntimeProducer,
  RuntimeSerializationOptions
} from './types.js';

type KafkaJsSendDelegate = (
  message: unknown
) => Promise<unknown>;

export type KafkaJsRuntimeProducer<TProducer = KafkaJsProducerLike> = TProducer &
  RuntimeProducer<KafkaJsProducerSendOptions>;

export type KafkaJsRuntimeProducerOptions<
  TProducer extends KafkaJsProducerLike = KafkaJsProducerLike
> = RuntimeSerializationOptions & {
  readonly producer: TProducer;
};

function isRuntimeEventMetadata(value: unknown): value is RuntimeEventMetadata {
  return (
    typeof value === 'object' &&
    value !== null &&
    'eventName' in value &&
    'payloadTypeName' in value &&
    'schemaFilePath' in value &&
    'schemaName' in value &&
    'subjectName' in value &&
    'topicName' in value
  );
}

export function createKafkaJsRuntimeProducer<
  TProducer extends KafkaJsProducerLike = KafkaJsProducerLike
>(
  options: KafkaJsRuntimeProducerOptions<TProducer>
): KafkaJsRuntimeProducer<TProducer> {
  return toKafkaJsRuntimeProducer(
    options.producer,
    createRuntimeProducer({
      producerTransport: createKafkaJsProducerTransport(options.producer),
      ...(options.schemaRegistry !== undefined
        ? { schemaRegistry: options.schemaRegistry }
        : { serialization: options.serialization })
    })
  );
}

export function toKafkaJsRuntimeProducer<
  TProducer extends KafkaJsProducerLike = KafkaJsProducerLike
>(
  producer: TProducer,
  runtimeProducer: RuntimeProducer<KafkaJsProducerSendOptions>
): KafkaJsRuntimeProducer<TProducer> {
  const send = producer.send.bind(producer) as KafkaJsSendDelegate;

  return createRuntimeClientProxy(producer, {
    send: ((messageOrMetadata: unknown, payload?: unknown, sendOptions?: unknown) =>
      isRuntimeEventMetadata(messageOrMetadata)
        ? runtimeProducer.send(messageOrMetadata, payload, sendOptions as never)
        : send(messageOrMetadata)) as KafkaJsRuntimeProducer<TProducer>['send']
  });
}
