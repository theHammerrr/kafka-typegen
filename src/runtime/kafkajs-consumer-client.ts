import { createRuntimeClientProxy } from './client-proxy.js';
import {
  type KafkaJsConsumerTransport,
  createKafkaJsConsumerTransport
} from './kafkajs-consumer.js';
import type {
  KafkaJsConsumerLike,
  KafkaJsConsumerRunOptions,
  KafkaJsConsumerSubscribeOptions,
  KafkaJsConsumerTransportOptions
} from './kafkajs-types.js';
import { createRuntimeConsumer } from './consumer-client.js';
import type {
  RuntimeConsumer,
  RuntimeEventMetadata,
  RuntimeSerializationOptions
} from './types.js';

type KafkaJsConsumerEventSource = {
  on?: (
    eventName: string | symbol,
    listener: (...args: unknown[]) => void
  ) => unknown;
};

export type KafkaJsRuntimeConsumer<TConsumer = KafkaJsConsumerLike> = Omit<
  TConsumer,
  'run' | 'stop'
> &
  RuntimeConsumer<KafkaJsConsumerSubscribeOptions> & {
    run(options?: KafkaJsConsumerRunOptions): Promise<void>;
  };

export type KafkaJsRuntimeConsumerOptions<
  TConsumer extends KafkaJsConsumerLike = KafkaJsConsumerLike
> = KafkaJsConsumerTransportOptions &
  RuntimeSerializationOptions & {
    readonly consumer: TConsumer;
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

export function createKafkaJsRuntimeConsumer<
  TConsumer extends KafkaJsConsumerLike = KafkaJsConsumerLike
>(
  options: KafkaJsRuntimeConsumerOptions<TConsumer>
): KafkaJsRuntimeConsumer<TConsumer> {
  const transport = createKafkaJsConsumerTransport(options.consumer, {
    ...(options.onError !== undefined ? { onError: options.onError } : {}),
    ...(options.runOptions !== undefined
      ? { runOptions: options.runOptions }
      : {})
  });

  return toKafkaJsRuntimeConsumer(
    options.consumer,
    transport,
    createRuntimeConsumer({
      consumerTransport: transport,
      ...(options.schemaRegistry !== undefined
        ? { schemaRegistry: options.schemaRegistry }
        : { serialization: options.serialization })
    })
  );
}

export function toKafkaJsRuntimeConsumer<
  TConsumer extends KafkaJsConsumerLike = KafkaJsConsumerLike
>(
  consumer: TConsumer,
  transport: KafkaJsConsumerTransport,
  runtimeConsumer: RuntimeConsumer<KafkaJsConsumerSubscribeOptions>
): KafkaJsRuntimeConsumer<TConsumer> {
  const nativeOn = (consumer as KafkaJsConsumerEventSource).on?.bind(consumer);

  return createRuntimeClientProxy(consumer, {
    close: runtimeConsumer.close.bind(runtimeConsumer),
    on: ((
      eventOrMetadata: unknown,
      handler: unknown,
      subscribeOptions?: unknown
    ) =>
      isRuntimeEventMetadata(eventOrMetadata)
        ? runtimeConsumer.on(
            eventOrMetadata,
            handler as Parameters<RuntimeConsumer['on']>[1],
            subscribeOptions as never
          )
        : nativeOn?.(
            eventOrMetadata as string | symbol,
            handler as (...args: unknown[]) => void
          )) as KafkaJsRuntimeConsumer<TConsumer>['on'],
    onTopic: runtimeConsumer.onTopic.bind(runtimeConsumer),
    run: transport.run.bind(transport),
    stop: runtimeConsumer.stop.bind(runtimeConsumer)
  });
}
