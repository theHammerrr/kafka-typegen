import { createPlatformaticClientProxy } from './platformatic-client-proxy.js';
import { createRuntimeConsumer } from './consumer-client.js';
import { createPlatformaticConsumerTransport } from './platformatic-consumer.js';
import type {
  PlatformaticConsumerLike,
  PlatformaticConsumerSubscribeOptions,
  PlatformaticConsumerTransportOptions
} from './platformatic-types.js';
import type {
  RuntimeConsumer,
  RuntimeEventMetadata,
  RuntimeSerializationOptions
} from './types.js';

type PlatformaticConsumerEventSource = {
  on?: (eventName: string | symbol, listener: (...args: unknown[]) => void) => unknown;
};

export type PlatformaticRuntimeConsumer<TConsumer = PlatformaticConsumerLike> = TConsumer &
  RuntimeConsumer<
    TConsumer extends PlatformaticConsumerLike<infer TKey>
      ? PlatformaticConsumerSubscribeOptions<TKey>
      : never
  >;

export type PlatformaticRuntimeConsumerOptions<
  TKey = unknown,
  TConsumer extends PlatformaticConsumerLike<TKey> = PlatformaticConsumerLike<TKey>
> = PlatformaticConsumerTransportOptions<TKey> &
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

export function createPlatformaticRuntimeConsumer<
  TKey = Buffer,
  TConsumer extends PlatformaticConsumerLike<TKey> = PlatformaticConsumerLike<TKey>
>(
  options: PlatformaticRuntimeConsumerOptions<TKey, TConsumer>
): PlatformaticRuntimeConsumer<TConsumer> {
  return toPlatformaticRuntimeConsumer<TKey, TConsumer>(
    options.consumer,
    createRuntimeConsumer({
      consumerTransport: createPlatformaticConsumerTransport(options.consumer, {
        ...(options.consumeOptions !== undefined
          ? { consumeOptions: options.consumeOptions }
          : {}),
        ...(options.onError !== undefined ? { onError: options.onError } : {})
      }),
      ...(options.schemaRegistry !== undefined
        ? { schemaRegistry: options.schemaRegistry }
        : { serialization: options.serialization })
    })
  );
}

export function toPlatformaticRuntimeConsumer<
  TKey = Buffer,
  TConsumer extends PlatformaticConsumerLike<TKey> = PlatformaticConsumerLike<TKey>
>(
  consumer: TConsumer,
  runtimeConsumer: RuntimeConsumer<PlatformaticConsumerSubscribeOptions<TKey>>
): PlatformaticRuntimeConsumer<TConsumer> {
  const nativeOn = (consumer as PlatformaticConsumerEventSource).on?.bind(consumer);

  return createPlatformaticClientProxy(consumer, {
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
          )) as PlatformaticRuntimeConsumer<TConsumer>['on'],
    onTopic: runtimeConsumer.onTopic.bind(runtimeConsumer)
  });
}
