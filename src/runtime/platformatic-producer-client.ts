import { createPlatformaticClientProxy } from './platformatic-client-proxy.js';
import { createRuntimeProducer } from './producer-client.js';
import { createPlatformaticProducerTransport } from './platformatic-producer.js';
import type {
  PlatformaticProducerLike,
  PlatformaticProducerSendOptions
} from './platformatic-types.js';
import type {
  RuntimeEventMetadata,
  RuntimeProducer,
  RuntimeSerializationOptions
} from './types.js';

type PlatformaticSendDelegate = (
  message: unknown,
  callback?: unknown
) => Promise<unknown> | void;

export type PlatformaticRuntimeProducer<TProducer = PlatformaticProducerLike> = TProducer &
  RuntimeProducer<
    TProducer extends PlatformaticProducerLike<infer TKey>
      ? PlatformaticProducerSendOptions<TKey>
      : never
  >;

export type PlatformaticRuntimeProducerOptions<
  TKey = unknown,
  TProducer extends PlatformaticProducerLike<TKey> = PlatformaticProducerLike<TKey>
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

export function createPlatformaticRuntimeProducer<
  TKey = Buffer,
  TProducer extends PlatformaticProducerLike<TKey> = PlatformaticProducerLike<TKey>
>(
  options: PlatformaticRuntimeProducerOptions<TKey, TProducer>
): PlatformaticRuntimeProducer<TProducer> {
  return toPlatformaticRuntimeProducer<TKey, TProducer>(
    options.producer,
    createRuntimeProducer({
      producerTransport: createPlatformaticProducerTransport(options.producer),
      ...(options.schemaRegistry !== undefined
        ? { schemaRegistry: options.schemaRegistry }
        : { serialization: options.serialization })
    })
  );
}

export function toPlatformaticRuntimeProducer<
  TKey = Buffer,
  TProducer extends PlatformaticProducerLike<TKey> = PlatformaticProducerLike<TKey>
>(
  producer: TProducer,
  runtimeProducer: RuntimeProducer<PlatformaticProducerSendOptions<TKey>>
): PlatformaticRuntimeProducer<TProducer> {
  const send = producer.send.bind(producer) as PlatformaticSendDelegate;

  return createPlatformaticClientProxy(producer, {
    send: ((
      messageOrMetadata: unknown,
      payloadOrCallback?: unknown,
      sendOptions?: unknown
    ) =>
      isRuntimeEventMetadata(messageOrMetadata)
        ? runtimeProducer.send(
            messageOrMetadata,
            payloadOrCallback,
            sendOptions as never
          )
        : send(messageOrMetadata, payloadOrCallback)
    ) as PlatformaticRuntimeProducer<TProducer>['send']
  });
}
