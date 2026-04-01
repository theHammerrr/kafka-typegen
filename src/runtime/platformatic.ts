import { Buffer } from 'node:buffer';

import type {
  ConsumeOptions,
  Consumer,
  Message,
  MessagesStream,
  Producer
} from '@platformatic/kafka';

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
import type {
  RuntimeClient,
  RuntimeClientOptions,
  RuntimeIncomingMessage,
  RuntimeSerializationHooks,
  RuntimeTransportConsumer,
  RuntimeTransportProducer
} from './types.js';

type PlatformaticProducerLike<TKey = unknown> = Pick<Producer<TKey, Buffer, string, Buffer>, 'send'>;
type PlatformaticConsumerLike<TKey = unknown> = Pick<Consumer<TKey, Buffer, Buffer, Buffer>, 'consume'>;
type PlatformaticMessage<TKey = unknown> = Message<TKey, Buffer, Buffer, Buffer> & {
  readonly schemaId?: string | number;
};
type PlatformaticMessagesStream<TKey = unknown> = Pick<MessagesStream<TKey, Buffer, Buffer, Buffer>, 'on'>;

export interface PlatformaticConsumerTransportOptions<TKey = unknown> {
  readonly consumeOptions?: Omit<ConsumeOptions<TKey, Buffer, Buffer, Buffer>, 'topics'>;
}

export interface PlatformaticRuntimeClientOptions<TKey = unknown>
  extends PlatformaticConsumerTransportOptions<TKey> {
  readonly consumer: PlatformaticConsumerLike<TKey>;
  readonly producer: PlatformaticProducerLike<TKey>;
  readonly serialization: RuntimeSerializationHooks;
}

function toPlatformaticHeaders(
  headers?: Readonly<Record<string, string>>
): Record<string, Buffer> | undefined {
  if (headers === undefined) {
    return undefined;
  }

  return Object.fromEntries(Object.entries(headers).map(([key, value]) => [key, Buffer.from(value)]));
}

function toRuntimeHeaders(
  headers: Map<Buffer, Buffer> | Record<string, Buffer> | undefined
): Readonly<Record<string, string>> | undefined {
  if (headers === undefined) {
    return undefined;
  }

  if (headers instanceof Map) {
    return Object.fromEntries(
      Array.from(headers.entries()).map(([key, value]) => [key.toString('utf8'), value.toString('utf8')])
    );
  }

  return Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [key, value.toString('utf8')])
  );
}

function toRuntimeIncomingMessage<TKey>(message: PlatformaticMessage<TKey>): RuntimeIncomingMessage {
  const headers = message.headers.size > 0 ? toRuntimeHeaders(message.headers) : undefined;

  return {
    topicName: message.topic,
    value: message.value,
    ...(headers !== undefined ? { headers } : {}),
    ...(message.key !== undefined ? { key: message.key } : {}),
    ...(message.offset !== undefined ? { offset: message.offset.toString() } : {}),
    ...(message.partition !== undefined ? { partition: message.partition } : {}),
    ...(message.schemaId !== undefined ? { schemaId: message.schemaId } : {}),
    ...(message.timestamp !== undefined ? { timestamp: message.timestamp.toString() } : {})
  };
}

export function createPlatformaticProducerTransport<TKey = unknown>(
  producer: PlatformaticProducerLike<TKey>
): RuntimeTransportProducer {
  return {
    async send(message) {
      const headers = toPlatformaticHeaders(message.headers);

      await producer.send({
        messages: [
          {
            topic: message.topicName,
            value: Buffer.from(message.value),
            ...(headers !== undefined ? { headers } : {}),
            ...(message.key !== undefined ? { key: message.key as TKey } : {})
          }
        ]
      });
    }
  };
}

export function createPlatformaticConsumerTransport<TKey = unknown>(
  consumer: PlatformaticConsumerLike<TKey>,
  options: PlatformaticConsumerTransportOptions<TKey> = {}
): RuntimeTransportConsumer {
  const handlersByTopic = new Map<
    string,
    Set<(message: RuntimeIncomingMessage) => Promise<void> | void>
  >();
  const streamsByTopic = new Map<string, PlatformaticMessagesStream<TKey>>();

  return {
    async onTopic(topicName, handler) {
      const existingHandlers = handlersByTopic.get(topicName);

      if (existingHandlers !== undefined) {
        existingHandlers.add(handler);
        return;
      }

      const topicHandlers = new Set<(message: RuntimeIncomingMessage) => Promise<void> | void>([handler]);
      handlersByTopic.set(topicName, topicHandlers);

      const stream = await consumer.consume({
        ...(options.consumeOptions ?? {}),
        topics: [topicName]
      });
      streamsByTopic.set(topicName, stream);

      stream.on('data', (message) => {
        const runtimeMessage = toRuntimeIncomingMessage(message as PlatformaticMessage<TKey>);

        for (const topicHandler of topicHandlers) {
          void Promise.resolve(topicHandler(runtimeMessage));
        }
      });

      stream.on('error', () => {});
    }
  };
}

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
