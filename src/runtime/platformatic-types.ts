import { Buffer } from 'node:buffer';

import type {
  ConsumeOptions,
  Consumer,
  Message,
  MessagesStream,
  Producer,
  SendOptions
} from '@platformatic/kafka';

import type { RuntimeSerializationOptions } from './types.js';

export type PlatformaticProducerLike<TKey = Buffer> = Pick<
  Producer<TKey, Buffer, Buffer, Buffer>,
  'send'
>;
export type PlatformaticConsumerLike<TKey = Buffer> = Pick<
  Consumer<TKey, Buffer, Buffer, Buffer>,
  'consume'
> &
  Partial<Pick<Consumer<TKey, Buffer, Buffer, Buffer>, 'close'>>;
export type PlatformaticMessage<TKey = unknown> = Message<TKey, Buffer, Buffer, Buffer> & {
  readonly schemaId?: string | number;
};
export type PlatformaticMessagesStream<TKey = unknown> = Pick<
  MessagesStream<TKey, Buffer, Buffer, Buffer>,
  'close' | 'on'
>;

export type PlatformaticProducerSendOptions<TKey = Buffer> = Omit<
  SendOptions<TKey, Buffer, Buffer, Buffer>,
  'messages'
>;

export type PlatformaticConsumerSubscribeOptions<TKey = Buffer> = Omit<
  ConsumeOptions<TKey, Buffer, Buffer, Buffer>,
  'topics'
>;

export interface PlatformaticConsumerTransportOptions<TKey = Buffer> {
  readonly consumeOptions?: PlatformaticConsumerSubscribeOptions<TKey>;
  readonly onError?: (error: unknown) => void;
}

export type PlatformaticRuntimeClientOptions<
  TKey = Buffer,
  TProducer extends PlatformaticProducerLike<TKey> = PlatformaticProducerLike<TKey>,
  TConsumer extends PlatformaticConsumerLike<TKey> = PlatformaticConsumerLike<TKey>
> = PlatformaticConsumerTransportOptions<TKey> &
  RuntimeSerializationOptions & {
  readonly consumer: TConsumer;
  readonly producer: TProducer;
};
