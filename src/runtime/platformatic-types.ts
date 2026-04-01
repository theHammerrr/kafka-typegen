import { Buffer } from 'node:buffer';

import type { ConsumeOptions, Consumer, Message, MessagesStream, Producer } from '@platformatic/kafka';

import type { RuntimeSerializationHooks } from './types.js';

export type PlatformaticProducerLike<TKey = unknown> = Pick<Producer<TKey, Buffer, string, Buffer>, 'send'>;
export type PlatformaticConsumerLike<TKey = unknown> = Pick<Consumer<TKey, Buffer, Buffer, Buffer>, 'consume'>;
export type PlatformaticMessage<TKey = unknown> = Message<TKey, Buffer, Buffer, Buffer> & {
  readonly schemaId?: string | number;
};
export type PlatformaticMessagesStream<TKey = unknown> = Pick<MessagesStream<TKey, Buffer, Buffer, Buffer>, 'on'>;

export interface PlatformaticConsumerTransportOptions<TKey = unknown> {
  readonly consumeOptions?: Omit<ConsumeOptions<TKey, Buffer, Buffer, Buffer>, 'topics'>;
}

export interface PlatformaticRuntimeClientOptions<TKey = unknown>
  extends PlatformaticConsumerTransportOptions<TKey> {
  readonly consumer: PlatformaticConsumerLike<TKey>;
  readonly producer: PlatformaticProducerLike<TKey>;
  readonly serialization: RuntimeSerializationHooks;
}
