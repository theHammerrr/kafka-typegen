import type {
  Consumer,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  EachMessagePayload,
  Producer,
  ProducerRecord
} from 'kafkajs';

import type { RuntimeSerializationOptions } from './types.js';

export type KafkaJsProducerLike = Pick<Producer, 'send'>;
export type KafkaJsConsumerLike = Pick<Consumer, 'run' | 'stop' | 'subscribe'>;
export type KafkaJsMessage = EachMessagePayload;

export type KafkaJsProducerSendOptions = Omit<ProducerRecord, 'messages' | 'topic'>;
export type KafkaJsConsumerRunOptions = Omit<
  ConsumerRunConfig,
  'eachBatch' | 'eachMessage'
>;
export type KafkaJsConsumerSubscribeOptions = Omit<ConsumerSubscribeTopic, 'topic'>;

export interface KafkaJsConsumerTransportOptions {
  readonly onError?: (error: unknown) => void;
  readonly runOptions?: KafkaJsConsumerRunOptions;
}

export type KafkaJsRuntimeClientOptions<
  TProducer extends KafkaJsProducerLike = KafkaJsProducerLike,
  TConsumer extends KafkaJsConsumerLike = KafkaJsConsumerLike
> = KafkaJsConsumerTransportOptions &
  RuntimeSerializationOptions & {
    readonly consumer: TConsumer;
    readonly producer: TProducer;
  };
