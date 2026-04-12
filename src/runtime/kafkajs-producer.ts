import { Buffer } from 'node:buffer';

import type {
  KafkaJsProducerLike,
  KafkaJsProducerSendOptions
} from './kafkajs-types.js';
import type { RuntimeTransportProducer } from './types.js';

export function createKafkaJsProducerTransport(
  producer: KafkaJsProducerLike
): RuntimeTransportProducer<KafkaJsProducerSendOptions> {
  return {
    async send(message, options) {
      await producer.send({
        ...(options ?? {}),
        messages: [
          {
            value: Buffer.from(message.value),
            ...(message.headers !== undefined
              ? { headers: message.headers }
              : {}),
            ...(message.key !== undefined
              ? { key: message.key as Buffer | string | null }
              : {})
          }
        ],
        topic: message.topicName
      });
    }
  };
}
