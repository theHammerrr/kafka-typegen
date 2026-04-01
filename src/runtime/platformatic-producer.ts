import { Buffer } from 'node:buffer';

import type { RuntimeTransportProducer } from './types.js';
import { toPlatformaticHeaders } from './platformatic-headers.js';
import type { PlatformaticProducerLike } from './platformatic-types.js';

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
