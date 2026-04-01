import { EventEmitter } from 'node:events';

import { describe, expect, it, vi } from 'vitest';

import {
  createPlatformaticConsumerTransport,
  createPlatformaticProducerTransport,
  createPlatformaticRuntimeClient,
  type RuntimeIncomingMessage,
  type RuntimeSerializationHooks
} from '../src/runtime/platformatic.js';

class MockMessagesStream<TKey = unknown> extends EventEmitter {
  public emitData(message: {
    headers?: Map<Buffer, Buffer>;
    key?: TKey;
    offset: bigint;
    partition: number;
    schemaId?: string | number;
    timestamp: bigint;
    topic: string;
    value: Buffer;
  }): void {
    this.emit('data', {
      headers: message.headers ?? new Map<Buffer, Buffer>(),
      ...message
    });
  }
}

describe('platformatic runtime adapter', () => {
  it('maps runtime outgoing messages into a platformatic producer send call', async () => {
    const send = vi.fn().mockResolvedValue(undefined);
    const transport = createPlatformaticProducerTransport({ send });

    await transport.send({
      headers: {
        'x-kafka-typegen-event': 'user.created'
      },
      key: Buffer.from('user-1'),
      topicName: 'user.events',
      value: new Uint8Array([1, 2, 3])
    });

    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith({
      messages: [
        {
          headers: {
            'x-kafka-typegen-event': Buffer.from('user.created')
          },
          key: Buffer.from('user-1'),
          topic: 'user.events',
          value: Buffer.from([1, 2, 3])
        }
      ]
    });
  });

  it('creates a single consume stream per topic and fans out runtime messages', async () => {
    const stream = new MockMessagesStream();
    const consume = vi.fn().mockResolvedValue(stream);
    const transport = createPlatformaticConsumerTransport(
      { consume },
      {
        consumeOptions: {
          autocommit: true
        }
      }
    );
    const handlerOne = vi.fn().mockResolvedValue(undefined);
    const handlerTwo = vi.fn().mockResolvedValue(undefined);

    await transport.onTopic('user.events', handlerOne);
    await transport.onTopic('user.events', handlerTwo);

    stream.emitData({
      headers: new Map([[Buffer.from('x-kafka-typegen-event'), Buffer.from('user.created')]]),
      key: Buffer.from('user-1'),
      offset: 5n,
      partition: 2,
      schemaId: 99,
      timestamp: 15n,
      topic: 'user.events',
      value: Buffer.from([4, 5, 6])
    });
    await Promise.resolve();

    expect(consume).toHaveBeenCalledTimes(1);
    expect(consume).toHaveBeenCalledWith({
      autocommit: true,
      topics: ['user.events']
    });
    expect(handlerOne).toHaveBeenCalledWith({
      headers: {
        'x-kafka-typegen-event': 'user.created'
      },
      key: Buffer.from('user-1'),
      offset: '5',
      partition: 2,
      schemaId: 99,
      timestamp: '15',
      topicName: 'user.events',
      value: Buffer.from([4, 5, 6])
    } satisfies RuntimeIncomingMessage);
    expect(handlerTwo).toHaveBeenCalledTimes(1);
  });

  it('ignores unknown events in topic-based runtime client handlers', async () => {
    const stream = new MockMessagesStream();
    const serialization: RuntimeSerializationHooks = {
      async deserialize(_metadata, message) {
        return JSON.parse(Buffer.from(message.value).toString('utf8'));
      },
      async serialize() {
        return {
          value: Buffer.from('unused')
        };
      }
    };
    const client = createPlatformaticRuntimeClient({
      consumer: {
        consume: vi.fn().mockResolvedValue(stream)
      },
      producer: {
        send: vi.fn().mockResolvedValue(undefined)
      },
      serialization
    });
    const handler = vi.fn().mockResolvedValue(undefined);

    await client.consumer.onTopic(
      'user.events',
      {
        'user.created': {
          eventName: 'user.created',
          payloadTypeName: 'UserCreatedPayload',
          schemaFilePath: 'user-created.avsc',
          schemaName: 'UserCreated',
          subjectName: 'user.events-user.created',
          topicName: 'user.events'
        }
      },
      handler
    );

    stream.emitData({
      headers: new Map([[Buffer.from('x-kafka-typegen-event'), Buffer.from('user.updated')]]),
      offset: 7n,
      partition: 0,
      timestamp: 30n,
      topic: 'user.events',
      value: Buffer.from(JSON.stringify({ id: '7' }))
    });
    await Promise.resolve();

    expect(handler).not.toHaveBeenCalled();
  });
});
