import { EventEmitter } from 'node:events';
import { Buffer } from 'node:buffer';

import avsc from 'avsc';
import type { Schema } from 'avsc';
import { describe, expect, it, vi } from 'vitest';

import {
  createPlatformaticConsumerTransport,
  createPlatformaticProducerTransport,
  createPlatformaticRuntimeClient,
  createPlatformaticRuntimeConsumer,
  createPlatformaticRuntimeProducer,
  type RuntimeIncomingMessage,
  type RuntimeSerializationHooks,
  type SchemaRegistryRuntimeClient
} from '../src/runtime/platformatic.js';
import {
  decodeSchemaRegistryWireFormat,
  encodeSchemaRegistryWireFormat
} from '../src/runtime/schema-registry-wire-format.js';

const { Type } = avsc;

const userCreatedSchema = {
  fields: [
    { name: 'id', type: 'string' },
    { name: 'email', type: 'string' },
    { name: 'isAdmin', type: 'boolean' }
  ],
  name: 'UserCreated',
  type: 'record'
};
const userCreatedSchemaText = JSON.stringify(userCreatedSchema);
const userCreatedAvroType = Type.forSchema(userCreatedSchema as Schema);

function createSchemaRegistryMock(): {
  readonly getLatestSchema: ReturnType<typeof vi.fn>;
  readonly getSchemaById: ReturnType<typeof vi.fn>;
  readonly registry: SchemaRegistryRuntimeClient;
} {
  const getLatestSchema = vi.fn(async (subjectName: string) => ({
    schema: userCreatedSchemaText,
    schemaId: 7,
    subjectName
  }));
  const getSchemaById = vi.fn(async (schemaId: number) => ({
    schema: userCreatedSchemaText,
    schemaId
  }));

  return {
    getLatestSchema,
    getSchemaById,
    registry: {
      getLatestSchema,
      getSchemaById
    }
  };
}

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

  it('supports schema registry serialization through the platformatic runtime client', async () => {
    const stream = new MockMessagesStream();
    const send = vi.fn().mockResolvedValue(undefined);
    const consume = vi.fn().mockResolvedValue(stream);
    const { getLatestSchema, getSchemaById, registry } = createSchemaRegistryMock();
    const client = createPlatformaticRuntimeClient({
      consumer: {
        consume
      },
      producer: {
        send
      },
      schemaRegistry: registry
    });
    const handledMessages: unknown[] = [];

    await client.consumer.on(
      {
        eventName: 'user.created',
        payloadTypeName: 'UserCreatedPayload',
        schemaFilePath: 'user-created.avsc',
        schemaName: 'UserCreated',
        subjectName: 'user.events-user.created',
        topicName: 'user.events'
      },
      async (message) => {
        handledMessages.push(message);
      }
    );
    await client.producer.send(
      {
        eventName: 'user.created',
        payloadTypeName: 'UserCreatedPayload',
        schemaFilePath: 'user-created.avsc',
        schemaName: 'UserCreated',
        subjectName: 'user.events-user.created',
        topicName: 'user.events'
      },
      {
        email: 'ada@example.com',
        id: 'user_1',
        isAdmin: true
      }
    );

    const producerPayload = send.mock.calls[0]?.[0]?.messages?.[0]?.value as Buffer;
    stream.emitData({
      headers: new Map([[Buffer.from('x-kafka-typegen-event'), Buffer.from('user.created')]]),
      offset: 1n,
      partition: 0,
      timestamp: 2n,
      topic: 'user.events',
      value: producerPayload
    });
    await new Promise((resolve) => setImmediate(resolve));

    expect(getLatestSchema).toHaveBeenCalledTimes(1);
    expect(getSchemaById).toHaveBeenCalledTimes(0);
    expect(handledMessages).toEqual([
      expect.objectContaining({
        payload: {
          email: 'ada@example.com',
          id: 'user_1',
          isAdmin: true
        }
      })
    ]);
  });

  it('supports schema registry serialization through the platformatic producer-only helper', async () => {
    const send = vi.fn().mockResolvedValue(undefined);
    const { getLatestSchema, registry } = createSchemaRegistryMock();
    const producer = createPlatformaticRuntimeProducer({
      producer: {
        send
      },
      schemaRegistry: registry
    });

    await producer.send(
      {
        eventName: 'user.created',
        payloadTypeName: 'UserCreatedPayload',
        schemaFilePath: 'user-created.avsc',
        schemaName: 'UserCreated',
        subjectName: 'user.events-user.created',
        topicName: 'user.events'
      },
      {
        email: 'grace@example.com',
        id: 'user_2',
        isAdmin: false
      }
    );

    expect(getLatestSchema).toHaveBeenCalledTimes(1);
    const encodedPayload = send.mock.calls[0]?.[0]?.messages?.[0]?.value as Buffer;
    const decodedPayload = userCreatedAvroType.fromBuffer(
      Buffer.from(decodeSchemaRegistryWireFormat(encodedPayload).payload)
    );

    expect(decodedPayload).toEqual({
      email: 'grace@example.com',
      id: 'user_2',
      isAdmin: false
    });
  });

  it('supports schema registry deserialization through the platformatic consumer-only helper', async () => {
    const stream = new MockMessagesStream();
    const consume = vi.fn().mockResolvedValue(stream);
    const { getSchemaById, registry } = createSchemaRegistryMock();
    const consumer = createPlatformaticRuntimeConsumer({
      consumer: {
        consume
      },
      schemaRegistry: registry
    });
    const handledMessages: unknown[] = [];

    await consumer.on(
      {
        eventName: 'user.created',
        payloadTypeName: 'UserCreatedPayload',
        schemaFilePath: 'user-created.avsc',
        schemaName: 'UserCreated',
        subjectName: 'user.events-user.created',
        topicName: 'user.events'
      },
      async (message) => {
        handledMessages.push(message);
      }
    );

    stream.emitData({
      headers: new Map([[Buffer.from('x-kafka-typegen-event'), Buffer.from('user.created')]]),
      offset: 1n,
      partition: 0,
      timestamp: 2n,
      topic: 'user.events',
      value: Buffer.from(
        encodeSchemaRegistryWireFormat(
          7,
          userCreatedAvroType.toBuffer({
            email: 'ada@example.com',
            id: 'user_1',
            isAdmin: true
          })
        )
      )
    });
    await new Promise((resolve) => setImmediate(resolve));

    expect(getSchemaById).toHaveBeenCalledTimes(1);
    expect(handledMessages).toEqual([
      expect.objectContaining({
        payload: {
          email: 'ada@example.com',
          id: 'user_1',
          isAdmin: true
        }
      })
    ]);
  });

  it('preserves native producer and consumer methods on wrapped platformatic runtime clients', async () => {
    const close = vi.fn();
    const nativeSend = vi.fn().mockResolvedValue(undefined);
    const nativeOn = vi.fn().mockReturnValue('native-on-result');
    const stream = new MockMessagesStream();
    const runtimeClient = createPlatformaticRuntimeClient({
      consumer: {
        consume: vi.fn().mockResolvedValue(stream),
        on: nativeOn,
        topics: ['user.events']
      },
      producer: {
        close,
        send: nativeSend
      },
      serialization: {
        async deserialize(_metadata, message) {
          return JSON.parse(Buffer.from(message.value).toString('utf8'));
        },
        async serialize(_metadata, payload) {
          return {
            value: Buffer.from(JSON.stringify(payload))
          };
        }
      }
    });

    await runtimeClient.producer.send({
      messages: [
        {
          topic: 'user.events',
          value: Buffer.from('native')
        }
      ]
    });
    runtimeClient.producer.close();

    const nativeOnResult = runtimeClient.consumer.on('consumer:error', () => {});

    expect(nativeSend).toHaveBeenCalledWith({
      messages: [
        {
          topic: 'user.events',
          value: Buffer.from('native')
        }
      ]
    }, undefined);
    expect(close).toHaveBeenCalledTimes(1);
    expect(nativeOn).toHaveBeenCalledTimes(1);
    expect(nativeOn).toHaveBeenCalledWith('consumer:error', expect.any(Function));
    expect(nativeOnResult).toBe('native-on-result');
    expect(runtimeClient.consumer.topics).toEqual(['user.events']);
  });
});
