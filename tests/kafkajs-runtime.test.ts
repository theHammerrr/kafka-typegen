import { EventEmitter } from 'node:events';
import { Buffer } from 'node:buffer';

import avsc from 'avsc';
import type { Schema } from 'avsc';
import { describe, expect, it, vi } from 'vitest';

import {
  createKafkaJsConsumerTransport,
  createKafkaJsProducerTransport
} from '../src/runtime/advanced.js';
import {
  createKafkaJsRuntimeConsumer,
  createKafkaJsRuntimeProducer
} from '../src/runtime/kafkajs.js';
import type {
  RuntimeEventMetadata,
  RuntimeSerializationHooks
} from '../src/runtime/index.js';
import {
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
const userCreatedAvroType = Type.forSchema(userCreatedSchema as Schema);

const userCreatedMetadata = {
  eventName: 'user.created',
  payloadTypeName: 'UserCreatedPayload',
  schemaFilePath: 'user-created.avsc',
  schemaName: 'UserCreated',
  subjectName: 'user.events-user.created',
  topicName: 'user.events'
} satisfies RuntimeEventMetadata;

function createSerialization(): RuntimeSerializationHooks {
  return {
    async deserialize(_metadata, message) {
      return JSON.parse(Buffer.from(message.value).toString('utf8'));
    },
    async serialize(_metadata, payload) {
      return {
        headers: {
          'x-kafka-typegen-event': 'user.created'
        },
        key: 'message-key',
        value: Buffer.from(JSON.stringify(payload))
      };
    }
  };
}

class MockKafkaJsProducer {
  public readonly connect = vi.fn(async () => {});
  public readonly disconnect = vi.fn(async () => {});
  public readonly send = vi.fn(async (_record: unknown) => []);
}

class MockKafkaJsConsumer extends EventEmitter {
  public readonly connect = vi.fn(async () => {});
  public readonly disconnect = vi.fn(async () => {});
  public readonly events = {
    CONNECT: 'consumer.connect',
    CRASH: 'consumer.crash'
  } as const;
  public runConfig: {
    autoCommit?: boolean;
    eachMessage?: (payload: {
      message: {
        headers?: Record<string, string | Buffer | Array<string | Buffer>>;
        key?: Buffer | string | null;
        offset: string;
        timestamp: string;
        value: Buffer | string | null;
      };
      partition: number;
      topic: string;
    }) => Promise<void>;
  } | undefined;
  public readonly run = vi.fn(async (config = {}) => {
    this.runConfig = config;
  });
  public readonly stop = vi.fn(async () => {});
  public readonly subscribe = vi.fn(async (_subscription: unknown) => {});
}

describe('KafkaJS runtime adapter', () => {
  it('produces runtime messages through KafkaJS producer.send', async () => {
    const producer = new MockKafkaJsProducer();
    const transport = createKafkaJsProducerTransport(producer);

    await transport.send(
      {
        headers: {
          'x-kafka-typegen-event': 'user.created'
        },
        key: 'message-key',
        topicName: 'user.events',
        value: Buffer.from('payload')
      },
      {
        acks: -1
      }
    );

    expect(producer.send).toHaveBeenCalledWith({
      acks: -1,
      messages: [
        {
          headers: {
            'x-kafka-typegen-event': 'user.created'
          },
          key: 'message-key',
          value: Buffer.from('payload')
        }
      ],
      topic: 'user.events'
    });
  });

  it('subscribes before run, dispatches messages, and forwards KafkaJS options', async () => {
    const consumer = new MockKafkaJsConsumer();
    const transport = createKafkaJsConsumerTransport(consumer, {
      runOptions: {
        autoCommit: false
      }
    });
    const handler = vi.fn(async () => {});

    await transport.onTopic('user.events', handler, {
      fromBeginning: true
    });
    await transport.run({
      autoCommit: true
    });
    await consumer.runConfig?.eachMessage?.({
      message: {
        headers: {
          'x-kafka-typegen-event': Buffer.from('user.created')
        },
        key: Buffer.from('message-key'),
        offset: '7',
        timestamp: '1700000000000',
        value: Buffer.from('payload')
      },
      partition: 2,
      topic: 'user.events'
    });

    expect(consumer.subscribe).toHaveBeenCalledWith({
      fromBeginning: true,
      topic: 'user.events'
    });
    expect(consumer.run).toHaveBeenCalledWith({
      autoCommit: true,
      eachMessage: expect.any(Function)
    });
    expect(handler).toHaveBeenCalledWith({
      headers: {
        'x-kafka-typegen-event': 'user.created'
      },
      key: Buffer.from('message-key'),
      offset: '7',
      partition: 2,
      timestamp: '1700000000000',
      topicName: 'user.events',
      value: Buffer.from('payload')
    });
  });

  it('rejects conflicting duplicate subscriptions and subscriptions after run starts', async () => {
    const consumer = new MockKafkaJsConsumer();
    const transport = createKafkaJsConsumerTransport(consumer);

    await transport.onTopic('user.events', async () => {}, {
      fromBeginning: true
    });
    await expect(
      transport.onTopic('user.events', async () => {}, {
        fromBeginning: false
      })
    ).rejects.toThrow(
      "Topic 'user.events' is already subscribed with different consume options."
    );

    await transport.run();
    await expect(
      transport.onTopic('order.events', async () => {}, {
        fromBeginning: true
      })
    ).rejects.toThrow(
      "Topic 'order.events' cannot be subscribed after the KafkaJS consumer has started."
    );
  });

  it('sends KafkaJS crash events and handler failures to onError', async () => {
    const consumer = new MockKafkaJsConsumer();
    const onError = vi.fn();
    const transport = createKafkaJsConsumerTransport(consumer, {
      onError
    });

    await transport.onTopic('user.events', async () => {
      throw new Error('handler failed');
    });
    await transport.run();

    consumer.emit('consumer.crash', {
      payload: {
        error: new Error('broker failed')
      }
    });
    await consumer.runConfig?.eachMessage?.({
      message: {
        offset: '8',
        timestamp: '1700000000001',
        value: Buffer.from('payload')
      },
      partition: 0,
      topic: 'user.events'
    });

    expect(onError).toHaveBeenCalledTimes(2);
    expect(onError).toHaveBeenNthCalledWith(1, expect.objectContaining({
      message: 'broker failed'
    }));
    expect(onError).toHaveBeenNthCalledWith(2, expect.objectContaining({
      message: 'handler failed'
    }));
  });

  it('wraps KafkaJS producer with typed runtime send and preserves native methods', async () => {
    const producer = new MockKafkaJsProducer();
    const runtimeProducer = createKafkaJsRuntimeProducer({
      producer,
      serialization: createSerialization()
    });

    await runtimeProducer.connect();
    await runtimeProducer.send(userCreatedMetadata, {
      email: 'ada@example.com',
      id: 'user_1',
      isAdmin: true
    }, {
      acks: -1
    });

    expect(producer.connect).toHaveBeenCalledTimes(1);
    expect(producer.send).toHaveBeenCalledWith({
      acks: -1,
      messages: [
        {
          headers: {
            'x-kafka-typegen-event': 'user.created'
          },
          key: 'message-key',
          value: Buffer.from(JSON.stringify({
            email: 'ada@example.com',
            id: 'user_1',
            isAdmin: true
          }))
        }
      ],
      topic: 'user.events'
    });
  });

  it('wraps KafkaJS consumer with typed runtime handlers and native run/stop', async () => {
    const consumer = new MockKafkaJsConsumer();
    const runtimeConsumer = createKafkaJsRuntimeConsumer({
      consumer,
      schemaRegistry: {
        async getLatestSchema(subjectName) {
          return {
            schema: JSON.stringify(userCreatedSchema),
            schemaId: 11,
            subjectName
          };
        },
        async getSchemaById(schemaId) {
          return {
            schema: JSON.stringify(userCreatedSchema),
            schemaId
          };
        }
      }
    });
    const handler = vi.fn(async () => {});

    await runtimeConsumer.connect();
    await runtimeConsumer.on(userCreatedMetadata, handler, {
      fromBeginning: true
    });
    await runtimeConsumer.run({
      autoCommit: false
    });
    await consumer.runConfig?.eachMessage?.({
      message: {
        headers: {
          'x-kafka-typegen-event': 'user.created'
        },
        offset: '12',
        timestamp: '1700000000002',
        value: Buffer.from(
          encodeSchemaRegistryWireFormat(
            11,
            userCreatedAvroType.toBuffer({
              email: 'ada@example.com',
              id: 'user_1',
              isAdmin: true
            })
          )
        )
      },
      partition: 1,
      topic: 'user.events'
    });
    await runtimeConsumer.stop();

    expect(consumer.connect).toHaveBeenCalledTimes(1);
    expect(consumer.subscribe).toHaveBeenCalledWith({
      fromBeginning: true,
      topic: 'user.events'
    });
    expect(consumer.run).toHaveBeenCalledWith({
      autoCommit: false,
      eachMessage: expect.any(Function)
    });
    expect(consumer.stop).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith({
      event: 'user.created',
      eventName: 'user.created',
      headers: {
        'x-kafka-typegen-event': 'user.created'
      },
      offset: '12',
      partition: 1,
      payload: {
        email: 'ada@example.com',
        id: 'user_1',
        isAdmin: true
      },
      timestamp: '1700000000002',
      topic: 'user.events',
      topicName: 'user.events'
    });
  });

  it('closes KafkaJS consumers by stopping consumption and disconnecting the native client', async () => {
    const consumer = new MockKafkaJsConsumer();
    const runtimeConsumer = createKafkaJsRuntimeConsumer({
      consumer,
      serialization: createSerialization()
    });

    await runtimeConsumer.on(userCreatedMetadata, async () => {}, {
      fromBeginning: true
    });
    await runtimeConsumer.run();
    await runtimeConsumer.close();

    expect(consumer.stop).toHaveBeenCalledTimes(1);
    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
  });
});
