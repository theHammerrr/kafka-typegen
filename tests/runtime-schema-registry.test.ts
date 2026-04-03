import { Buffer } from 'node:buffer';

import avsc from 'avsc';
import type { Schema } from 'avsc';
import { describe, expect, it, vi } from 'vitest';

import {
  createConfluentSchemaRegistryRuntimeClient,
  createRuntimeClient,
  createRuntimeConsumer,
  createRuntimeProducer,
  type RuntimeClientOptions,
  type RuntimeEventMetadata,
  type RuntimeIncomingMessage,
  type RuntimeSerializationHooks,
  type SchemaRegistryRuntimeClient
} from '../src/index.js';
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

const userCreatedMetadata: RuntimeEventMetadata = {
  eventName: 'user.created',
  payloadTypeName: 'UserCreatedPayload',
  schemaFilePath: 'user-created.avsc',
  schemaName: 'UserCreated',
  subjectName: 'user.events-user.created',
  topicName: 'user.events'
};

function createSchemaRegistryMock(): {
  readonly registry: SchemaRegistryRuntimeClient;
  readonly getLatestSchema: ReturnType<typeof vi.fn>;
  readonly getSchemaById: ReturnType<typeof vi.fn>;
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

describe('runtime schema registry support', () => {
  it('encodes producer messages with the schema registry subject and caches subject lookups', async () => {
    const sentMessages: RuntimeIncomingMessage[] = [];
    const { getLatestSchema, registry } = createSchemaRegistryMock();
    const producer = createRuntimeProducer({
      producerTransport: {
        async send(message) {
          sentMessages.push(message as RuntimeIncomingMessage);
        }
      },
      schemaRegistry: registry
    });

    await producer.send(userCreatedMetadata, {
      email: 'ada@example.com',
      id: 'user_1',
      isAdmin: true
    });
    await producer.send(userCreatedMetadata, {
      email: 'grace@example.com',
      id: 'user_2',
      isAdmin: false
    });

    expect(getLatestSchema).toHaveBeenCalledTimes(1);
    expect(getLatestSchema).toHaveBeenCalledWith('user.events-user.created');
    expect(sentMessages).toHaveLength(2);
    expect(sentMessages[0]?.headers).toEqual({
      'x-kafka-typegen-event': 'user.created'
    });
    expect(sentMessages[0]?.schemaId).toBe(7);

    const decodedPayload = decodeSchemaRegistryWireFormat(sentMessages[0]!.value);
    expect(decodedPayload.schemaId).toBe(7);
    expect(userCreatedAvroType.fromBuffer(Buffer.from(decodedPayload.payload))).toEqual({
      email: 'ada@example.com',
      id: 'user_1',
      isAdmin: true
    });
  });

  it('decodes consumer messages via schema registry and caches schema-by-id lookups', async () => {
    const { getSchemaById, registry } = createSchemaRegistryMock();
    let registeredHandler:
      | ((message: RuntimeIncomingMessage) => Promise<void> | void)
      | undefined;

    const consumer = createRuntimeConsumer({
      consumerTransport: {
        async onTopic(_topicName, handler) {
          registeredHandler = handler;
        }
      },
      schemaRegistry: registry
    });
    const receivedMessages: unknown[] = [];

    await consumer.on(userCreatedMetadata, async (message) => {
      receivedMessages.push(message);
    });

    const encodedPayload = userCreatedAvroType.toBuffer({
      email: 'ada@example.com',
      id: 'user_1',
      isAdmin: true
    });
    const registryPayload = encodeSchemaRegistryWireFormat(7, encodedPayload);

    await registeredHandler?.({
      headers: {
        'x-kafka-typegen-event': 'user.created'
      },
      topicName: 'user.events',
      value: registryPayload
    });
    await registeredHandler?.({
      headers: {
        'x-kafka-typegen-event': 'user.created'
      },
      topicName: 'user.events',
      value: registryPayload
    });

    expect(getSchemaById).toHaveBeenCalledTimes(1);
    expect(getSchemaById).toHaveBeenCalledWith(7);
    expect(receivedMessages).toEqual([
      expect.objectContaining({
        event: 'user.created',
        payload: {
          email: 'ada@example.com',
          id: 'user_1',
          isAdmin: true
        },
        topic: 'user.events'
      }),
      expect.objectContaining({
        event: 'user.created',
        payload: {
          email: 'ada@example.com',
          id: 'user_1',
          isAdmin: true
        },
        topic: 'user.events'
      })
    ]);
  });

  it('supports the full generic runtime client with schema registry', async () => {
    const { registry } = createSchemaRegistryMock();
    let registeredHandler:
      | ((message: RuntimeIncomingMessage) => Promise<void> | void)
      | undefined;
    const sentMessages: RuntimeIncomingMessage[] = [];

    const runtime = createRuntimeClient({
      consumerTransport: {
        async onTopic(_topicName, handler) {
          registeredHandler = handler;
        }
      },
      producerTransport: {
        async send(message) {
          sentMessages.push(message as RuntimeIncomingMessage);
        }
      },
      schemaRegistry: registry
    });

    await runtime.producer.send(userCreatedMetadata, {
      email: 'ada@example.com',
      id: 'user_1',
      isAdmin: true
    });

    const encodedPayload = userCreatedAvroType.toBuffer({
      email: 'grace@example.com',
      id: 'user_2',
      isAdmin: false
    });
    const handledMessages: unknown[] = [];

    await runtime.consumer.on(userCreatedMetadata, async (message) => {
      handledMessages.push(message);
    });
    await registeredHandler?.({
      headers: {
        'x-kafka-typegen-event': 'user.created'
      },
      topicName: 'user.events',
      value: encodeSchemaRegistryWireFormat(7, encodedPayload)
    });

    expect(sentMessages[0]?.schemaId).toBe(7);
    expect(handledMessages).toEqual([
      expect.objectContaining({
        payload: {
          email: 'grace@example.com',
          id: 'user_2',
          isAdmin: false
        }
      })
    ]);
  });

  it('creates a runtime schema registry client from Confluent HTTP endpoints', async () => {
    const fetchMock = vi.fn(async (input: string | URL) => {
      const url = String(input);

      if (url.endsWith('/subjects/user.events-user.created/versions/latest')) {
        return new Response(
          JSON.stringify({
            id: 7,
            schema: userCreatedSchemaText,
            subject: 'user.events-user.created'
          }),
          {
            headers: { 'Content-Type': 'application/json' },
            status: 200
          }
        );
      }

      if (url.endsWith('/schemas/ids/7')) {
        return new Response(
          JSON.stringify({
            schema: userCreatedSchemaText
          }),
          {
            headers: { 'Content-Type': 'application/json' },
            status: 200
          }
        );
      }

      return new Response('Not found', { status: 404 });
    });

    vi.stubGlobal('fetch', fetchMock);

    try {
      const registry = createConfluentSchemaRegistryRuntimeClient({
        auth: {
          password: 'secret',
          username: 'demo'
        },
        url: 'http://localhost:8081/'
      });

      await expect(registry.getLatestSchema('user.events-user.created')).resolves.toEqual({
        schema: userCreatedSchemaText,
        schemaId: 7,
        subjectName: 'user.events-user.created'
      });
      await expect(registry.getSchemaById(7)).resolves.toEqual({
        schema: userCreatedSchemaText,
        schemaId: 7
      });

      expect(fetchMock).toHaveBeenNthCalledWith(
        1,
        'http://localhost:8081/subjects/user.events-user.created/versions/latest',
        {
          headers: {
            Accept: 'application/json',
            Authorization: `Basic ${Buffer.from('demo:secret').toString('base64')}`
          }
        }
      );
      expect(fetchMock).toHaveBeenNthCalledWith(2, 'http://localhost:8081/schemas/ids/7', {
        headers: {
          Accept: 'application/json',
          Authorization: `Basic ${Buffer.from('demo:secret').toString('base64')}`
        }
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('accepts direct Confluent Schema Registry options in runtime helpers', async () => {
    const sentMessages: RuntimeIncomingMessage[] = [];
    const fetchMock = vi.fn(async (input: string | URL) => {
      const url = String(input);

      if (url.endsWith('/subjects/user.events-user.created/versions/latest')) {
        return new Response(
          JSON.stringify({
            id: 7,
            schema: userCreatedSchemaText
          }),
          {
            headers: { 'Content-Type': 'application/json' },
            status: 200
          }
        );
      }

      if (url.endsWith('/schemas/ids/7')) {
        return new Response(
          JSON.stringify({
            schema: userCreatedSchemaText
          }),
          {
            headers: { 'Content-Type': 'application/json' },
            status: 200
          }
        );
      }

      return new Response('Not found', { status: 404 });
    });

    vi.stubGlobal('fetch', fetchMock);

    try {
      const producer = createRuntimeProducer({
        producerTransport: {
          async send(message) {
            sentMessages.push(message as RuntimeIncomingMessage);
          }
        },
        schemaRegistry: {
          url: 'http://localhost:8081'
        }
      });

      await producer.send(userCreatedMetadata, {
        email: 'ada@example.com',
        id: 'user_1',
        isAdmin: true
      });

      expect(fetchMock).toHaveBeenCalledWith(
        'http://localhost:8081/subjects/user.events-user.created/versions/latest',
        {
          headers: {
            Accept: 'application/json'
          }
        }
      );
      expect(sentMessages[0]?.schemaId).toBe(7);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('rejects ambiguous or missing runtime serialization configuration', async () => {
    const { registry } = createSchemaRegistryMock();
    const serialization: RuntimeSerializationHooks = {
      async deserialize<TPayload>() {
        return {} as TPayload;
      },
      async serialize() {
        return {
          value: new Uint8Array()
        };
      }
    };

    expect(() =>
      createRuntimeClient({
        consumerTransport: {
          async onTopic() {}
        },
        producerTransport: {
          async send() {}
        },
        schemaRegistry: registry,
        serialization
      } as unknown as RuntimeClientOptions)
    ).toThrowError("Runtime helpers require exactly one of 'serialization' or 'schemaRegistry'.");

    expect(() =>
      createRuntimeClient({
        consumerTransport: {
          async onTopic() {}
        },
        producerTransport: {
          async send() {}
        }
      } as never)
    ).toThrowError("Runtime helpers require exactly one of 'serialization' or 'schemaRegistry'.");
  });
});
