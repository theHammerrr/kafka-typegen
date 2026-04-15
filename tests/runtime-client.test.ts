import ts from 'typescript';
import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createPlatformaticRuntimeProducer,
  createRuntimeConsumer,
  createRuntimeClient,
  createRuntimeProducer,
  createTypeGenerator,
  resolveConfig,
  type RuntimeIncomingMessage,
  type RuntimeSerializationHooks
} from '../src/index.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');

type GeneratedClientModule = {
  createClient: (runtime: ReturnType<typeof createRuntimeClient>) => {
    producer: {
      userEvents?: {
        userCreated?: {
          send: (payload: unknown, options?: unknown) => Promise<void>;
        };
        userProfiled?: {
          send: (payload: unknown, options?: unknown) => Promise<void>;
        };
      };
      userLifecycle?: {
        userUpdated?: {
          send: (payload: unknown, options?: unknown) => Promise<void>;
        };
      };
    };
    consumer: {
      userEvents?: {
        on?: (
          handler: (message: unknown) => Promise<void> | void,
          options?: unknown
        ) => Promise<void>;
        userCreated?: {
          on: (
            handler: (message: unknown) => Promise<void> | void,
            options?: unknown
          ) => Promise<void>;
        };
        userProfiled?: {
          on: (
            handler: (message: unknown) => Promise<void> | void,
            options?: unknown
          ) => Promise<void>;
        };
      };
      userLifecycle?: {
        userUpdated?: {
          on: (
            handler: (message: unknown) => Promise<void> | void,
            options?: unknown
          ) => Promise<void>;
        };
      };
    };
  };
  createConsumer: (
    runtimeConsumer: ReturnType<typeof createRuntimeConsumer>
  ) => {
    userEvents?: {
      on?: (
        handler: (message: unknown) => Promise<void> | void,
        options?: unknown
      ) => Promise<void>;
      userCreated: {
        on: (
          handler: (message: unknown) => Promise<void> | void,
          options?: unknown
        ) => Promise<void>;
      };
    };
    userLifecycle?: {
      userUpdated: {
        on: (
          handler: (message: unknown) => Promise<void> | void,
          options?: unknown
        ) => Promise<void>;
      };
    };
  };
  createProducer: (runtimeProducer: ReturnType<typeof createRuntimeProducer>) => {
    userEvents?: {
      userCreated: {
        send: (payload: unknown, options?: unknown) => Promise<void>;
      };
    };
    userLifecycle?: {
      userUpdated: {
        send: (payload: unknown, options?: unknown) => Promise<void>;
      };
    };
  };
};

class PrivateFieldProducer {
  #closed = false;

  public async send(): Promise<never> {
    throw new Error('Not used in private-field native method binding test');
  }

  public close(): void {
    this.#closed = true;
  }

  public isClosed(): boolean {
    return this.#closed;
  }
}

async function loadGeneratedClientModule(
  configInput: Parameters<typeof resolveConfig>[0]
): Promise<GeneratedClientModule> {
  const config = resolveConfig(configInput);
  const catalog = await createCatalogBuilder().build(config);
  const output = await createTypeGenerator().generate(catalog);
  const transpiled = ts.transpileModule(output.files[0]?.contents ?? '', {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022
    }
  }).outputText;

  const module = { exports: {} as GeneratedClientModule };
  const evaluator = new Function('exports', 'module', transpiled);

  evaluator(module.exports, module);

  return module.exports;
}

describe('runtime client integration', () => {
  it('serializes producer payloads and routes them through the producer transport', async () => {
    const serializationCalls: unknown[] = [];
    const outgoingMessages: unknown[] = [];

    const serialization: RuntimeSerializationHooks = {
      async deserialize() {
        throw new Error('Not used in producer test');
      },
      async serialize(metadata, payload) {
        serializationCalls.push({ metadata, payload });

        return {
          headers: { 'content-type': 'application/json' },
          schemaId: 17,
          value: new TextEncoder().encode(JSON.stringify(payload))
        };
      }
    };

    const runtime = createRuntimeClient({
      consumerTransport: {
        async onTopic() {}
      },
      producerTransport: {
        async send(message) {
          outgoingMessages.push(message);
        }
      },
      serialization
    });

    await runtime.producer.send(
      {
        eventName: 'user.created',
        payloadTypeName: 'UserCreatedPayload',
        schemaFilePath: 'user-created.avsc',
        schemaName: 'UserCreated',
        subjectName: 'user.events-user.created',
        topicName: 'user.events'
      },
      { id: '1' }
    );

    expect(serializationCalls).toHaveLength(1);
    expect(outgoingMessages).toEqual([
      expect.objectContaining({
        headers: expect.objectContaining({
          'content-type': 'application/json',
          'x-kafka-typegen-event': 'user.created'
        }),
        schemaId: 17,
        topicName: 'user.events'
      })
    ]);
  });

  it('wires generated client producer and consumer through the runtime integration layer', async () => {
    const registeredTopicHandlers = new Map<string, (message: RuntimeIncomingMessage) => Promise<void> | void>();
    const serialization: RuntimeSerializationHooks = {
      async deserialize(_metadata, message) {
        return JSON.parse(new TextDecoder().decode(message.value));
      },
      async serialize(_metadata, payload) {
        return {
          value: new TextEncoder().encode(JSON.stringify(payload))
        };
      }
    };

    const sentMessages: unknown[] = [];
    const runtime = createRuntimeClient({
      consumerTransport: {
        async onTopic(topicName, handler) {
          registeredTopicHandlers.set(topicName, handler);
        }
      },
      producerTransport: {
        async send(message) {
          sentMessages.push(message);
        }
      },
      serialization
    });

    const generatedModule = await loadGeneratedClientModule({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.updated',
              schemaPath: './user-updated.avsc'
            }
          ],
          name: 'user.lifecycle'
        },
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const client = generatedModule.createClient(runtime);
    const receivedMessages: unknown[] = [];

    await client.consumer.userLifecycle?.userUpdated?.on(async (message) => {
      receivedMessages.push(message);
    });

    await client.producer.userLifecycle?.userUpdated?.send({
      displayName: 'Ada',
      id: '42',
      metadata: { role: 'admin' }
    });

    const topicHandler = registeredTopicHandlers.get('user.lifecycle');

    expect(sentMessages).toHaveLength(1);
    expect(topicHandler).toBeDefined();

    await topicHandler?.({
      headers: {
        'x-kafka-typegen-event': 'user.updated'
      },
      topicName: 'user.lifecycle',
      value: new TextEncoder().encode(
        JSON.stringify({
          displayName: 'Ada',
          id: '42',
          metadata: { role: 'admin' }
        })
      )
    });

    expect(receivedMessages).toEqual([
      expect.objectContaining({
        eventName: 'user.updated',
        payload: {
          displayName: 'Ada',
          id: '42',
          metadata: { role: 'admin' }
        },
        topicName: 'user.lifecycle'
      })
    ]);
  });

  it('supports producer-only and consumer-only runtime helpers', async () => {
    const sentMessages: unknown[] = [];
    const topicHandlers = new Map<string, (message: RuntimeIncomingMessage) => Promise<void> | void>();
    const serialization: RuntimeSerializationHooks = {
      async deserialize(_metadata, message) {
        return JSON.parse(new TextDecoder().decode(message.value));
      },
      async serialize(_metadata, payload) {
        return {
          value: new TextEncoder().encode(JSON.stringify(payload))
        };
      }
    };

    const runtimeProducer = createRuntimeProducer({
      producerTransport: {
        async send(message) {
          sentMessages.push(message);
        }
      },
      serialization
    });
    const runtimeConsumer = createRuntimeConsumer({
      consumerTransport: {
        async onTopic(topicName, handler) {
          topicHandlers.set(topicName, handler);
        }
      },
      serialization
    });

    const generatedModule = await loadGeneratedClientModule({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const producer = generatedModule.createProducer(runtimeProducer);

    await producer.userEvents!.userCreated.send({ id: '7' });

    const receivedMessages: unknown[] = [];
    const consumer = generatedModule.createConsumer(runtimeConsumer);
    await consumer.userEvents!.userCreated.on(async (message) => {
      receivedMessages.push(message);
    });

    await topicHandlers.get('user.events')?.({
      headers: {
        'x-kafka-typegen-event': 'user.created'
      },
      topicName: 'user.events',
      value: new TextEncoder().encode(JSON.stringify({ id: '7' }))
    });

    expect(sentMessages).toHaveLength(1);
    expect(receivedMessages).toEqual([
      expect.objectContaining({
        eventName: 'user.created',
        payload: { id: '7' }
      })
    ]);
  });

  it('forwards generated producer and consumer options to the runtime layer', async () => {
    const sentOptions: unknown[] = [];
    const subscribeOptions: unknown[] = [];
    const topicHandlers = new Map<
      string,
      (message: RuntimeIncomingMessage) => Promise<void> | void
    >();
    const serialization: RuntimeSerializationHooks = {
      async deserialize(_metadata, message) {
        return JSON.parse(new TextDecoder().decode(message.value));
      },
      async serialize(_metadata, payload) {
        return {
          value: new TextEncoder().encode(JSON.stringify(payload))
        };
      }
    };

    const runtimeProducer = createRuntimeProducer({
      producerTransport: {
        async send(_message, options) {
          sentOptions.push(options);
        }
      },
      serialization
    });
    const runtimeConsumer = createRuntimeConsumer({
      consumerTransport: {
        async onTopic(topicName, handler, options) {
          subscribeOptions.push({ options, topicName });
          topicHandlers.set(topicName, handler);
        }
      },
      serialization
    });

    const generatedModule = await loadGeneratedClientModule({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const producer = generatedModule.createProducer(runtimeProducer);
    await producer.userEvents!.userCreated.send({ id: '11' }, { acks: -1 });
    await producer.userEvents!.userCreated.send({ id: '12' }, { acks: 1 });

    const consumer = generatedModule.createConsumer(runtimeConsumer);
    await consumer.userEvents!.userCreated.on(async () => {}, { autocommit: false });
    await consumer.userEvents!.userCreated.on(async () => {}, { autocommit: true });

    expect(sentOptions).toEqual([{ acks: -1 }, { acks: 1 }]);
    expect(subscribeOptions).toEqual([
      {
        options: { autocommit: false },
        topicName: 'user.events'
      },
      {
        options: { autocommit: true },
        topicName: 'user.events'
      }
    ]);
  });

  it('preserves private-field-backed native producer methods through generated wrappers', async () => {
    const generatedModule = await loadGeneratedClientModule({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const producerClient = new PrivateFieldProducer();
    const runtimeProducer = createPlatformaticRuntimeProducer({
      producer: producerClient,
      serialization: {
        async deserialize() {
          throw new Error('Not used in native method binding test');
        },
        async serialize() {
          return {
            value: new Uint8Array()
          };
        }
      }
    });

    const producer = generatedModule.createProducer(
      runtimeProducer as ReturnType<typeof createRuntimeProducer>
    ) as ReturnType<typeof generatedModule.createProducer> & {
      close(): void;
      isClosed(): boolean;
    };

    producer.close();

    expect(producer.isClosed()).toBe(true);
  });
});
