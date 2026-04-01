import ts from 'typescript';
import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createRuntimeClient,
  createTypeGenerator,
  resolveConfig,
  type RuntimeIncomingMessage,
  type RuntimeSerializationHooks
} from '../src/index.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');

type GeneratedClientModule = {
  createClient: (runtime: ReturnType<typeof createRuntimeClient>) => {
    producer: {
      send: (event: string, payload: unknown) => Promise<void>;
    };
    consumer: {
      on: (event: string, handler: (message: unknown) => Promise<void> | void) => Promise<void>;
      onTopic: (topic: string, handler: (message: unknown) => Promise<void> | void) => Promise<void>;
    };
  };
};

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

    await client.consumer.on('user.updated', async (message) => {
      receivedMessages.push(message);
    });

    await client.producer.send('user.updated', {
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
});
