import { describe, expect, it, vi } from 'vitest';

import {
  createRuntimeConsumer,
  createRuntimeProducer,
  executeSync,
  resolveConfig,
  type KafkaTypegenObservedEvent,
  type RuntimeIncomingMessage,
  type RuntimeSerializationHooks,
  type SyncClients
} from '../src/index.js';

const testMetadata = {
  eventName: 'user.created',
  payloadTypeName: 'UserCreatedPayload',
  schemaFilePath: 'user-created.avsc',
  schemaName: 'UserCreated',
  subjectName: 'user.events-user.created',
  topicName: 'user.events'
} as const;

describe('observability', () => {
  it('emits structured producer events for successful sends', async () => {
    const events: KafkaTypegenObservedEvent[] = [];
    const producer = createRuntimeProducer({
      observer: {
        onEvent(event) {
          events.push(event);
        }
      },
      producerTransport: {
        async send() {}
      },
      serialization: {
        async deserialize() {
          throw new Error('Not used');
        },
        async serialize() {
          return {
            value: new Uint8Array([1, 2, 3])
          };
        }
      }
    });

    await producer.send(testMetadata, { id: '1' });

    expect(events).toEqual([
      {
        eventName: 'user.created',
        topicName: 'user.events',
        type: 'runtime.producer.send.start'
      },
      {
        eventName: 'user.created',
        topicName: 'user.events',
        type: 'runtime.producer.send.success'
      }
    ]);
  });

  it('logs and emits consumer handler failures', async () => {
    let registeredHandler:
      | ((message: RuntimeIncomingMessage) => Promise<void> | void)
      | undefined;
    const events: KafkaTypegenObservedEvent[] = [];
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      warn: vi.fn()
    };
    const consumer = createRuntimeConsumer({
      consumerTransport: {
        async onTopic(_topicName, handler) {
          registeredHandler = handler;
        }
      },
      logger,
      observer: {
        onEvent(event) {
          events.push(event);
        }
      },
      serialization: {
        async deserialize<TPayload>() {
          return { id: '1' } as TPayload;
        },
        async serialize() {
          throw new Error('Not used');
        }
      }
    });

    await consumer.on(testMetadata, async () => {
      throw new Error('handler failed');
    });

    await expect(
      registeredHandler?.({
        headers: {
          'x-kafka-typegen-event': 'user.created'
        },
        topicName: 'user.events',
        value: new Uint8Array([1])
      } satisfies RuntimeIncomingMessage)
    ).rejects.toThrow('handler failed');

    expect(logger.error).toHaveBeenCalledWith('Runtime consumer handler failed.', {
      error: 'handler failed',
      eventName: 'user.created',
      topicName: 'user.events'
    });
    expect(events).toEqual([
      {
        eventName: 'user.created',
        topicName: 'user.events',
        type: 'runtime.consumer.handle.start'
      },
      {
        error: 'handler failed',
        eventName: 'user.created',
        topicName: 'user.events',
        type: 'runtime.consumer.handle.failure'
      }
    ]);
  });

  it('emits sync lifecycle events', async () => {
    const config = resolveConfig({
      outputDir: './generated',
      sync: {
        kafka: {
          brokers: ['localhost:9092']
        }
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './tests/fixtures/schemas/user-created.avsc'
            }
          ],
          name: 'user.events',
          sync: {
            partitions: 1,
            replicationFactor: 1
          }
        }
      ]
    });
    const events: KafkaTypegenObservedEvent[] = [];
    const clients: SyncClients = {
      kafkaAdmin: {
        async createTopics() {},
        async listTopics() {
          return [];
        }
      }
    };

    const result = await executeSync(
      {
        catalog: { events: [], topics: [] } as never,
        config,
        options: { apply: false, json: false, target: 'kafka' }
      },
      {
        apply: false,
        clients,
        config,
        observer: {
          onEvent(event) {
            events.push(event);
          }
        },
        target: 'kafka'
      }
    );

    expect(result.operations).toEqual([
      {
        action: 'create',
        details: 'Topic will be created.',
        resourceName: 'user.events',
        target: 'kafka'
      }
    ]);
    expect(events).toEqual([
      {
        apply: false,
        target: 'kafka',
        type: 'sync.start'
      },
      {
        action: 'create',
        details: 'Topic will be created.',
        resourceName: 'user.events',
        target: 'kafka',
        type: 'sync.operation'
      },
      {
        apply: false,
        operationCount: 1,
        target: 'kafka',
        type: 'sync.complete'
      }
    ]);
  });
});
