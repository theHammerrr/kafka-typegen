import ts from 'typescript';
import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createTypeGenerator,
  resolveConfig,
  type RuntimeConsumerMessage,
  type RuntimeEventMetadata
} from '../src/index.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');

type GeneratedConsumerModule = {
  createConsumer: (runtimeConsumer: {
    on: (
      metadata: RuntimeEventMetadata,
      handler: (message: RuntimeConsumerMessage<unknown>) => Promise<void> | void
    ) => Promise<void>;
    onTopic: (
      topicName: string,
      metadataByEvent: Readonly<Record<string, RuntimeEventMetadata>>,
      handler: (message: RuntimeConsumerMessage<unknown>) => Promise<void> | void
    ) => Promise<void>;
  }) => {
    userEvents?: {
      on?: (handler: (message: unknown) => Promise<void> | void) => Promise<void>;
      userCreated?: {
        on: (handler: (message: unknown) => Promise<void> | void) => Promise<void>;
      };
    };
    userLifecycle?: {
      on?: (handler: (message: unknown) => Promise<void> | void) => Promise<void>;
      userUpdated?: {
        on: (handler: (message: unknown) => Promise<void> | void) => Promise<void>;
      };
    };
  };
};

async function loadGeneratedModule(configInput: Parameters<typeof resolveConfig>[0]): Promise<GeneratedConsumerModule> {
  const config = resolveConfig(configInput);
  const catalog = await createCatalogBuilder().build(config);
  const output = await createTypeGenerator().generate(catalog);
  const transpiled = ts.transpileModule(output.files[0]?.contents ?? '', {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022
    }
  }).outputText;

  const module = { exports: {} as GeneratedConsumerModule };
  const evaluator = new Function('exports', 'module', transpiled);

  evaluator(module.exports, module);

  return module.exports;
}

describe('generated consumer runtime', () => {
  it('routes topic-first event registration through event metadata', async () => {
    const generatedModule = await loadGeneratedModule({
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

    const registrations: Array<{ metadata: RuntimeEventMetadata; handler: unknown }> = [];
    const consumer = generatedModule.createConsumer({
      async on(metadata, handler) {
        registrations.push({ handler, metadata });
      },
      async onTopic() {}
    });

    const handler = () => undefined;
    await consumer.userEvents?.userCreated?.on(handler);

    expect(registrations).toEqual([
      {
        handler,
        metadata: expect.objectContaining({
          eventName: 'user.created',
          topicName: 'user.events'
        })
      }
    ]);
  });

  it('routes topic-level consumer registration through metadata-by-event', async () => {
    const generatedModule = await loadGeneratedModule({
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
            },
            {
              name: 'user.updated',
              schemaPath: './user-updated.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const topicRegistrations: Array<{
      handler: unknown;
      metadataByEvent: Readonly<Record<string, RuntimeEventMetadata>>;
      topicName: string;
    }> = [];
    const eventRegistrations: Array<{ metadata: RuntimeEventMetadata; handler: unknown }> = [];
    const consumer = generatedModule.createConsumer({
      async on(metadata, handler) {
        eventRegistrations.push({ handler, metadata });
      },
      async onTopic(topicName, metadataByEvent, handler) {
        topicRegistrations.push({ handler, metadataByEvent, topicName });
      }
    });

    const eventHandler = () => undefined;
    const topicHandler = () => undefined;

    await consumer.userEvents?.userCreated?.on(eventHandler);
    await consumer.userEvents?.on?.(topicHandler);

    expect(eventRegistrations[0]?.metadata.eventName).toBe('user.created');
    expect(eventRegistrations[0]?.metadata.topicName).toBe('user.events');
    expect(topicRegistrations).toEqual([
      {
        handler: topicHandler,
        metadataByEvent: {
          'user.created': expect.objectContaining({
            eventName: 'user.created',
            topicName: 'user.events'
          }),
          'user.updated': expect.objectContaining({
            eventName: 'user.updated',
            topicName: 'user.events'
          })
        },
        topicName: 'user.events'
      }
    ]);
  });
});
