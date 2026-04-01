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
      handler: (message: RuntimeConsumerMessage<unknown>) => Promise<void> | void
    ) => Promise<void>;
  }) => {
    on: (event: string, handler: (message: unknown) => Promise<void> | void) => Promise<void>;
    onTopic: (topic: string, handler: (message: unknown) => Promise<void> | void) => Promise<void>;
    events: Record<string, { on: (handler: (message: unknown) => Promise<void> | void) => Promise<void> }>;
  };
  producerEventMetadata: Record<string, RuntimeEventMetadata>;
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
  it('routes event-first consumer registration through event metadata', async () => {
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
    await consumer.on('user.created', handler);

    expect(registrations).toEqual([
      {
        handler,
        metadata: generatedModule.producerEventMetadata['user.created']
      }
    ]);
  });

  it('routes topic-based consumer registration by topic name', async () => {
    const generatedModule = await loadGeneratedModule({
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

    const topicRegistrations: Array<{ handler: unknown; topicName: string }> = [];
    const eventRegistrations: Array<{ metadata: RuntimeEventMetadata; handler: unknown }> = [];
    const consumer = generatedModule.createConsumer({
      async on(metadata, handler) {
        eventRegistrations.push({ handler, metadata });
      },
      async onTopic(topicName, handler) {
        topicRegistrations.push({ handler, topicName });
      }
    });

    const eventHandler = () => undefined;
    const topicHandler = () => undefined;

    await consumer.events['userUpdated']!.on(eventHandler);
    await consumer.onTopic('user.lifecycle', topicHandler);

    expect(eventRegistrations[0]?.metadata.eventName).toBe('user.updated');
    expect(eventRegistrations[0]?.metadata.topicName).toBe('user.lifecycle');
    expect(topicRegistrations).toEqual([
      {
        handler: topicHandler,
        topicName: 'user.lifecycle'
      }
    ]);
  });
});
