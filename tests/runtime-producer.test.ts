import ts from 'typescript';
import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createTypeGenerator,
  resolveConfig,
  type RuntimeEventMetadata
} from '../src/index.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');

type GeneratedModule = {
  createProducer: (runtimeProducer: {
    send: (metadata: RuntimeEventMetadata, payload: unknown) => Promise<void>;
  }) => {
    userEvents?: {
      userCreated?: {
        send: (payload: unknown) => Promise<void>;
      };
    };
    userLifecycle?: {
      userUpdated?: {
        send: (payload: unknown) => Promise<void>;
      };
    };
  };
};

async function loadGeneratedModule(configInput: Parameters<typeof resolveConfig>[0]): Promise<GeneratedModule> {
  const config = resolveConfig(configInput);
  const catalog = await createCatalogBuilder().build(config);
  const output = await createTypeGenerator().generate(catalog);
  const transpiled = ts.transpileModule(output.files[0]?.contents ?? '', {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022
    }
  }).outputText;

  const module = { exports: {} as GeneratedModule };
  const evaluator = new Function('exports', 'module', transpiled);

  evaluator(module.exports, module);

  return module.exports;
}

describe('generated producer runtime', () => {
  it('routes topic-first producer helpers through event metadata', async () => {
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

    const calls: Array<{ metadata: RuntimeEventMetadata; payload: unknown }> = [];
    const producer = generatedModule.createProducer({
      async send(metadata, payload) {
        calls.push({ metadata, payload });
      }
    });

    await producer.userEvents?.userCreated?.send({
      email: 'a@example.com',
      id: '1',
      isAdmin: true
    });

    expect(calls).toEqual([
      {
        metadata: expect.objectContaining({
          eventName: 'user.created',
          payloadTypeName: 'UserCreatedPayload',
          schemaFilePath: 'user-created.avsc',
          schemaName: 'UserCreated',
          subjectName: 'user.events-user.created',
          topicName: 'user.events'
        }),
        payload: { email: 'a@example.com', id: '1', isAdmin: true }
      }
    ]);
  });

  it('routes grouped producer helpers to the correct topic metadata', async () => {
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

    const calls: Array<{ metadata: RuntimeEventMetadata; payload: unknown }> = [];
    const producer = generatedModule.createProducer({
      async send(metadata, payload) {
        calls.push({ metadata, payload });
      }
    });

    await producer.userLifecycle?.userUpdated?.send({
      displayName: 'Ada',
      id: '42',
      metadata: { role: 'admin' }
    });

    expect(calls[0]?.metadata.topicName).toBe('user.lifecycle');
    expect(calls[0]?.metadata.eventName).toBe('user.updated');
    expect(calls[0]?.metadata.subjectName).toBe('user.lifecycle-user.updated');
    expect(calls[0]?.payload).toEqual({
      displayName: 'Ada',
      id: '42',
      metadata: { role: 'admin' }
    });
  });
});
