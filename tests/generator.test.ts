import { readFile } from 'node:fs/promises';
import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createTypeGenerator,
  resolveConfig
} from '../src/index.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');
const generatedFixturesDir = resolvePath('tests', 'fixtures', 'generated');

async function buildGeneratedOutput(configInput: Parameters<typeof resolveConfig>[0]) {
  const config = resolveConfig(configInput);
  const catalog = await createCatalogBuilder().build(config);
  return createTypeGenerator().generate(catalog);
}

describe('type generation', () => {
  it('matches the single-event generated output', async () => {
    const output = await buildGeneratedOutput({
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
    const contents = output.files.find((file) => file.filePath === 'kafka-client.ts')?.contents ?? '';
    const expected = await readFile(resolvePath(generatedFixturesDir, 'single-event.ts'), 'utf8');

    expect(contents).toBe(expected);
    expect(output.files.map((file) => file.filePath)).toEqual(['kafka-client.ts', 'index.ts']);
  });

  it('matches the multi-event generated output deterministically', async () => {
    const output = await buildGeneratedOutput({
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
    const contents = output.files.find((file) => file.filePath === 'kafka-client.ts')?.contents ?? '';
    const expected = await readFile(resolvePath(generatedFixturesDir, 'multi-event.ts'), 'utf8');

    expect(contents).toBe(expected);
  });

  it('emits a local package wrapper when packageName is configured', async () => {
    const output = await buildGeneratedOutput({
      generation: {
        packageName: '@acme/generated-kafka'
      },
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

    expect(output.files).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          contents: "export * from './kafka-client.js';\n",
          filePath: 'index.ts'
        }),
        expect.objectContaining({
          filePath: 'package.json'
        })
      ])
    );
    expect(output.files.find((file) => file.filePath === 'package.json')?.contents).toContain('"name": "@acme/generated-kafka"');
  });

  it('emits a schema registry config constant with url only', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      schemaRegistry: {
        auth: {
          password: 'secret-password',
          username: 'registry-user'
        },
        url: 'http://localhost:8081'
      },
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

    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain("export const SchemaRegistryConfig = {\n  url: 'http://localhost:8081'\n} as const;");
    expect(contents).not.toContain('secret-password');
    expect(contents).not.toContain('registry-user');
  });
});
