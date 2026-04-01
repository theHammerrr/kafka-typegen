import { cp, mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { createCatalogBuilder, createTypeGenerator, resolveConfig } from '../src/index.js';

const schemaFixturesDir = join('tests', 'fixtures', 'schemas');
const tempDirs: string[] = [];

async function createTempWorkspace(): Promise<string> {
  const tempDir = await mkdtemp(join(tmpdir(), 'kafka-typegen-e2e-'));
  tempDirs.push(tempDir);

  await cp(schemaFixturesDir, join(tempDir, 'schemas'), { recursive: true });

  return tempDir;
}

async function generateFromWorkspace(
  workspace: string,
  configInput: Parameters<typeof resolveConfig>[0]
): Promise<string> {
  const originalCwd = process.cwd();

  process.chdir(workspace);

  try {
    const config = resolveConfig(configInput);
    const catalog = await createCatalogBuilder().build(config);
    const output = await createTypeGenerator().generate(catalog);

    return output.files[0]?.contents ?? '';
  } finally {
    process.chdir(originalCwd);
  }
}

afterEach(async () => {
  for (const tempDir of tempDirs.splice(0, tempDirs.length)) {
    await import('node:fs/promises').then(({ rm }) => rm(tempDir, { force: true, recursive: true }));
  }
});

describe('end-to-end generation', () => {
  it('generates a coherent single-event client from config through codegen', async () => {
    const workspace = await createTempWorkspace();
    const contents = await generateFromWorkspace(workspace, {
      outputDir: './generated',
      runtime: {
        module: '@acme/kafka-runtime'
      },
      sources: {
        rootDir: './schemas'
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

    expect(contents).toContain("import type { RuntimeClient, RuntimeConsumer, RuntimeEventMetadata, RuntimeProducer } from '@acme/kafka-runtime';");
    expect(contents).toContain("export type EventName = 'user.created';");
    expect(contents).toContain("schemaFilePath: 'user-created.avsc';");
    expect(contents).toContain('export function createClient(runtime: RuntimeClient): GeneratedClient {');
  });

  it('generates stable multi-event output across the full pipeline', async () => {
    const workspace = await createTempWorkspace();
    const contents = await generateFromWorkspace(workspace, {
      outputDir: './generated',
      sources: {
        rootDir: './schemas'
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
    const expected = await readFile(join('tests', 'fixtures', 'generated', 'multi-event.ts'), 'utf8');

    expect(contents).toBe(expected);
  });

  it('uses the platformatic runtime module when that transport is selected', async () => {
    const workspace = await createTempWorkspace();
    const contents = await generateFromWorkspace(workspace, {
      outputDir: './generated',
      runtime: {
        transport: '@platformatic/kafka'
      },
      sources: {
        rootDir: './schemas'
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

    expect(contents).toContain(
      "import type { RuntimeClient, RuntimeConsumer, RuntimeEventMetadata, RuntimeProducer } from 'kafka-typegen/runtime/platformatic';"
    );
  });

  it('surfaces schema load failures with actionable messaging', async () => {
    const workspace = await createTempWorkspace();

    await expect(
      generateFromWorkspace(workspace, {
        outputDir: './generated',
        sources: {
          rootDir: './schemas'
        },
        topics: [
          {
            events: [
              {
                name: 'user.created',
                schemaPath: './missing.avsc'
              }
            ],
            name: 'user.events'
          }
        ]
      })
    ).rejects.toThrowError("Failed to load schema file");
  });
});
