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

async function buildGeneratedContents(configInput: Parameters<typeof resolveConfig>[0]): Promise<string> {
  const config = resolveConfig(configInput);
  const catalog = await createCatalogBuilder().build(config);
  const output = await createTypeGenerator().generate(catalog);

  return output.files[0]?.contents ?? '';
}

describe('type generation', () => {
  it('matches the single-event generated output', async () => {
    const contents = await buildGeneratedContents({
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

    const expected = await readFile(resolvePath(generatedFixturesDir, 'single-event.ts'), 'utf8');

    expect(contents).toBe(expected);
  });

  it('matches the multi-event generated output deterministically', async () => {
    const contents = await buildGeneratedContents({
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

    const expected = await readFile(resolvePath(generatedFixturesDir, 'multi-event.ts'), 'utf8');

    expect(contents).toBe(expected);
  });
});
