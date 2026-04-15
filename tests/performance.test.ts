import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve as resolvePath } from 'node:path';
import { performance } from 'node:perf_hooks';

import { afterEach, describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createTypeGenerator,
  resolveConfig
} from '../src/index.js';

const tempDirs: string[] = [];

async function createLargeCatalogWorkspace(topicCount = 60, eventsPerTopic = 4) {
  const workspace = await mkdtemp(join(tmpdir(), 'kafka-typegen-performance-'));
  tempDirs.push(workspace);

  const topics = [];

  for (let topicIndex = 0; topicIndex < topicCount; topicIndex += 1) {
    const events = [];

    for (let eventIndex = 0; eventIndex < eventsPerTopic; eventIndex += 1) {
      const schemaName = `PerfTopic${topicIndex}Event${eventIndex}`;
      const schemaFileName = `${schemaName}.avsc`;
      const schemaPath = resolvePath(workspace, schemaFileName);

      await writeFile(
        schemaPath,
        JSON.stringify(
          {
            type: 'record',
            name: schemaName,
            namespace: 'com.example.performance',
            fields: [
              {
                name: 'id',
                type: 'string'
              },
              {
                name: 'displayName',
                type: ['null', 'string']
              },
              {
                name: 'metadata',
                type: {
                  type: 'map',
                  values: 'string'
                }
              }
            ]
          },
          null,
          2
        ),
        'utf8'
      );

      events.push({
        name: `perf.topic${topicIndex}.event${eventIndex}`,
        schemaPath: `./${schemaFileName}`
      });
    }

    topics.push({
      name: `perf.topic.${topicIndex}`,
      events
    });
  }

  return resolveConfig({
    outputDir: './generated',
    sources: {
      rootDir: workspace
    },
    topics
  });
}

afterEach(async () => {
  for (const tempDir of tempDirs.splice(0, tempDirs.length)) {
    await rm(tempDir, { force: true, recursive: true });
  }
});

describe('performance smoke tests', () => {
  it('builds and generates a large unique-schema catalog within a generous regression budget', async () => {
    const config = await createLargeCatalogWorkspace();
    const buildStart = performance.now();
    const catalog = await createCatalogBuilder().build(config);
    const buildDurationMs = performance.now() - buildStart;

    const generateStart = performance.now();
    const output = await createTypeGenerator().generate(catalog);
    const generateDurationMs = performance.now() - generateStart;

    expect(catalog.topics).toHaveLength(60);
    expect(catalog.events).toHaveLength(240);
    expect(output.files.map((file) => file.filePath)).toEqual([
      'kafka-client.ts',
      'index.ts'
    ]);
    expect(output.files[0]?.contents.length ?? 0).toBeGreaterThan(100_000);

    // This is a smoke guardrail, not a microbenchmark. The threshold is intentionally
    // generous so that real regressions are visible without making the test flaky.
    expect(buildDurationMs).toBeLessThan(10_000);
    expect(generateDurationMs).toBeLessThan(10_000);
  });
});
