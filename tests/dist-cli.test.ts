import { execFile } from 'node:child_process';
import { cp, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve as resolvePath } from 'node:path';
import { promisify } from 'node:util';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const execFileAsync = promisify(execFile);
const cliPath = resolvePath('dist', 'cli.cjs');
const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');
const tsupCliPath = resolvePath('..', '..', 'node_modules', 'tsup', 'dist', 'cli-default.js');
const tempDirs: string[] = [];

beforeAll(async () => {
  await execFileAsync(process.execPath, [tsupCliPath], {
    cwd: resolvePath('.')
  });
}, 60000);

afterAll(async () => {
  for (const tempDir of tempDirs.splice(0, tempDirs.length)) {
    await rm(tempDir, { force: true, recursive: true });
  }
});

describe('built CLI artifact', () => {
  it('generates a client from dist/cli.cjs', async () => {
    const workspace = await mkdtemp(join(tmpdir(), 'kafka-typegen-dist-cli-'));
    tempDirs.push(workspace);

    await cp(schemaFixturesDir, join(workspace, 'schemas'), { recursive: true });
    await writeFile(
      join(workspace, 'kafka-typegen.config.mjs'),
      [
        'export default {',
        "  outputDir: './generated',",
        '  sources: {',
        "    rootDir: './schemas'",
        '  },',
        '  topics: [',
        '    {',
        "      name: 'user.events',",
        '      events: [',
        '        {',
        "          name: 'user.created',",
        "          schemaPath: './user-created.avsc'",
        '        }',
        '      ]',
        '    }',
        '  ]',
        '};',
        ''
      ].join('\n')
    );

    const { stdout } = await execFileAsync(process.execPath, [
      cliPath,
      'generate',
      '--config',
      join(workspace, 'kafka-typegen.config.mjs')
    ]);

    const generatedContents = await readFile(
      join(workspace, 'generated', 'kafka-client.ts'),
      'utf8'
    );

    expect(stdout).toContain('Generated 2 file(s)');
    expect(generatedContents).toContain('export interface GeneratedProducerTopics');
    expect(generatedContents).toContain('userEvents: {');
  });
});
