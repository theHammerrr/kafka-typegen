import { cp, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { runCli, type CliDependencies } from '../src/cli.js';

const cliFixturePath = join('tests', 'fixtures', 'cli', 'kafka-typegen.config.mjs');
const schemaFixturesDir = join('tests', 'fixtures', 'schemas');

const tempDirs: string[] = [];

async function createTempWorkspace(): Promise<string> {
  const tempDir = await mkdtemp(join(tmpdir(), 'kafka-typegen-cli-'));
  tempDirs.push(tempDir);

  return tempDir;
}

async function createWorkspaceFromFixture(configContents?: string): Promise<string> {
  const workspace = await createTempWorkspace();

  await cp(schemaFixturesDir, join(workspace, 'schemas'), { recursive: true });
  await writeFile(
    join(workspace, 'kafka-typegen.config.mjs'),
    configContents ?? (await readFile(cliFixturePath, 'utf8')),
    'utf8'
  );

  return workspace;
}

async function runCliCapture(
  args: readonly string[],
  cwd: string,
  dependencies: CliDependencies = {}
): Promise<{ code: number; stderr: string; stdout: string }> {
  const originalCwd = process.cwd();
  const originalStdoutWrite = process.stdout.write.bind(process.stdout);
  const originalStderrWrite = process.stderr.write.bind(process.stderr);
  let stdout = '';
  let stderr = '';

  process.chdir(cwd);
  process.stdout.write = ((chunk: string | Uint8Array) => {
    stdout += String(chunk);
    return true;
  }) as typeof process.stdout.write;
  process.stderr.write = ((chunk: string | Uint8Array) => {
    stderr += String(chunk);
    return true;
  }) as typeof process.stderr.write;

  try {
    const code = await runCli(args, dependencies);

    return { code, stderr, stdout };
  } finally {
    process.chdir(originalCwd);
    process.stdout.write = originalStdoutWrite;
    process.stderr.write = originalStderrWrite;
  }
}

afterEach(async () => {
  for (const tempDir of tempDirs.splice(0, tempDirs.length)) {
    await import('node:fs/promises').then(({ rm }) => rm(tempDir, { force: true, recursive: true }));
  }
});

describe('cli', () => {
  it('loads config via --config and writes generated output', async () => {
    const workspace = await createWorkspaceFromFixture();
    const configPath = join(workspace, 'kafka-typegen.config.mjs');

    const result = await runCliCapture(['--config', configPath], workspace);
    const generatedFile = await readFile(join(workspace, 'generated', 'kafka-client.ts'), 'utf8');

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Generated 2 file(s)');
    expect(generatedFile).toContain('export interface UserCreatedPayload');
    expect(generatedFile).toContain('createClient');
  });

  it('discovers the default config file in the current working directory', async () => {
    const workspace = await createWorkspaceFromFixture();

    const result = await runCliCapture([], workspace);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain(join(workspace, 'kafka-typegen.config.mjs'));
  });

  it('fails clearly for invalid config input', async () => {
    const workspace = await createWorkspaceFromFixture('export default { outputDir: "", topics: [] };');
    const configPath = join(workspace, 'kafka-typegen.config.mjs');

    const result = await runCliCapture(['--config', configPath], workspace);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Invalid kafka-typegen config');
  });

  it('fails clearly when --config is missing a value', async () => {
    const workspace = await createWorkspaceFromFixture();

    const result = await runCliCapture(['--config'], workspace);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain("The '--config' flag requires a file path.");
  });

  it('writes output under the configured output directory', async () => {
    const workspace = await createWorkspaceFromFixture(`
      export default {
        outputDir: './artifacts/types',
        sources: { rootDir: './schemas' },
        topics: [
          { name: 'user.events', events: [{ name: 'user.created', schemaPath: './user-created.avsc' }] }
        ]
      };
    `);
    const configPath = join(workspace, 'kafka-typegen.config.mjs');

    const result = await runCliCapture(['--config', configPath], workspace);
    const generatedPath = join(workspace, 'artifacts', 'types', 'kafka-client.ts');
    const generatedFile = await readFile(generatedPath, 'utf8');

    expect(result.code).toBe(0);
    expect(generatedFile).toContain('UserCreatedPayload');
  });

  it('runs sync in dry-run mode by default', async () => {
    const workspace = await createWorkspaceFromFixture(`
      export default {
        outputDir: './generated',
        schemaRegistry: { url: 'http://localhost:8081' },
        sync: { kafka: { brokers: ['localhost:9092'] } },
        sources: { rootDir: './schemas' },
        topics: [
          {
            name: 'user.events',
            sync: {
              partitions: 2,
              replicationFactor: 1
            },
            events: [{ name: 'user.created', schemaPath: './user-created.avsc' }]
          }
        ]
      };
    `);

    const result = await runCliCapture(['sync'], workspace, {
      createSyncClients() {
        return {
          kafkaAdmin: {
            async createTopics() {},
            async listTopics() {
              return [];
            }
          },
          schemaRegistry: {
            async getLatestSubject() {
              return undefined;
            },
            async registerSubject() {}
          }
        };
      }
    });

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Planned 2 sync operation(s).');
    expect(result.stdout).toContain('[kafka] CREATE user.events');
    expect(result.stdout).toContain('[registry] CREATE user.events-user.created');
  });

  it('supports sync json output', async () => {
    const workspace = await createWorkspaceFromFixture(`
      export default {
        outputDir: './generated',
        sync: { kafka: { brokers: ['localhost:9092'] } },
        sources: { rootDir: './schemas' },
        topics: [
          {
            name: 'user.events',
            sync: {
              partitions: 1,
              replicationFactor: 1
            },
            events: [{ name: 'user.created', schemaPath: './user-created.avsc' }]
          }
        ]
      };
    `);

    const result = await runCliCapture(['sync', '--target', 'kafka', '--json'], workspace, {
      createSyncClients() {
        return {
          kafkaAdmin: {
            async createTopics() {},
            async listTopics() {
              return [];
            }
          }
        };
      }
    });

    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({
      applied: false,
      operations: [expect.objectContaining({ action: 'create', target: 'kafka' })]
    });
  });
});
