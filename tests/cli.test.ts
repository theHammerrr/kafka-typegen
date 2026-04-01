import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve as resolvePath } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

import { runCli } from '../src/cli.js';

const cliFixturePath = resolvePath('tests', 'fixtures', 'cli', 'kafka-typegen.config.mjs');
const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');

const tempDirs: string[] = [];

async function createTempWorkspace(): Promise<string> {
  const tempDir = await mkdtemp(join(tmpdir(), 'kafka-typegen-cli-'));
  tempDirs.push(tempDir);

  return tempDir;
}

async function runCliCapture(args: readonly string[], cwd: string): Promise<{ code: number; stderr: string; stdout: string }> {
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
    const code = await runCli(args);

    return { code, stderr, stdout };
  } finally {
    process.chdir(originalCwd);
    process.stdout.write = originalStdoutWrite;
    process.stderr.write = originalStderrWrite;
  }
}

afterEach(async () => {
  for (const tempDir of tempDirs.splice(0, tempDirs.length)) {
    await import('node:fs/promises').then(({ rm }) =>
      rm(tempDir, { force: true, recursive: true })
    );
  }
});

describe('cli', () => {
  it('loads config via --config and writes generated output', async () => {
    const workspace = await createTempWorkspace();
    const configPath = join(workspace, 'kafka-typegen.config.mjs');

    await writeFile(
      configPath,
      (await readFile(cliFixturePath, 'utf8')).replace('../schemas', schemaFixturesDir.replaceAll('\\', '/')),
      'utf8'
    );

    const result = await runCliCapture(['--config', configPath], workspace);
    const generatedFile = await readFile(join(workspace, 'generated', 'kafka-client.ts'), 'utf8');

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Generated 1 file(s)');
    expect(generatedFile).toContain('export interface UserCreatedPayload');
    expect(generatedFile).toContain('createClient');
  });

  it('discovers the default config file in the current working directory', async () => {
    const workspace = await createTempWorkspace();
    const configPath = join(workspace, 'kafka-typegen.config.mjs');

    await writeFile(
      configPath,
      (await readFile(cliFixturePath, 'utf8')).replace('../schemas', schemaFixturesDir.replaceAll('\\', '/')),
      'utf8'
    );

    const result = await runCliCapture([], workspace);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain(configPath);
  });

  it('fails clearly for invalid config input', async () => {
    const workspace = await createTempWorkspace();
    const configPath = join(workspace, 'kafka-typegen.config.mjs');

    await writeFile(
      configPath,
      'export default { outputDir: "", topics: [] };',
      'utf8'
    );

    const result = await runCliCapture(['--config', configPath], workspace);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Invalid kafka-typegen config');
  });

  it('writes output under the configured output directory', async () => {
    const workspace = await createTempWorkspace();
    const configPath = join(workspace, 'kafka-typegen.config.mjs');
    const nestedOutputConfig = `
      export default {
        outputDir: './artifacts/types',
        sources: { rootDir: '${schemaFixturesDir.replaceAll('\\', '/')}' },
        topics: [
          { name: 'user.events', events: [{ name: 'user.created', schemaPath: './user-created.avsc' }] }
        ]
      };
    `;

    await writeFile(configPath, nestedOutputConfig, 'utf8');
    await mkdir(join(workspace, 'artifacts'), { recursive: true });

    const result = await runCliCapture(['--config', configPath], workspace);
    const generatedPath = join(workspace, 'artifacts', 'types', 'kafka-client.ts');
    const generatedFile = await readFile(generatedPath, 'utf8');

    expect(result.code).toBe(0);
    expect(generatedFile).toContain('UserCreatedPayload');
  });
});
