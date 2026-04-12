import { execFile } from 'node:child_process';
import {
  access,
  cp,
  mkdir,
  mkdtemp,
  rm,
  writeFile
} from 'node:fs/promises';
import { dirname, join, resolve as resolvePath } from 'node:path';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const cacheRoot = resolvePath('.cache');
const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');
const cliPath = resolvePath('dist', 'cli.cjs');
const tscPath = resolvePath('node_modules', 'typescript', 'bin', 'tsc');

export interface CommandResult {
  readonly exitCode: number;
  readonly stderr: string;
  readonly stdout: string;
}

export async function createIntegrationWorkspace(
  configContents: string,
  appFiles: Readonly<Record<string, string>>
): Promise<string> {
  await access(cliPath).catch(() => {
    throw new Error(
      "Built package artifacts are missing. Run 'pnpm build' before 'pnpm test:integration'."
    );
  });
  await mkdir(cacheRoot, { recursive: true });

  const workspace = await mkdtemp(join(cacheRoot, 'kafka-typegen-integration-'));

  await cp(schemaFixturesDir, join(workspace, 'schemas'), { recursive: true });
  await materializePackage(workspace);
  await writeFile(join(workspace, 'kafka-typegen.config.mjs'), configContents);
  await writeFile(join(workspace, 'tsconfig.json'), createTsConfigText());

  for (const [relativePath, contents] of Object.entries(appFiles)) {
    const targetPath = join(workspace, 'src', relativePath);
    await mkdir(dirname(targetPath), { recursive: true });
    await writeFile(targetPath, contents);
  }

  return workspace;
}

export async function removeIntegrationWorkspace(workspace: string): Promise<void> {
  await rm(workspace, { force: true, recursive: true });
}

export async function runCliCommand(
  workspace: string,
  args: readonly string[]
): Promise<CommandResult> {
  return runCommand(process.execPath, [cliPath, ...args], workspace);
}

export async function runTypecheck(workspace: string): Promise<CommandResult> {
  return runCommand(process.execPath, [tscPath, '-p', 'tsconfig.json', '--noEmit'], workspace);
}

export async function buildWorkspace(workspace: string): Promise<CommandResult> {
  return runCommand(process.execPath, [tscPath, '-p', 'tsconfig.json'], workspace);
}

export async function runWorkspaceScript(
  workspace: string,
  scriptPath: string
): Promise<CommandResult> {
  return runCommand(process.execPath, [join(workspace, 'dist', scriptPath)], workspace);
}

async function materializePackage(workspace: string): Promise<void> {
  const packageDir = join(workspace, 'node_modules', 'kafka-typegen');
  await mkdir(packageDir, { recursive: true });
  await cp(resolvePath('package.json'), join(packageDir, 'package.json'));
  await cp(resolvePath('dist'), join(packageDir, 'dist'), { recursive: true });
}

async function runCommand(
  command: string,
  args: readonly string[],
  cwd: string
): Promise<CommandResult> {
  try {
    const { stderr, stdout } = await execFileAsync(command, [...args], { cwd });
    return { exitCode: 0, stderr, stdout };
  } catch (error) {
    const processError = error as { code?: unknown; stderr?: string; stdout?: string };
    return {
      exitCode: typeof processError.code === 'number' ? processError.code : 1,
      stderr: processError.stderr ?? String(error),
      stdout: processError.stdout ?? ''
    };
  }
}

function createTsConfigText(): string {
  return JSON.stringify(
    {
      compilerOptions: {
        module: 'NodeNext',
        moduleResolution: 'NodeNext',
        outDir: './dist',
        rootDir: './src',
        skipLibCheck: true,
        strict: true,
        target: 'ES2022',
        types: ['node']
      },
      include: ['src/**/*.ts']
    },
    null,
    2
  );
}
