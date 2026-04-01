#!/usr/bin/env node

import { mkdir, writeFile } from 'node:fs/promises';
import { access } from 'node:fs/promises';
import { constants as fsConstants } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { dirname, resolve as resolvePath } from 'node:path';

import { createCatalogBuilder } from './catalog/index.js';
import { resolveConfig } from './config/index.js';
import { createTypeGenerator } from './generator/index.js';

const DEFAULT_CONFIG_FILE = 'kafka-typegen.config.mjs';

interface CliOptions {
  readonly configPath?: string;
}

function parseArgs(argv: readonly string[]): CliOptions {
  let configPath: string | undefined;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === '--config') {
      configPath = argv[index + 1];
      index += 1;
      continue;
    }
  }

  return configPath !== undefined ? { configPath } : {};
}

async function resolveConfigFilePath(options: CliOptions): Promise<string> {
  const candidatePath = options.configPath ?? DEFAULT_CONFIG_FILE;
  const resolvedPath = resolvePath(candidatePath);

  await access(resolvedPath, fsConstants.F_OK);

  return resolvedPath;
}

async function loadUserConfig(configFilePath: string): Promise<unknown> {
  const importedModule = await import(pathToFileURL(configFilePath).href);

  if ('default' in importedModule) {
    return importedModule.default;
  }

  throw new Error(`Config file '${configFilePath}' must export a default config object.`);
}

async function writeGeneratedFiles(outputDir: string, files: readonly { filePath: string; contents: string }[]) {
  await mkdir(outputDir, { recursive: true });

  await Promise.all(
    files.map(async (file) => {
      const targetPath = resolvePath(outputDir, file.filePath);

      await mkdir(dirname(targetPath), { recursive: true });
      await writeFile(targetPath, file.contents, 'utf8');
    })
  );
}

export async function runCli(argv: readonly string[] = process.argv.slice(2)): Promise<number> {
  const originalCwd = process.cwd();

  try {
    const options = parseArgs(argv);
    const configFilePath = await resolveConfigFilePath(options);
    const configDirectory = dirname(configFilePath);

    process.chdir(configDirectory);

    const userConfig = await loadUserConfig(configFilePath);
    const normalizedConfig = resolveConfig(userConfig);
    const catalog = await createCatalogBuilder().build(normalizedConfig);
    const output = await createTypeGenerator().generate(catalog);

    await writeGeneratedFiles(normalizedConfig.resolvedOutputDir, output.files);

    process.stdout.write(
      `Generated ${output.files.length} file(s) into '${normalizedConfig.resolvedOutputDir}' using '${configFilePath}'.\n`
    );

    return 0;
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown CLI error.';

    process.stderr.write(`${message}\n`);

    return 1;
  } finally {
    process.chdir(originalCwd);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  runCli().then((exitCode) => {
    process.exitCode = exitCode;
  });
}
