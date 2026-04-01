#!/usr/bin/env node

import { dirname } from 'node:path';

import { createCatalogBuilder } from './catalog/index.js';
import { loadUserConfig, resolveConfigFilePath } from './cli/config-file.js';
import { isDirectCliExecution, parseArgs } from './cli/args.js';
import { writeGeneratedFiles } from './cli/write-output.js';
import { resolveConfig } from './config/index.js';
import { createTypeGenerator } from './generator/index.js';
import { createSyncClients, executeSync, formatSyncResult, type SyncClients } from './sync/index.js';

export interface CliDependencies {
  readonly createSyncClients?: (config: ReturnType<typeof resolveConfig>) => SyncClients;
}

async function runGenerateCommand(configFilePath: string): Promise<number> {
  const userConfig = await loadUserConfig(configFilePath);
  const normalizedConfig = resolveConfig(userConfig);
  const catalog = await createCatalogBuilder().build(normalizedConfig);
  const output = await createTypeGenerator().generate(catalog);

  await writeGeneratedFiles(normalizedConfig.resolvedOutputDir, output.files);
  process.stdout.write(`Generated ${output.files.length} file(s) into '${normalizedConfig.resolvedOutputDir}' using '${configFilePath}'.\n`);
  return 0;
}

async function runSyncCommand(
  configFilePath: string,
  options: ReturnType<typeof parseArgs>,
  dependencies: CliDependencies
): Promise<number> {
  const userConfig = await loadUserConfig(configFilePath);
  const normalizedConfig = resolveConfig(userConfig);
  const catalog = await createCatalogBuilder().build(normalizedConfig);
  const result = await executeSync(
    { catalog, config: normalizedConfig, options: options.sync },
    {
      apply: options.sync.apply,
      clients: dependencies.createSyncClients?.(normalizedConfig) ?? createSyncClients(normalizedConfig),
      config: normalizedConfig,
      target: options.sync.target
    }
  );

  process.stdout.write(
    `${options.sync.json === true ? JSON.stringify(result, null, 2) : formatSyncResult(result)}\n`
  );
  return 0;
}

export async function runCli(argv: readonly string[] = process.argv.slice(2), dependencies: CliDependencies = {}): Promise<number> {
  const originalCwd = process.cwd();

  try {
    const options = parseArgs(argv);
    const configFilePath = await resolveConfigFilePath(options);
    process.chdir(dirname(configFilePath));
    return await (options.command === 'sync'
      ? runSyncCommand(configFilePath, options, dependencies)
      : runGenerateCommand(configFilePath));
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown CLI error.';
    process.stderr.write(`${message}\n`);
    return 1;
  } finally {
    process.chdir(originalCwd);
  }
}

if (isDirectCliExecution(process.argv)) {
  runCli().then((exitCode) => {
    process.exitCode = exitCode;
  });
}
