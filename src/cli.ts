#!/usr/bin/env node

import { dirname } from 'node:path';

import { createCatalogBuilder } from './catalog/index.js';
import { loadUserConfig, resolveConfigFilePath } from './cli/config-file.js';
import { isDirectCliExecution, parseArgs } from './cli/args.js';
import { writeGeneratedFiles } from './cli/write-output.js';
import { resolveConfig } from './config/index.js';
import { createTypeGenerator } from './generator/index.js';

export async function runCli(argv: readonly string[] = process.argv.slice(2)): Promise<number> {
  const originalCwd = process.cwd();

  try {
    const options = parseArgs(argv);
    const configFilePath = await resolveConfigFilePath(options);
    process.chdir(dirname(configFilePath));

    const userConfig = await loadUserConfig(configFilePath);
    const normalizedConfig = resolveConfig(userConfig);
    const catalog = await createCatalogBuilder().build(normalizedConfig);
    const output = await createTypeGenerator().generate(catalog);

    await writeGeneratedFiles(normalizedConfig.resolvedOutputDir, output.files);
    process.stdout.write(`Generated ${output.files.length} file(s) into '${normalizedConfig.resolvedOutputDir}' using '${configFilePath}'.\n`);
    return 0;
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
