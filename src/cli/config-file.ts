import { constants as fsConstants } from 'node:fs';
import { access } from 'node:fs/promises';
import { resolve as resolvePath } from 'node:path';
import { pathToFileURL } from 'node:url';

import type { CliOptions } from './args.js';

const DEFAULT_CONFIG_FILE = 'kafka-typegen.config.mjs';

export async function resolveConfigFilePath(options: CliOptions): Promise<string> {
  const candidatePath = options.configPath ?? DEFAULT_CONFIG_FILE;
  const resolvedPath = resolvePath(candidatePath);

  await access(resolvedPath, fsConstants.F_OK);
  return resolvedPath;
}

export async function loadUserConfig(configFilePath: string): Promise<unknown> {
  const importedModule = await import(pathToFileURL(configFilePath).href);

  if ('default' in importedModule) {
    return importedModule.default;
  }

  throw new Error(`Config file '${configFilePath}' must export a default config object.`);
}
