import type { SyncCliOptions, SyncTarget } from '../sync/index.js';

export interface CliOptions {
  readonly command: 'generate' | 'sync';
  readonly configPath?: string;
  readonly sync: SyncCliOptions;
}

export function isDirectCliExecution(argv: readonly string[]): boolean {
  const entrypoint = argv[1];

  return typeof entrypoint === 'string' && /(?:^|[\\/])cli(?:\.[cm]?js)?$/u.test(entrypoint);
}

function readFlagValue(argv: readonly string[], index: number, message: string): string {
  const candidate = argv[index + 1];
  if (candidate === undefined || candidate.startsWith('--')) {
    throw new Error(message);
  }

  return candidate;
}

export function parseArgs(argv: readonly string[]): CliOptions {
  let command: 'generate' | 'sync' = argv[0] === 'sync' ? 'sync' : 'generate';
  let configPath: string | undefined;
  let apply = false;
  let json = false;
  let target: SyncTarget = 'all';
  const startIndex = argv[0] === 'sync' || argv[0] === 'generate' ? 1 : 0;

  for (let index = startIndex; index < argv.length; index += 1) {
    const argument = argv[index];

    if (argument === '--config') {
      configPath = readFlagValue(argv, index, "The '--config' flag requires a file path.");
      index += 1;
      continue;
    }

    if (argument === '--target') {
      const nextTarget = readFlagValue(argv, index, "The '--target' flag requires a value.");
      if (nextTarget !== 'all' && nextTarget !== 'kafka' && nextTarget !== 'registry') {
        throw new Error(`Unsupported sync target '${nextTarget}'.`);
      }

      target = nextTarget;
      index += 1;
      continue;
    }

    if (argument === '--apply') {
      apply = true;
      continue;
    }

    if (argument === '--json') {
      json = true;
      continue;
    }

    throw new Error(`Unknown CLI argument '${argument}'.`);
  }

  return { command, ...(configPath !== undefined ? { configPath } : {}), sync: { apply, json, target } };
}
