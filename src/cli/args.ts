export interface CliOptions {
  readonly configPath?: string;
}

export function isDirectCliExecution(argv: readonly string[]): boolean {
  const entrypoint = argv[1];

  return typeof entrypoint === 'string' && /(?:^|[\\/])cli(?:\.[cm]?js)?$/u.test(entrypoint);
}

export function parseArgs(argv: readonly string[]): CliOptions {
  let configPath: string | undefined;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === '--config') {
      const candidatePath = argv[index + 1];
      if (candidatePath === undefined || candidatePath.startsWith('--')) {
        throw new Error("The '--config' flag requires a file path.");
      }

      configPath = candidatePath;
      index += 1;
      continue;
    }

    throw new Error(`Unknown CLI argument '${argument}'.`);
  }

  return configPath !== undefined ? { configPath } : {};
}
