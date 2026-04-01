import type { KafkaTypegenConfig, NormalizedKafkaTypegenConfig } from './types.js';
import { normalizeConfig, validateConfig } from './schema.js';

export function defineConfig<const TConfig extends KafkaTypegenConfig>(config: TConfig): TConfig {
  return config;
}

export function resolveConfig(config: unknown): NormalizedKafkaTypegenConfig {
  return normalizeConfig(validateConfig(config));
}
