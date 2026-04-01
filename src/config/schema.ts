import type { ConfigValidationIssue, KafkaTypegenConfig, NormalizedKafkaTypegenConfig } from './types.js';
import { ConfigValidationError } from './types.js';
import { buildValidationIssue } from './issues.js';
import { normalizeConfig as normalizeConfigValue } from './normalize.js';
import { kafkaTypegenConfigSchema } from './schemas.js';
import { validateSemanticConfig } from './semantic-validation.js';

export function validateConfig(config: unknown): KafkaTypegenConfig {
  const parsedConfig = kafkaTypegenConfigSchema.safeParse(config);

  if (!parsedConfig.success) {
    throw new ConfigValidationError(
      parsedConfig.error.issues.map<ConfigValidationIssue>((issue) => buildValidationIssue(issue.path, issue.message))
    );
  }

  const validatedConfig = parsedConfig.data as KafkaTypegenConfig;
  validateSemanticConfig(validatedConfig);
  return validatedConfig;
}

export function normalizeConfig(config: KafkaTypegenConfig): NormalizedKafkaTypegenConfig {
  return normalizeConfigValue(config);
}
