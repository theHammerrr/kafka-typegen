export { defineConfig, resolveConfig } from './define-config.js';
export { normalizeConfig, validateConfig } from './schema.js';
export {
  ConfigValidationError,
  type ConfigValidationIssue,
  type KafkaTypegenConfig,
  type KafkaTypegenEventConfig,
  type KafkaTypegenRuntimeConfig,
  type KafkaTypegenSchemaRegistryConfig,
  type KafkaTypegenTopicConfig,
  type NormalizedEventConfig,
  type NormalizedKafkaTypegenConfig,
  type NormalizedTopicConfig
} from './types.js';
