import { z } from 'zod';

import type {
  ConfigValidationIssue,
  KafkaTypegenConfig,
  NormalizedEventConfig,
  NormalizedKafkaTypegenConfig,
  NormalizedTopicConfig
} from './types.js';
import { ConfigValidationError } from './types.js';

const nonEmptyStringSchema = z.string().trim().min(1, 'Expected a non-empty string.');

const eventConfigSchema = z.object({
  keySchemaPath: nonEmptyStringSchema.optional(),
  schemaPath: nonEmptyStringSchema
});

const topicConfigSchema = z.object({
  events: z.record(nonEmptyStringSchema, eventConfigSchema).refine(
    (events) => Object.keys(events).length > 0,
    'Each topic must define at least one event.'
  )
});

const kafkaTypegenConfigSchema = z.object({
  outputDir: nonEmptyStringSchema,
  runtime: z
    .object({
      clientModule: nonEmptyStringSchema.optional()
    })
    .optional(),
  schemaRegistry: z
    .object({
      url: nonEmptyStringSchema
    })
    .optional(),
  topics: z
    .record(nonEmptyStringSchema, topicConfigSchema)
    .refine((topics) => Object.keys(topics).length > 0, 'At least one topic must be configured.')
});

export function validateConfig(config: unknown): KafkaTypegenConfig {
  const parsedConfig = kafkaTypegenConfigSchema.safeParse(config);

  if (!parsedConfig.success) {
    throw new ConfigValidationError(
      parsedConfig.error.issues.map<ConfigValidationIssue>((issue) => ({
        message: issue.message,
        path: issue.path.length > 0 ? issue.path.join('.') : 'config'
      }))
    );
  }

  return parsedConfig.data as KafkaTypegenConfig;
}

export function normalizeConfig(config: KafkaTypegenConfig): NormalizedKafkaTypegenConfig {
  const topics: NormalizedTopicConfig[] = Object.entries(config.topics)
    .sort(([leftTopic], [rightTopic]) => leftTopic.localeCompare(rightTopic))
    .map(([topicName, topicConfig]) => {
      const events: NormalizedEventConfig[] = Object.entries(topicConfig.events)
        .sort(([leftEvent], [rightEvent]) => leftEvent.localeCompare(rightEvent))
        .map(([eventName, eventConfig]) => ({
          eventName,
          schemaPath: eventConfig.schemaPath,
          topicName,
          ...(eventConfig.keySchemaPath !== undefined
            ? { keySchemaPath: eventConfig.keySchemaPath }
            : {})
        }));

      return {
        events,
        topicName
      };
    });

  return {
    events: topics.flatMap((topic) => topic.events),
    outputDir: config.outputDir,
    topics,
    ...(config.runtime !== undefined ? { runtime: config.runtime } : {}),
    ...(config.schemaRegistry !== undefined ? { schemaRegistry: config.schemaRegistry } : {})
  };
}
