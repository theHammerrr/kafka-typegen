import { z } from 'zod';

import {
  createSchemaRegistryAuthSchema,
  createTopicSyncConfigSchema,
  saslMechanismSchema,
  schemaRegistryCompatibilitySchema,
  schemaRegistryDriftActionSchema
} from './sync-schemas.js';

const nonEmptyStringSchema = z.string().trim().min(1, 'Expected a non-empty string.');
const positiveIntegerSchema = z.int().positive('Expected a positive integer.');
const nonNegativeIntegerSchema = z.int().nonnegative('Expected a non-negative integer.');

export const subjectNameStrategySchema = z.enum(['event-name', 'topic-name', 'topic-event']);
export const runtimeTransportSchema = z.enum(['kafkajs', '@platformatic/kafka']);

const schemaRegistryAuthSchema = createSchemaRegistryAuthSchema(
  nonEmptyStringSchema
);
const topicSyncConfigSchema = createTopicSyncConfigSchema(
  nonNegativeIntegerSchema,
  positiveIntegerSchema
);

const eventConfigSchema = z.object({
  keySchemaPath: nonEmptyStringSchema.optional(),
  name: nonEmptyStringSchema,
  schemaPath: nonEmptyStringSchema,
  subject: nonEmptyStringSchema.optional()
}).strict();

const topicConfigSchema = z.object({
  events: z.array(eventConfigSchema).min(1, 'Each topic must define at least one event.'),
  keySchemaPath: nonEmptyStringSchema.optional(),
  name: nonEmptyStringSchema,
  subjectStrategy: subjectNameStrategySchema.optional(),
  sync: topicSyncConfigSchema.optional()
}).strict();

export const kafkaTypegenConfigSchema = z.object({
  generation: z
    .object({
      avroExternalTypes: z.record(nonEmptyStringSchema, nonEmptyStringSchema).optional(),
      avroSemanticMode: z.enum(['default', 'safe']).optional(),
      typesFileName: nonEmptyStringSchema.optional()
    })
    .strict()
    .optional(),
  naming: z
    .object({
      eventTypeSuffix: nonEmptyStringSchema.optional(),
      topicTypeSuffix: nonEmptyStringSchema.optional()
    })
    .strict()
    .optional(),
  outputDir: nonEmptyStringSchema,
  runtime: z
    .object({
      module: nonEmptyStringSchema.optional(),
      transport: runtimeTransportSchema.optional()
    })
    .strict()
    .optional(),
  schemaRegistry: z
    .object({
      auth: schemaRegistryAuthSchema.optional(),
      subjectStrategy: subjectNameStrategySchema.optional(),
      url: nonEmptyStringSchema
    })
    .strict()
    .optional(),
  sync: z
    .object({
      kafka: z
        .object({
          brokers: z.array(nonEmptyStringSchema).min(1, 'Expected at least one broker.'),
          clientId: nonEmptyStringSchema.optional(),
          failOnDrift: z.boolean().optional(),
          sasl: z
            .object({
              mechanism: saslMechanismSchema,
              password: nonEmptyStringSchema,
              username: nonEmptyStringSchema
            })
            .strict()
            .optional(),
          ssl: z.boolean().optional()
        })
        .strict()
        .optional(),
      schemaRegistry: z
        .object({
          compatibility: schemaRegistryCompatibilitySchema.optional(),
          failOnDrift: z.boolean().optional(),
          onDrift: schemaRegistryDriftActionSchema.optional()
        })
        .strict()
        .optional()
    })
    .strict()
    .optional(),
  sources: z.object({ rootDir: nonEmptyStringSchema.optional() }).strict().optional(),
  topics: z.array(topicConfigSchema).min(1, 'At least one topic must be configured.')
}).strict();
