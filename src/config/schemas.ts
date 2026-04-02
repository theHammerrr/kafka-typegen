import { z } from 'zod';

const nonEmptyStringSchema = z.string().trim().min(1, 'Expected a non-empty string.');
const positiveIntegerSchema = z.int().positive('Expected a positive integer.');

export const subjectNameStrategySchema = z.enum(['event-name', 'topic-name', 'topic-event']);
export const runtimeTransportSchema = z.enum(['kafkajs', '@platformatic/kafka']);
const saslMechanismSchema = z.enum(['plain', 'scram-sha-256', 'scram-sha-512']);

const eventConfigSchema = z.object({
  keySchemaPath: nonEmptyStringSchema.optional(),
  name: nonEmptyStringSchema,
  schemaPath: nonEmptyStringSchema,
  subject: nonEmptyStringSchema.optional()
});

const topicConfigSchema = z.object({
  events: z.array(eventConfigSchema).min(1, 'Each topic must define at least one event.'),
  keySchemaPath: nonEmptyStringSchema.optional(),
  name: nonEmptyStringSchema,
  subjectStrategy: subjectNameStrategySchema.optional(),
  sync: z
    .object({
      configEntries: z.record(nonEmptyStringSchema, nonEmptyStringSchema).optional(),
      partitions: positiveIntegerSchema.optional(),
      replicationFactor: positiveIntegerSchema.optional()
    })
    .optional()
});

export const kafkaTypegenConfigSchema = z.object({
  generation: z
    .object({
      clientName: nonEmptyStringSchema.optional(),
      packageName: nonEmptyStringSchema.optional(),
      typesFileName: nonEmptyStringSchema.optional()
    })
    .optional(),
  naming: z.object({ eventTypeSuffix: nonEmptyStringSchema.optional(), topicTypeSuffix: nonEmptyStringSchema.optional() }).optional(),
  outputDir: nonEmptyStringSchema,
  runtime: z.object({ module: nonEmptyStringSchema.optional(), transport: runtimeTransportSchema.optional() }).optional(),
  schemaRegistry: z.object({ subjectStrategy: subjectNameStrategySchema.optional(), url: nonEmptyStringSchema }).optional(),
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
            .optional(),
          ssl: z.boolean().optional()
        })
        .optional(),
      schemaRegistry: z
        .object({
          failOnDrift: z.boolean().optional(),
          password: nonEmptyStringSchema.optional(),
          url: nonEmptyStringSchema.optional(),
          username: nonEmptyStringSchema.optional()
        })
        .optional()
    })
    .optional(),
  sources: z.object({ rootDir: nonEmptyStringSchema.optional() }).optional(),
  topics: z.array(topicConfigSchema).min(1, 'At least one topic must be configured.')
});
