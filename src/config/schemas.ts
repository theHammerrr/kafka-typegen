import { z } from 'zod';

const nonEmptyStringSchema = z.string().trim().min(1, 'Expected a non-empty string.');

export const subjectNameStrategySchema = z.enum(['event-name', 'topic-name', 'topic-event']);
export const runtimeTransportSchema = z.enum(['kafkajs', '@platformatic/kafka']);

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
  subjectStrategy: subjectNameStrategySchema.optional()
});

export const kafkaTypegenConfigSchema = z.object({
  generation: z.object({ clientName: nonEmptyStringSchema.optional(), typesFileName: nonEmptyStringSchema.optional() }).optional(),
  naming: z.object({ eventTypeSuffix: nonEmptyStringSchema.optional(), topicTypeSuffix: nonEmptyStringSchema.optional() }).optional(),
  outputDir: nonEmptyStringSchema,
  runtime: z.object({ module: nonEmptyStringSchema.optional(), transport: runtimeTransportSchema.optional() }).optional(),
  schemaRegistry: z.object({ subjectStrategy: subjectNameStrategySchema.optional(), url: nonEmptyStringSchema }).optional(),
  sources: z.object({ rootDir: nonEmptyStringSchema.optional() }).optional(),
  topics: z.array(topicConfigSchema).min(1, 'At least one topic must be configured.')
});
