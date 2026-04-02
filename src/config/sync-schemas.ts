import { z } from 'zod';

export const cleanupPolicySchema = z.enum(['delete', 'compact', 'compact,delete']);
export const compressionTypeSchema = z.enum([
  'producer',
  'uncompressed',
  'gzip',
  'snappy',
  'lz4',
  'zstd'
]);
export const saslMechanismSchema = z.enum([
  'plain',
  'scram-sha-256',
  'scram-sha-512'
]);

export function createSchemaRegistryAuthSchema(
  nonEmptyStringSchema: z.ZodString
) {
  return z
    .object({
      password: nonEmptyStringSchema.optional(),
      token: nonEmptyStringSchema.optional(),
      username: nonEmptyStringSchema.optional()
    })
    .strict();
}

export function createTopicSyncConfigSchema(
  nonNegativeIntegerSchema: z.ZodNumber,
  positiveIntegerSchema: z.ZodNumber
) {
  return z
    .object({
      cleanupPolicy: cleanupPolicySchema.optional(),
      compressionType: compressionTypeSchema.optional(),
      maxMessageBytes: positiveIntegerSchema.optional(),
      minCompactionLagMs: nonNegativeIntegerSchema.optional(),
      partitions: positiveIntegerSchema,
      replicationFactor: positiveIntegerSchema,
      retentionBytes: positiveIntegerSchema.optional(),
      retentionMs: positiveIntegerSchema.optional()
    })
    .strict();
}
