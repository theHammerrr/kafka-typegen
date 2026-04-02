import { z } from 'zod';

export const cleanupPolicySchema = z.enum([
  'compact',
  'compact,delete',
  'delete'
]);
export const compressionTypeSchema = z.enum([
  'producer',
  'uncompressed',
  'gzip',
  'snappy',
  'lz4',
  'zstd'
]);
const messageTimestampTypeSchema = z.enum(['CreateTime', 'LogAppendTime']);
export const saslMechanismSchema = z.enum([
  'plain',
  'scram-sha-256',
  'scram-sha-512'
]);
const nonEmptyStringSchema = z.string().trim().min(1, 'Expected a non-empty string.');
const throttledReplicasSchema = z.union([
  nonEmptyStringSchema,
  z.array(nonEmptyStringSchema).min(1, 'Expected at least one replica entry.')
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
      compressionGzipLevel: z.int().min(-1).max(9).optional(),
      compressionLz4Level: z.int().min(1).max(17).optional(),
      compressionType: compressionTypeSchema.optional(),
      compressionZstdLevel: z.int().min(-131072).max(22).optional(),
      deleteRetentionMs: nonNegativeIntegerSchema.optional(),
      fileDeleteDelayMs: nonNegativeIntegerSchema.optional(),
      flushMessages: nonNegativeIntegerSchema.optional(),
      flushMs: nonNegativeIntegerSchema.optional(),
      followerReplicationThrottledReplicas: throttledReplicasSchema.optional(),
      indexIntervalBytes: nonNegativeIntegerSchema.optional(),
      leaderReplicationThrottledReplicas: throttledReplicasSchema.optional(),
      localRetentionBytes: z.int().min(-2, 'Expected an integer greater than or equal to -2.').optional(),
      localRetentionMs: z.int().min(-2, 'Expected an integer greater than or equal to -2.').optional(),
      maxCompactionLagMs: positiveIntegerSchema.optional(),
      maxMessageBytes: positiveIntegerSchema.optional(),
      messageTimestampAfterMaxMs: nonNegativeIntegerSchema.optional(),
      messageTimestampBeforeMaxMs: nonNegativeIntegerSchema.optional(),
      messageTimestampType: messageTimestampTypeSchema.optional(),
      minCleanableDirtyRatio: z.number().min(0).max(1).optional(),
      minCompactionLagMs: nonNegativeIntegerSchema.optional(),
      minInSyncReplicas: positiveIntegerSchema.optional(),
      partitions: positiveIntegerSchema,
      preallocate: z.boolean().optional(),
      remoteLogCopyDisable: z.boolean().optional(),
      remoteLogDeleteOnDisable: z.boolean().optional(),
      remoteStorageEnable: z.boolean().optional(),
      replicationFactor: positiveIntegerSchema,
      retentionBytes: z.int().min(-1, 'Expected an integer greater than or equal to -1.').optional(),
      retentionMs: z.int().min(-1, 'Expected an integer greater than or equal to -1.').optional(),
      segmentBytes: z.int().min(1048576, 'Expected an integer greater than or equal to 1048576.').optional(),
      segmentIndexBytes: z.int().min(4, 'Expected an integer greater than or equal to 4.').optional(),
      segmentJitterMs: nonNegativeIntegerSchema.optional(),
      segmentMs: positiveIntegerSchema.optional(),
      uncleanLeaderElectionEnable: z.boolean().optional()
    })
    .strict();
}
