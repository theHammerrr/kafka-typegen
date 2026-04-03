import type { EventCatalog } from '../catalog/index.js';
import type { NormalizedTopicSyncConfig } from '../config/index.js';

import type { DesiredKafkaTopic } from './types.js';

const topicConfigKeys = {
  cleanupPolicy: 'cleanup.policy',
  compressionGzipLevel: 'compression.gzip.level',
  compressionLz4Level: 'compression.lz4.level',
  compressionType: 'compression.type',
  compressionZstdLevel: 'compression.zstd.level',
  deleteRetentionMs: 'delete.retention.ms',
  fileDeleteDelayMs: 'file.delete.delay.ms',
  flushMessages: 'flush.messages',
  flushMs: 'flush.ms',
  followerReplicationThrottledReplicas:
    'follower.replication.throttled.replicas',
  indexIntervalBytes: 'index.interval.bytes',
  leaderReplicationThrottledReplicas:
    'leader.replication.throttled.replicas',
  localRetentionBytes: 'local.retention.bytes',
  localRetentionMs: 'local.retention.ms',
  maxCompactionLagMs: 'max.compaction.lag.ms',
  maxMessageBytes: 'max.message.bytes',
  messageTimestampAfterMaxMs: 'message.timestamp.after.max.ms',
  messageTimestampBeforeMaxMs: 'message.timestamp.before.max.ms',
  messageTimestampType: 'message.timestamp.type',
  minCleanableDirtyRatio: 'min.cleanable.dirty.ratio',
  minCompactionLagMs: 'min.compaction.lag.ms',
  minInSyncReplicas: 'min.insync.replicas',
  preallocate: 'preallocate',
  remoteLogCopyDisable: 'remote.log.copy.disable',
  remoteLogDeleteOnDisable: 'remote.log.delete.on.disable',
  remoteStorageEnable: 'remote.storage.enable',
  retentionBytes: 'retention.bytes',
  retentionMs: 'retention.ms',
  segmentBytes: 'segment.bytes',
  segmentIndexBytes: 'segment.index.bytes',
  segmentJitterMs: 'segment.jitter.ms',
  segmentMs: 'segment.ms',
  uncleanLeaderElectionEnable: 'unclean.leader.election.enable'
} satisfies Record<
  Exclude<keyof NormalizedTopicSyncConfig, 'partitions' | 'replicationFactor'>,
  string
>;

function buildConfigEntries(
  sync: NonNullable<EventCatalog['config']['topics'][number]['sync']>
): Readonly<Record<string, string>> {
  return Object.fromEntries(
    Object.entries(topicConfigKeys).flatMap(([propertyName, configName]) => {
      const value = sync[propertyName as keyof typeof topicConfigKeys];
      if (value === undefined) {
        return [];
      }

      return [
        [
          configName,
          Array.isArray(value) ? value.join(',') : String(value)
        ]
      ];
    })
  );
}

export function buildKafkaTopicPlan(catalog: EventCatalog): readonly DesiredKafkaTopic[] {
  return catalog.config.topics.flatMap((topic) =>
    topic.sync === undefined
      ? []
      : [
          {
            configEntries: buildConfigEntries(topic.sync),
            partitions: topic.sync.partitions,
            replicationFactor: topic.sync.replicationFactor,
            topicName: topic.topicName
          }
        ]
  );
}
