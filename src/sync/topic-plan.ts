import type { EventCatalog } from '../catalog/index.js';

import type { DesiredKafkaTopic } from './types.js';

function buildConfigEntries(
  sync: NonNullable<EventCatalog['config']['topics'][number]['sync']>
): Readonly<Record<string, string>> {
  return {
    ...(sync.cleanupPolicy !== undefined
      ? { 'cleanup.policy': sync.cleanupPolicy }
      : {}),
    ...(sync.compressionType !== undefined
      ? { 'compression.type': sync.compressionType }
      : {}),
    ...(sync.maxMessageBytes !== undefined
      ? { 'max.message.bytes': String(sync.maxMessageBytes) }
      : {}),
    ...(sync.minCompactionLagMs !== undefined
      ? { 'min.compaction.lag.ms': String(sync.minCompactionLagMs) }
      : {}),
    ...(sync.retentionBytes !== undefined
      ? { 'retention.bytes': String(sync.retentionBytes) }
      : {}),
    ...(sync.retentionMs !== undefined
      ? { 'retention.ms': String(sync.retentionMs) }
      : {})
  };
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
