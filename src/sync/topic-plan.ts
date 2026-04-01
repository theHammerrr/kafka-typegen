import type { EventCatalog } from '../catalog/index.js';

import type { DesiredKafkaTopic } from './types.js';

export function buildKafkaTopicPlan(catalog: EventCatalog): readonly DesiredKafkaTopic[] {
  return catalog.config.topics.map((topic) => ({
    configEntries: topic.sync.configEntries,
    partitions: topic.sync.partitions,
    replicationFactor: topic.sync.replicationFactor,
    topicName: topic.topicName
  }));
}
