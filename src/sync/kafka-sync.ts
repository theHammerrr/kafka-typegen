import type { NormalizedKafkaTypegenConfig } from '../config/index.js';

import type { KafkaAdminClient, SyncOperation } from './types.js';
import { buildKafkaTopicPlan } from './topic-plan.js';

export async function executeKafkaSync(
  config: NormalizedKafkaTypegenConfig,
  kafkaAdmin: KafkaAdminClient,
  apply: boolean
): Promise<readonly SyncOperation[]> {
  const desiredTopics = buildKafkaTopicPlan({ config, events: [], topics: [] });
  const existingTopics = new Map((await kafkaAdmin.listTopics()).map((topic) => [topic.topicName, topic]));
  const createTopics = desiredTopics.filter((topic) => !existingTopics.has(topic.topicName));
  const driftOperations: SyncOperation[] = [];

  for (const topic of desiredTopics) {
    const existing = existingTopics.get(topic.topicName);

    if (existing === undefined) {
      continue;
    }

    driftOperations.push(
      existing.partitions === topic.partitions && existing.replicationFactor === topic.replicationFactor
        ? {
            action: 'noop',
            details: 'Topic already exists with matching inspectable settings.',
            resourceName: topic.topicName,
            target: 'kafka'
          }
        : {
            action: 'drift',
            details: `Existing topic has partitions=${existing.partitions}, replicationFactor=${existing.replicationFactor}.`,
            resourceName: topic.topicName,
            target: 'kafka'
          }
    );
  }

  if (config.sync?.kafka?.failOnDrift === true && driftOperations.some((operation) => operation.action === 'drift')) {
    throw new Error('Kafka sync detected topic drift and sync.kafka.failOnDrift is enabled.');
  }

  if (apply && createTopics.length > 0) {
    await kafkaAdmin.createTopics(createTopics);
  }

  return [
    ...createTopics.map((topic) => ({
      action: 'create',
      details: apply ? 'Topic created.' : 'Topic will be created.',
      resourceName: topic.topicName,
      target: 'kafka'
    }) satisfies SyncOperation),
    ...driftOperations
  ];
}
