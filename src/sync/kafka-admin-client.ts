import { Kafka } from 'kafkajs';

import type { NormalizedSyncKafkaConfig } from '../config/index.js';

import { toKafkaJsSasl } from './kafka-sasl.js';
import type { DesiredKafkaTopic, KafkaAdminClient, RemoteKafkaTopic } from './types.js';

export class KafkaJsAdminClient implements KafkaAdminClient {
  public constructor(private readonly config: NormalizedSyncKafkaConfig) {}

  public async createTopics(topics: readonly DesiredKafkaTopic[]): Promise<void> {
    const admin = await this.connect();

    try {
      await admin.createTopics({
        topics: topics.map((topic) => ({
          configEntries: Object.entries(topic.configEntries).map(([name, value]) => ({ name, value })),
          numPartitions: topic.partitions,
          replicationFactor: topic.replicationFactor,
          topic: topic.topicName
        }))
      });
    } finally {
      await admin.disconnect();
    }
  }

  public async listTopics(): Promise<readonly RemoteKafkaTopic[]> {
    const admin = await this.connect();

    try {
      const metadata = await admin.fetchTopicMetadata();
      return metadata.topics.map((topic) => ({
        partitions: topic.partitions.length,
        replicationFactor: topic.partitions[0]?.replicas.length ?? 0,
        topicName: topic.name
      }));
    } finally {
      await admin.disconnect();
    }
  }

  private async connect() {
    const kafka = new Kafka({
      brokers: [...this.config.brokers],
      clientId: this.config.clientId,
      ssl: this.config.ssl,
      ...(this.config.sasl !== undefined ? { sasl: toKafkaJsSasl(this.config.sasl) } : {})
    });
    const admin = kafka.admin();
    await admin.connect();
    return admin;
  }
}
