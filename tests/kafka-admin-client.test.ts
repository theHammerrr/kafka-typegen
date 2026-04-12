import { beforeEach, describe, expect, it, vi } from 'vitest';

const { adminClient, kafkaConfigs } = vi.hoisted(() => ({
  adminClient: {
    connect: vi.fn(async () => {}),
    createTopics: vi.fn(async () => {}),
    disconnect: vi.fn(async () => {}),
    fetchTopicMetadata: vi.fn(async () => ({ topics: [] }))
  },
  kafkaConfigs: [] as unknown[]
}));

vi.mock('kafkajs', () => ({
  Kafka: class Kafka {
    public constructor(config: unknown) {
      kafkaConfigs.push(config);
    }

    public admin() {
      return adminClient;
    }
  }
}));

describe('KafkaJsAdminClient', () => {
  beforeEach(() => {
    kafkaConfigs.length = 0;
    vi.clearAllMocks();
  });

  it('passes SSL and SASL config to KafkaJS', async () => {
    const { KafkaJsAdminClient } = await import(
      '../src/sync/kafka-admin-client.js'
    );
    const client = new KafkaJsAdminClient({
      brokers: ['localhost:9092'],
      clientId: 'sync-client',
      failOnDrift: false,
      sasl: {
        mechanism: 'scram-sha-512',
        password: 'secret',
        username: 'sync-user'
      },
      ssl: true
    });

    await client.listTopics();

    expect(kafkaConfigs).toEqual([
      {
        brokers: ['localhost:9092'],
        clientId: 'sync-client',
        sasl: {
          mechanism: 'scram-sha-512',
          password: 'secret',
          username: 'sync-user'
        },
        ssl: true
      }
    ]);
    expect(adminClient.connect).toHaveBeenCalledTimes(1);
    expect(adminClient.disconnect).toHaveBeenCalledTimes(1);
  });
});
