import { describe, expect, it } from 'vitest';

import { buildKafkaTopicPlan, buildSchemaRegistryPlan, executeSync, resolveConfig, type SyncClients } from '../src/index.js';

function createConfig() {
  return resolveConfig({
    outputDir: './generated',
    schemaRegistry: {
      url: 'http://localhost:8081'
    },
    sync: {
      kafka: {
        brokers: ['localhost:9092']
      }
    },
    topics: [
      {
        events: [
          {
            name: 'user.created',
            schemaPath: './tests/fixtures/schemas/user-created.avsc'
          }
        ],
        name: 'user.events',
        sync: {
          cleanupPolicy: 'delete',
          compressionType: 'lz4',
          maxMessageBytes: 1048576,
          minCompactionLagMs: 60000,
          partitions: 3,
          replicationFactor: 2,
          retentionBytes: 10485760,
          retentionMs: 86400000
        }
      }
    ]
  });
}

describe('sync planning', () => {
  it('builds desired kafka topics from normalized config', () => {
    const config = createConfig();

    expect(buildKafkaTopicPlan({ config, events: [], topics: [] })).toEqual([
      {
        configEntries: {
          'cleanup.policy': 'delete',
          'compression.type': 'lz4',
          'max.message.bytes': '1048576',
          'min.compaction.lag.ms': '60000',
          'retention.bytes': '10485760',
          'retention.ms': '86400000'
        },
        partitions: 3,
        replicationFactor: 2,
        topicName: 'user.events'
      }
    ]);
  });

  it('builds desired registry subjects from the catalog', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const catalog = await createCatalogBuilder().build(createConfig());
    const subjects = buildSchemaRegistryPlan(catalog);

    expect(subjects[0]).toMatchObject({
      eventName: 'user.created',
      subjectName: 'user.events-user.created',
      topicName: 'user.events'
    });
    expect(subjects[0]?.schemaText).toContain('"name":"UserCreated"');
  });
});

describe('sync execution', () => {
  it('creates missing topics and registry subjects during apply', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const config = createConfig();
    const catalog = await createCatalogBuilder().build(config);
    const createdTopics: string[] = [];
    const createdSubjects: string[] = [];
    const clients: SyncClients = {
      kafkaAdmin: {
        async createTopics(topics) {
          createdTopics.push(...topics.map((topic) => topic.topicName));
        },
        async listTopics() {
          return [];
        }
      },
      schemaRegistry: {
        async getLatestSubject() {
          return undefined;
        },
        async registerSubject(subject) {
          createdSubjects.push(subject.subjectName);
        }
      }
    };

    const result = await executeSync(
      { catalog, config, options: { apply: true, json: false, target: 'all' } },
      { apply: true, clients, config, target: 'all' }
    );

    expect(createdTopics).toEqual(['user.events']);
    expect(createdSubjects).toEqual(['user.events-user.created']);
    expect(result.operations.map((operation) => operation.action)).toEqual(['create', 'create']);
  });

  it('reports drift for mismatched existing resources during dry-run', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const config = createConfig();
    const catalog = await createCatalogBuilder().build(config);
    const clients: SyncClients = {
      kafkaAdmin: {
        async createTopics() {},
        async listTopics() {
          return [{ partitions: 1, replicationFactor: 1, topicName: 'user.events' }];
        }
      },
      schemaRegistry: {
        async getLatestSubject(subjectName) {
          return { schemaText: '{}', subjectName };
        },
        async registerSubject() {}
      }
    };

    const result = await executeSync(
      { catalog, config, options: { apply: false, json: false, target: 'all' } },
      { apply: false, clients, config, target: 'all' }
    );

    expect(result.operations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ action: 'drift', target: 'kafka' }),
        expect.objectContaining({ action: 'drift', target: 'registry' })
      ])
    );
  });
});
