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
          compressionGzipLevel: 5,
          compressionLz4Level: 9,
          compressionType: 'lz4',
          compressionZstdLevel: 3,
          deleteRetentionMs: 120000,
          fileDeleteDelayMs: 30000,
          flushMessages: 1000,
          flushMs: 500,
          followerReplicationThrottledReplicas: ['0:1', '1:2'],
          indexIntervalBytes: 4096,
          leaderReplicationThrottledReplicas: '*',
          localRetentionBytes: -2,
          localRetentionMs: -2,
          maxCompactionLagMs: 7200000,
          maxMessageBytes: 1048576,
          messageTimestampAfterMaxMs: 3600000,
          messageTimestampBeforeMaxMs: 3600000,
          messageTimestampType: 'CreateTime',
          minCleanableDirtyRatio: 0.5,
          minCompactionLagMs: 60000,
          minInSyncReplicas: 2,
          partitions: 3,
          preallocate: true,
          remoteLogCopyDisable: false,
          remoteLogDeleteOnDisable: true,
          remoteStorageEnable: false,
          replicationFactor: 2,
          retentionBytes: 10485760,
          retentionMs: 86400000,
          segmentBytes: 1073741824,
          segmentIndexBytes: 10485760,
          segmentJitterMs: 1000,
          segmentMs: 604800000,
          uncleanLeaderElectionEnable: false
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
          'compression.gzip.level': '5',
          'compression.lz4.level': '9',
          'compression.type': 'lz4',
          'compression.zstd.level': '3',
          'delete.retention.ms': '120000',
          'file.delete.delay.ms': '30000',
          'flush.messages': '1000',
          'flush.ms': '500',
          'follower.replication.throttled.replicas': '0:1,1:2',
          'index.interval.bytes': '4096',
          'leader.replication.throttled.replicas': '*',
          'local.retention.bytes': '-2',
          'local.retention.ms': '-2',
          'max.compaction.lag.ms': '7200000',
          'max.message.bytes': '1048576',
          'message.timestamp.after.max.ms': '3600000',
          'message.timestamp.before.max.ms': '3600000',
          'message.timestamp.type': 'CreateTime',
          'min.cleanable.dirty.ratio': '0.5',
          'min.compaction.lag.ms': '60000',
          'min.insync.replicas': '2',
          preallocate: 'true',
          'remote.log.copy.disable': 'false',
          'remote.log.delete.on.disable': 'true',
          'remote.storage.enable': 'false',
          'retention.bytes': '10485760',
          'retention.ms': '86400000',
          'segment.bytes': '1073741824',
          'segment.index.bytes': '10485760',
          'segment.jitter.ms': '1000',
          'segment.ms': '604800000',
          'unclean.leader.election.enable': 'false'
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
        },
        async updateSubjectCompatibility() {
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
        async registerSubject() {},
        async updateSubjectCompatibility() {}
      }
    };

    const result = await executeSync(
      { catalog, config, options: { apply: false, json: false, target: 'all' } },
      { apply: false, clients, config, target: 'all' }
    );

    expect(result.operations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ action: 'drift', target: 'kafka' }),
        expect.objectContaining({ action: 'update', target: 'registry' })
      ])
    );
  });

  it('registers a new subject version on schema drift during apply by default', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const config = createConfig();
    const catalog = await createCatalogBuilder().build(config);
    const registeredSubjects: string[] = [];
    const clients: SyncClients = {
      schemaRegistry: {
        async getLatestSubject(subjectName) {
          return { schemaText: '{}', subjectName };
        },
        async registerSubject(subject) {
          registeredSubjects.push(subject.subjectName);
        },
        async updateSubjectCompatibility() {}
      }
    };

    const result = await executeSync(
      { catalog, config, options: { apply: true, json: false, target: 'registry' } },
      { apply: true, clients, config, target: 'registry' }
    );

    expect(registeredSubjects).toEqual(['user.events-user.created']);
    expect(result.operations).toEqual([
      expect.objectContaining({ action: 'update', target: 'registry' })
    ]);
  });

  it('fails on subject drift when schemaRegistry.onDrift is fail', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const config = resolveConfig({
      outputDir: './generated',
      schemaRegistry: {
        url: 'http://localhost:8081'
      },
      sync: {
        schemaRegistry: {
          onDrift: 'fail'
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
          name: 'user.events'
        }
      ]
    });
    const catalog = await createCatalogBuilder().build(config);
    const clients: SyncClients = {
      schemaRegistry: {
        async getLatestSubject(subjectName) {
          return { schemaText: '{}', subjectName };
        },
        async registerSubject() {},
        async updateSubjectCompatibility() {}
      }
    };

    await expect(
      executeSync(
        { catalog, config, options: { apply: false, json: false, target: 'registry' } },
        { apply: false, clients, config, target: 'registry' }
      )
    ).rejects.toThrowError(
      'Schema Registry sync detected subject drift and sync.schemaRegistry.onDrift is set to fail.'
    );
  });

  it('reports subject drift without registering when schemaRegistry.onDrift is ignore', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const config = resolveConfig({
      outputDir: './generated',
      schemaRegistry: {
        url: 'http://localhost:8081'
      },
      sync: {
        schemaRegistry: {
          onDrift: 'ignore'
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
          name: 'user.events'
        }
      ]
    });
    const catalog = await createCatalogBuilder().build(config);
    let registerCount = 0;
    const clients: SyncClients = {
      schemaRegistry: {
        async getLatestSubject(subjectName) {
          return { schemaText: '{}', subjectName };
        },
        async registerSubject() {
          registerCount += 1;
        },
        async updateSubjectCompatibility() {}
      }
    };

    const result = await executeSync(
      { catalog, config, options: { apply: true, json: false, target: 'registry' } },
      { apply: true, clients, config, target: 'registry' }
    );

    expect(registerCount).toBe(0);
    expect(result.operations).toEqual([
      expect.objectContaining({ action: 'drift', target: 'registry' })
    ]);
  });

  it('executes only the selected sync target', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const config = createConfig();
    const catalog = await createCatalogBuilder().build(config);
    let kafkaCalls = 0;
    let registryCalls = 0;
    const clients: SyncClients = {
      kafkaAdmin: {
        async createTopics() {
          kafkaCalls += 1;
        },
        async listTopics() {
          kafkaCalls += 1;
          return [];
        }
      },
      schemaRegistry: {
        async getLatestSubject() {
          registryCalls += 1;
          return undefined;
        },
        async registerSubject() {
          registryCalls += 1;
        },
        async updateSubjectCompatibility() {
          registryCalls += 1;
        }
      }
    };

    const kafkaOnlyResult = await executeSync(
      { catalog, config, options: { apply: false, json: false, target: 'kafka' } },
      { apply: false, clients, config, target: 'kafka' }
    );
    expect(kafkaOnlyResult.operations).toEqual([
      expect.objectContaining({ target: 'kafka' })
    ]);
    expect(kafkaCalls).toBeGreaterThan(0);
    expect(registryCalls).toBe(0);

    kafkaCalls = 0;
    const registryOnlyResult = await executeSync(
      { catalog, config, options: { apply: false, json: false, target: 'registry' } },
      { apply: false, clients, config, target: 'registry' }
    );
    expect(registryOnlyResult.operations).toEqual([
      expect.objectContaining({ target: 'registry' })
    ]);
    expect(kafkaCalls).toBe(0);
    expect(registryCalls).toBeGreaterThan(0);
  });

  it('fails clearly when a selected sync target has no configured client', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const config = createConfig();
    const catalog = await createCatalogBuilder().build(config);

    await expect(
      executeSync(
        { catalog, config, options: { apply: false, json: false, target: 'kafka' } },
        { apply: false, clients: {}, config, target: 'kafka' }
      )
    ).rejects.toThrowError('Kafka sync client is not configured.');

    await expect(
      executeSync(
        { catalog, config, options: { apply: false, json: false, target: 'registry' } },
        { apply: false, clients: {}, config, target: 'registry' }
      )
    ).rejects.toThrowError('Schema Registry sync client is not configured.');
  });

  it('applies configured subject compatibility before registering a new schema version', async () => {
    const { createCatalogBuilder } = await import('../src/catalog/index.js');
    const config = resolveConfig({
      outputDir: './generated',
      schemaRegistry: {
        url: 'http://localhost:8081'
      },
      sync: {
        schemaRegistry: {
          compatibility: 'BACKWARD'
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
          name: 'user.events'
        }
      ]
    });
    const catalog = await createCatalogBuilder().build(config);
    const calls: string[] = [];
    const clients: SyncClients = {
      schemaRegistry: {
        async getLatestSubject(subjectName) {
          return { schemaText: '{}', subjectName };
        },
        async registerSubject(subject) {
          calls.push(`register:${subject.subjectName}`);
        },
        async updateSubjectCompatibility(subjectName, compatibility) {
          calls.push(`compatibility:${subjectName}:${compatibility}`);
        }
      }
    };

    const result = await executeSync(
      { catalog, config, options: { apply: true, json: false, target: 'registry' } },
      { apply: true, clients, config, target: 'registry' }
    );

    expect(calls).toEqual([
      'compatibility:user.events-user.created:BACKWARD',
      'register:user.events-user.created'
    ]);
    expect(result.operations).toEqual([
      expect.objectContaining({
        action: 'update',
        details:
          "Registered a new schema version for event 'user.created'. Compatibility BACKWARD was applied."
      })
    ]);
  });
});
