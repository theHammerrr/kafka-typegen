import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  ConfigValidationError,
  defineConfig,
  resolveConfig,
  validateConfig,
  type KafkaTypegenConfig
} from '../src/index.js';

describe('config validation', () => {
  it('accepts a valid single-event topic config', () => {
    const config = defineConfig({
      outputDir: './generated',
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './schemas/user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    } satisfies KafkaTypegenConfig);

    expect(validateConfig(config)).toEqual(config);
  });

  it('accepts a valid multi-event topic config', () => {
    const config = defineConfig({
      outputDir: './generated',
      schemaRegistry: {
        subjectStrategy: 'topic-name',
        url: 'http://localhost:8081'
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './schemas/user-created.avsc'
            },
            {
              keySchemaPath: './schemas/user-updated-key.avsc',
              name: 'user.updated',
              schemaPath: './schemas/user-updated.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    } satisfies KafkaTypegenConfig);

    expect(validateConfig(config)).toEqual(config);
  });

  it('accepts @platformatic/kafka as a runtime transport', () => {
    const config = defineConfig({
      outputDir: './generated',
      runtime: {
        transport: '@platformatic/kafka'
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './schemas/user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    } satisfies KafkaTypegenConfig);

    expect(validateConfig(config)).toEqual(config);
  });

  it('accepts sync configuration for kafka topics and schema registry', () => {
    const config = defineConfig({
      outputDir: './generated',
      schemaRegistry: {
        url: 'http://localhost:8081'
      },
      sync: {
        kafka: {
          brokers: ['localhost:9092']
        },
        schemaRegistry: {
          failOnDrift: true
        }
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './schemas/user-created.avsc'
            }
          ],
          name: 'user.events',
          sync: {
            partitions: 3,
            replicationFactor: 2
          }
        }
      ]
    } satisfies KafkaTypegenConfig);

    expect(validateConfig(config)).toEqual(config);
  });

  it('rejects duplicate event names across topics', () => {
    expect(() =>
      validateConfig({
        outputDir: './generated',
        topics: [
          {
            events: [
              {
                name: 'user.created',
                schemaPath: './schemas/user-created.avsc'
              }
            ],
            name: 'user.events'
          },
          {
            events: [
              {
                name: 'user.created',
                schemaPath: './schemas/audit-user-created.avsc'
              }
            ],
            name: 'audit.events'
          }
        ]
      })
    ).toThrowError(ConfigValidationError);

    try {
      validateConfig({
        outputDir: './generated',
        topics: [
          {
            events: [
              {
                name: 'user.created',
                schemaPath: './schemas/user-created.avsc'
              }
            ],
            name: 'user.events'
          },
          {
            events: [
              {
                name: 'user.created',
                schemaPath: './schemas/audit-user-created.avsc'
              }
            ],
            name: 'audit.events'
          }
        ]
      });
    } catch (error) {
      expect(error).toBeInstanceOf(ConfigValidationError);

      const validationError = error as ConfigValidationError;

      expect(validationError.issues).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ path: 'topics[1].events[0].name' })
        ])
      );
    }
  });

  it('rejects missing schema paths', () => {
    expect(() =>
      validateConfig({
        outputDir: './generated',
        topics: [
          {
            events: [
              {
                name: 'user.created',
                schemaPath: ''
              }
            ],
            name: 'user.events'
          }
        ]
      })
    ).toThrowError(ConfigValidationError);
  });

  it('rejects invalid subject strategies with exact paths', () => {
    expect(() =>
      validateConfig({
        outputDir: './generated',
        schemaRegistry: {
          subjectStrategy: 'invalid',
          url: 'http://localhost:8081'
        },
        topics: [
          {
            events: [
              {
                name: 'user.created',
                schemaPath: './schemas/user-created.avsc'
              }
            ],
            name: 'user.events'
          }
        ]
      })
    ).toThrowError(ConfigValidationError);

    try {
      validateConfig({
        outputDir: './generated',
        schemaRegistry: {
          subjectStrategy: 'invalid',
          url: 'http://localhost:8081'
        },
        topics: [
          {
            events: [
              {
                name: 'user.created',
                schemaPath: './schemas/user-created.avsc'
              }
            ],
            name: 'user.events'
          }
        ]
      });
    } catch (error) {
      const validationError = error as ConfigValidationError;

      expect(validationError.issues).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ path: 'schemaRegistry.subjectStrategy' })
        ])
      );
    }
  });
});

describe('config normalization', () => {
  it('produces deterministic normalized output with derived metadata', () => {
    const normalized = resolveConfig({
      generation: {
        clientName: 'AppClient',
        packageName: '@acme/generated-kafka',
        typesFileName: 'types.ts'
      },
      naming: {
        eventTypeSuffix: 'EventPayload',
        topicTypeSuffix: 'Stream'
      },
      outputDir: './generated',
      runtime: {
        module: './runtime/custom',
        transport: 'kafkajs'
      },
      schemaRegistry: {
        subjectStrategy: 'topic-name',
        url: 'http://localhost:8081'
      },
      sources: {
        rootDir: './fixtures'
      },
      topics: [
        {
          events: [
            {
              name: 'user.updated',
              schemaPath: './schemas/user-updated.avsc'
            }
          ],
          name: 'z.topic'
        },
        {
          events: [
            {
              keySchemaPath: './schemas/user-created-key.avsc',
              name: 'user.created',
              schemaPath: './schemas/user-created.avsc',
              subject: 'custom-user-created-subject'
            },
            {
              name: 'user.deleted',
              schemaPath: './schemas/user-deleted.avsc'
            }
          ],
          keySchemaPath: './schemas/topic-key.avsc',
          name: 'a.topic',
          subjectStrategy: 'topic-event'
        }
      ]
    });

    expect(normalized.topics.map((topic) => topic.topicName)).toEqual(['a.topic', 'z.topic']);
    expect(normalized.events.map((event) => event.eventName)).toEqual([
      'user.created',
      'user.deleted',
      'user.updated'
    ]);
    expect(normalized.sources.rootDir).toBe(resolvePath('./fixtures'));
    expect(normalized.resolvedOutputDir).toBe(resolvePath('./generated'));
    expect(normalized.generation).toEqual({
      clientName: 'AppClient',
      packageName: '@acme/generated-kafka',
      typesFileName: 'types.ts'
    });
    expect(normalized.naming).toEqual({
      eventTypeSuffix: 'EventPayload',
      topicTypeSuffix: 'Stream'
    });
    expect(normalized.events[0]).toMatchObject({
      eventName: 'user.created',
      keySchemaPath: './schemas/user-created-key.avsc',
      resolvedKeySchemaPath: resolvePath('./fixtures', './schemas/user-created-key.avsc'),
      resolvedSchemaPath: resolvePath('./fixtures', './schemas/user-created.avsc'),
      subjectName: 'custom-user-created-subject',
      topicName: 'a.topic'
    });
    expect(normalized.events[1]).toMatchObject({
      eventName: 'user.deleted',
      keySchemaPath: './schemas/topic-key.avsc',
      resolvedKeySchemaPath: resolvePath('./fixtures', './schemas/topic-key.avsc'),
      subjectName: 'a.topic-user.deleted'
    });
    expect(normalized.events[2]).toMatchObject({
      eventName: 'user.updated',
      subjectName: 'z.topic'
    });
  });

  it('defaults runtime modules by transport and preserves explicit overrides', () => {
    const defaultKafkaJsConfig = resolveConfig({
      outputDir: './generated',
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './schemas/user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const defaultPlatformaticConfig = resolveConfig({
      outputDir: './generated',
      runtime: {
        transport: '@platformatic/kafka'
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './schemas/user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const overriddenPlatformaticConfig = resolveConfig({
      outputDir: './generated',
      runtime: {
        module: './runtime/custom-platformatic',
        transport: '@platformatic/kafka'
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './schemas/user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    expect(defaultKafkaJsConfig.runtime).toEqual({
      module: 'kafka-typegen/runtime',
      transport: 'kafkajs'
    });
    expect(defaultPlatformaticConfig.runtime).toEqual({
      module: 'kafka-typegen/runtime/platformatic',
      transport: '@platformatic/kafka'
    });
    expect(overriddenPlatformaticConfig.runtime).toEqual({
      module: './runtime/custom-platformatic',
      transport: '@platformatic/kafka'
    });
  });

  it('normalizes sync defaults and inherits schema registry url for sync', () => {
    const normalized = resolveConfig({
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
              schemaPath: './schemas/user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    expect(normalized.sync).toEqual({
      kafka: {
        brokers: ['localhost:9092'],
        clientId: 'kafka-typegen-sync',
        failOnDrift: false,
        ssl: false
      },
      schemaRegistry: {
        failOnDrift: false,
        url: 'http://localhost:8081'
      }
    });
    expect(normalized.topics[0]?.sync).toEqual({
      configEntries: {},
      partitions: 1,
      replicationFactor: 1
    });
  });
});
