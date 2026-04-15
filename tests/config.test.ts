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

  it('accepts Avro external type mappings and semantic rendering mode', () => {
    const config = defineConfig({
      generation: {
        avroExternalTypes: {
          'com.external.ExternalAddress': "import('./external-types.js').ExternalAddress"
        },
        avroSemanticMode: 'safe'
      },
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
        auth: {
          password: 'registry-password',
          username: 'registry-user'
        },
        url: 'http://localhost:8081'
      },
      sync: {
        kafka: {
          brokers: ['localhost:9092']
        },
        schemaRegistry: {
          compatibility: 'FULL',
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
            cleanupPolicy: 'compact',
            compressionType: 'lz4',
            maxMessageBytes: 1_048_576,
            minCompactionLagMs: 60000,
            partitions: 3,
            replicationFactor: 2,
            retentionBytes: 10_485_760,
            retentionMs: 86_400_000
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

  it('rejects legacy sync schema registry connection fields', () => {
    expect(() =>
      validateConfig({
        outputDir: './generated',
        sync: {
          schemaRegistry: {
            url: 'http://localhost:8081'
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
      })
    ).toThrowError(ConfigValidationError);
  });

  it('rejects removed generation options', () => {
    expect(() =>
      validateConfig({
        generation: {
          clientName: 'AppClient'
        },
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
      })
    ).toThrowError(ConfigValidationError);

    expect(() =>
      validateConfig({
        generation: {
          packageName: '@acme/generated-kafka'
        },
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
      })
    ).toThrowError(ConfigValidationError);

    try {
      validateConfig({
        generation: {
          packageName: '@acme/generated-kafka'
        },
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
    } catch (error) {
      const validationError = error as ConfigValidationError;

      expect(validationError.issues).toEqual([
        {
          message: 'Unrecognized key: "packageName"',
          path: 'generation'
        }
      ]);
    }
  });

  it('rejects Avro external type mappings that reuse the same TypeScript type expression', () => {
    try {
      validateConfig({
        generation: {
          avroExternalTypes: {
            'com.external.Address': "import('./types.js').SharedAddress",
            'com.external.BillingAddress': "import('./types.js').SharedAddress"
          }
        },
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
    } catch (error) {
      const validationError = error as ConfigValidationError;

      expect(validationError.issues).toEqual([
        {
          message: "TypeScript type 'import('./types.js').SharedAddress' is mapped from both 'com.external.Address' and 'com.external.BillingAddress'.",
          path: 'generation.avroExternalTypes'
        }
      ]);
      return;
    }

    throw new Error('Expected validation to fail for duplicate external type mappings.');
  });

  it('requires topic sync config for every topic when kafka sync is enabled', () => {
    expect(() =>
      validateConfig({
        outputDir: './generated',
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
      })
    ).toThrowError(ConfigValidationError);
  });

  it('requires top-level schema registry config when schema registry sync policy is enabled', () => {
    expect(() =>
      validateConfig({
        outputDir: './generated',
        sync: {
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
            name: 'user.events'
          }
        ]
      })
    ).toThrowError(ConfigValidationError);
  });

  it('accepts schema registry sync drift and compatibility policy', () => {
    const config = defineConfig({
      outputDir: './generated',
      schemaRegistry: {
        url: 'http://localhost:8081'
      },
      sync: {
        schemaRegistry: {
          compatibility: 'BACKWARD_TRANSITIVE',
          onDrift: 'register'
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
    } satisfies KafkaTypegenConfig);

    expect(validateConfig(config)).toEqual(config);
  });

  it('accepts every schema registry compatibility policy', () => {
    const compatibilities = [
      'BACKWARD',
      'BACKWARD_TRANSITIVE',
      'FORWARD',
      'FORWARD_TRANSITIVE',
      'FULL',
      'FULL_TRANSITIVE',
      'NONE'
    ] as const;

    for (const compatibility of compatibilities) {
      expect(
        validateConfig({
          outputDir: './generated',
          schemaRegistry: {
            url: 'http://localhost:8081'
          },
          sync: {
            schemaRegistry: {
              compatibility,
              onDrift: 'register'
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
        })
      ).toMatchObject({
        sync: {
          schemaRegistry: {
            compatibility
          }
        }
      });
    }
  });

  it('rejects schema registry configs that combine failOnDrift and onDrift', () => {
    expect(() =>
      validateConfig({
        outputDir: './generated',
        schemaRegistry: {
          url: 'http://localhost:8081'
        },
        sync: {
          schemaRegistry: {
            failOnDrift: true,
            onDrift: 'register'
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
      })
    ).toThrowError(ConfigValidationError);
  });
});

describe('config normalization', () => {
  it('produces deterministic normalized output with derived metadata', () => {
    const normalized = resolveConfig({
      generation: {
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
      apiMode: 'minimal',
      avroExternalTypes: {},
      avroSemanticMode: 'default',
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
      module: 'kafka-typegen/runtime/kafkajs',
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

  it('normalizes Avro generation defaults and sorts external type mappings', () => {
    const normalized = resolveConfig({
      generation: {
        avroExternalTypes: {
          'com.zeta.Address': "import('./types.js').ZetaAddress",
          'com.alpha.Address': "import('./types.js').AlphaAddress"
        },
        avroSemanticMode: 'safe'
      },
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

    expect(normalized.generation).toEqual({
      apiMode: 'minimal',
      avroExternalTypes: {
        'com.alpha.Address': "import('./types.js').AlphaAddress",
        'com.zeta.Address': "import('./types.js').ZetaAddress"
      },
      avroSemanticMode: 'safe',
      typesFileName: 'kafka-client.ts'
    });
  });

  it('normalizes sync defaults and inherits schema registry url for sync', () => {
    const normalized = resolveConfig({
      outputDir: './generated',
      schemaRegistry: {
        auth: {
          token: 'registry-token'
        },
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
          name: 'user.events',
          sync: {
            partitions: 1,
            replicationFactor: 1
          }
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
        auth: {
          token: 'registry-token'
        },
        onDrift: 'register',
        url: 'http://localhost:8081'
      }
    });
    expect(normalized.schemaRegistry).toEqual({
      auth: {
        token: 'registry-token'
      },
      subjectStrategy: 'topic-event',
      url: 'http://localhost:8081'
    });
    expect(normalized.topics[0]?.sync).toEqual({
      partitions: 1,
      replicationFactor: 1
    });
  });

  it('preserves registry-only sync without requiring topic sync config', () => {
    const normalized = resolveConfig({
      outputDir: './generated',
      schemaRegistry: {
        url: 'http://localhost:8081'
      },
      sync: {
        schemaRegistry: {
          compatibility: 'FORWARD',
          onDrift: 'fail'
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

    expect(normalized.sync?.schemaRegistry).toEqual({
      compatibility: 'FORWARD',
      onDrift: 'fail',
      url: 'http://localhost:8081'
    });
    expect(normalized.topics[0]?.sync).toBeUndefined();
  });

  it('maps legacy sync.schemaRegistry.failOnDrift to onDrift fail', () => {
    const normalized = resolveConfig({
      outputDir: './generated',
      schemaRegistry: {
        url: 'http://localhost:8081'
      },
      sync: {
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
          name: 'user.events'
        }
      ]
    });

    expect(normalized.sync?.schemaRegistry).toEqual({
      onDrift: 'fail',
      url: 'http://localhost:8081'
    });
  });
});
