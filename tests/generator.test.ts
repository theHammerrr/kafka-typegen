import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createTypeGenerator,
  resolveConfig
} from '../src/index.js';
import { toTypeScriptType } from '../src/generator/avro-type-renderer.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');
async function buildGeneratedOutput(configInput: Parameters<typeof resolveConfig>[0]) {
  const config = resolveConfig(configInput);
  const catalog = await createCatalogBuilder().build(config);
  return createTypeGenerator().generate(catalog);
}

describe('type generation', () => {
  it('emits a compact minimal client for a single-event topic by default', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents = output.files.find((file) => file.filePath === 'kafka-client.ts')?.contents ?? '';

    expect(output.files.map((file) => file.filePath)).toEqual(['kafka-client.ts', 'index.ts']);
    expect(contents).toContain('export interface UserCreatedPayloadMessage {');
    expect(contents).toContain('export interface GeneratedProducerTopics<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> {');
    expect(contents).toContain('userEvents: GeneratedUserEventsProducerTopic<TRuntimeProducer>;');
    expect(contents).toContain('producerMetadataByTopic.userEvents.userCreated');
    expect(contents).not.toContain('export const EventNames = {');
    expect(contents).not.toContain('export const TopicNames = {');
    expect(contents).not.toContain('export interface EventPayloadByName {');
    expect(contents).not.toContain('export const producerEventMetadata');
    expect(contents).toContain([
      'export interface GeneratedUserEventsProducerTopic<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> {',
      '  userCreated: {',
      '    send(payload: UserCreatedPayload, options?: GeneratedProducerSendOptions<TRuntimeProducer>): Promise<void>;',
      '  };',
      '}',
      '',
      'export interface GeneratedProducerTopics<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> {',
      '  userEvents: GeneratedUserEventsProducerTopic<TRuntimeProducer>;',
      '}',
      '',
      'export type GeneratedProducer<TRuntimeProducer extends RuntimeProducer = RuntimeProducer> = TRuntimeProducer & GeneratedProducerTopics<TRuntimeProducer>;',
      '',
      'export function createProducer<TRuntimeProducer extends RuntimeProducer>(runtimeProducer: TRuntimeProducer): GeneratedProducer<TRuntimeProducer> {',
      '  const runtimeSend = runtimeProducer.send.bind(runtimeProducer);',
      '',
      '  return Object.assign(Object.create(runtimeProducer), {',
      '    userEvents: {',
      '      userCreated: {',
      '        send(payload: UserCreatedPayload, options?: GeneratedProducerSendOptions<TRuntimeProducer>) {',
      '          return runtimeSend(producerMetadataByTopic.userEvents.userCreated, payload, options);',
      '        }',
      '      }',
      '    }',
      '  }) as GeneratedProducer<TRuntimeProducer>;',
      '}'
    ].join('\n'));
    expect(contents).toContain([
      'export function createClient<TRuntimeClient extends RuntimeClient>(runtime: TRuntimeClient): GeneratedClient<TRuntimeClient> {',
      '  return Object.assign(Object.create(runtime), {',
      '    producer: createProducer(runtime.producer),',
      '    consumer: createConsumer(runtime.consumer)',
      '  }) as GeneratedClient<TRuntimeClient>;',
      '}'
    ].join('\n'));
  });

  it('emits topic-level subscriptions only when a topic has multiple events', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            },
            {
              name: 'user.updated',
              schemaPath: './user-updated.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents = output.files.find((file) => file.filePath === 'kafka-client.ts')?.contents ?? '';

    expect(contents).toContain(`export type UserEventsTopicMessage = UserCreatedPayloadMessage | UserUpdatedPayloadMessage;`);
    expect(contents).toContain('on(handler: (message: UserEventsTopicMessage) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;');
    expect(contents).toContain("topicName: 'user.events'");
    expect(contents).toContain("'user.updated': producerMetadataByTopic.userEvents.userUpdated");
    expect(contents).toContain([
      'export interface GeneratedUserEventsConsumerTopic<TRuntimeConsumer extends RuntimeConsumer = RuntimeConsumer> {',
      '  on(handler: (message: UserEventsTopicMessage) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;',
      '  userCreated: {',
      '    on(handler: (message: UserCreatedPayloadMessage) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;',
      '  };',
      '  userUpdated: {',
      '    on(handler: (message: UserUpdatedPayloadMessage) => Promise<void> | void, options?: GeneratedConsumerSubscribeOptions<TRuntimeConsumer>): Promise<void>;',
      '  };',
      '}'
    ].join('\n'));
  });

  it('derives minimal topic properties from the full topic name when names are prefixed', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'ktg-run-123.user.events'
        }
      ]
    });
    const contents = output.files.find((file) => file.filePath === 'kafka-client.ts')?.contents ?? '';

    expect(contents).toContain('ktgRun123UserEvents: GeneratedKtgRun123UserEventsProducerTopic<TRuntimeProducer>;');
    expect(contents).toContain('producerMetadataByTopic.ktgRun123UserEvents.userCreated');
    expect(contents).toContain("topicName: 'ktg-run-123.user.events'");
  });

  it('fails loudly when full topic names sanitize to the same generated property', async () => {
    await expect(
      buildGeneratedOutput({
        outputDir: './generated',
        sources: {
          rootDir: schemaFixturesDir
        },
        topics: [
          {
            events: [
              {
                name: 'user.created',
                schemaPath: './user-created.avsc'
              }
            ],
            name: 'tenant-a.user.events'
          },
          {
            events: [
              {
                name: 'user.updated',
                schemaPath: './user-updated.avsc'
              }
            ],
            name: 'tenant_a.user.events'
          }
        ]
      })
    ).rejects.toThrow(/Generated topic property 'tenantAUserEvents' collides between topics 'tenant[_-]a\.user\.events' and 'tenant[-_]a\.user\.events'\./);
  });

  it('supports the legacy advanced generated surface when explicitly requested', async () => {
    const output = await buildGeneratedOutput({
      generation: {
        apiMode: 'advanced'
      },
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents = output.files.find((file) => file.filePath === 'kafka-client.ts')?.contents ?? '';

    expect(contents).toContain("export const EventNames = {");
    expect(contents).toContain("export const TopicNames = {");
    expect(contents).toContain('export const producerEventMetadata');
    expect(contents).toContain('producer.send = ((');
    expect(contents).toContain([
      'export const EventNames = {',
      "  UserCreated: 'user.created',",
      '} as const;',
      '',
      'export const TopicNames = {',
      "  UserEvents: 'user.events',",
      '} as const;'
    ].join('\n'));
  });

  it('emits only a source-file and index re-export for direct relative imports', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    expect(output.files.map((file) => file.filePath)).toEqual([
      'kafka-client.ts',
      'index.ts'
    ]);
    expect(output.files.find((file) => file.filePath === 'index.ts')?.contents)
      .toBe("export * from './kafka-client.js';\n");
  });

  it('emits a schema registry config constant with url only', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      schemaRegistry: {
        auth: {
          password: 'secret-password',
          username: 'registry-user'
        },
        url: 'http://localhost:8081'
      },
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain("export const SchemaRegistryConfig = {\n  url: 'http://localhost:8081'\n} as const;");
    expect(contents).not.toContain('secret-password');
    expect(contents).not.toContain('registry-user');
  });

  it('emits nested named declarations and logical types from Avro schemas', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.profiled',
              schemaPath: './user-profile.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain(`export type UserStatus = 'ACTIVE' | 'DISABLED';`);
    expect(contents).toContain(`export interface Address {
  street: string;
  createdAt: AvroTimestampMillis;
}`);
    expect(contents).toContain(`export interface UserProfiledPayload {
  id: string;
  status: UserStatus;
  primaryAddress: Address;
  shippingAddress: Address;
  birthDate: AvroDate;
  balance: null | AvroDecimal;
}`);
  });

  it('resolves cross-file named references and recursive record references', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'address.shared',
              schemaPath: './shared-address.avsc'
            },
            {
              name: 'user.addressReferenced',
              schemaPath: './user-address-referenced.avsc'
            },
            {
              name: 'user.nodeLinked',
              schemaPath: './user-node.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain(`export interface AddressSharedPayload {
  street: string;
}`);
    expect(contents).toContain(`export interface UserAddressReferencedPayload {
  primaryAddress: AddressSharedPayload;
  shippingAddress: AddressSharedPayload;
}`);
    expect(contents).toContain(`export interface UserNodeLinkedPayload {
  id: string;
  next: null | UserNodeLinkedPayload;
}`);
  });

  it('emits payload aliases for top-level enum and fixed roots', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.statusChanged',
              schemaPath: './user-status.avsc'
            },
            {
              name: 'user.sessionTokenIssued',
              schemaPath: './session-token.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain(`export type UserStatus = 'ACTIVE' | 'DISABLED';`);
    expect(contents).toContain('export type UserStatusChangedPayload = UserStatus;');
    expect(contents).toContain('export type SessionToken = Uint8Array;');
    expect(contents).toContain('export type UserSessionTokenIssuedPayload = SessionToken;');
  });

  it('supports mixed top-level record, enum, and fixed roots deterministically', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            },
            {
              name: 'user.statusChanged',
              schemaPath: './user-status.avsc'
            },
            {
              name: 'user.sessionTokenIssued',
              schemaPath: './session-token.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain('export interface UserCreatedPayload {');
    expect(contents).toContain('export type UserStatusChangedPayload = UserStatus;');
    expect(contents).toContain('export type UserSessionTokenIssuedPayload = SessionToken;');
  });

  it('renders configured external Avro type mappings as TypeScript type expressions', async () => {
    const output = await buildGeneratedOutput({
      generation: {
        avroExternalTypes: {
          'com.external.ExternalAddress': "import('./external-types.js').ExternalAddress"
        }
      },
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.externalUser',
              schemaPath: './external-user.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain(
      "address: import('./external-types.js').ExternalAddress;"
    );
  });

  it('rejects configured external Avro type mappings that collide with generated names', async () => {
    await expect(
      buildGeneratedOutput({
        generation: {
          avroExternalTypes: {
            'com.external.ExternalAddress': 'UserExternalUserPayload'
          }
        },
        outputDir: './generated',
        sources: {
          rootDir: schemaFixturesDir
        },
        topics: [
          {
            events: [
              {
                name: 'user.externalUser',
                schemaPath: './external-user.avsc'
              }
            ],
            name: 'user.events'
          }
        ]
      })
    ).rejects.toThrow(
      "Configured external Avro type 'com.external.ExternalAddress' collides with generated type 'UserExternalUserPayload'"
    );
  });

  it('renders Avro references, logical types, nested records, unions, arrays, maps, and enums', () => {
    expect(toTypeScriptType('com.example.UserCreated')).toBe('UserCreated');
    expect(toTypeScriptType({ logicalType: 'uuid', type: 'string' })).toBe('string');
    expect(toTypeScriptType({ logicalType: 'date', type: 'int' })).toBe('AvroDate');
    expect(toTypeScriptType({ logicalType: 'timestamp-millis', type: 'long' })).toBe('AvroTimestampMillis');
    expect(toTypeScriptType({ logicalType: 'decimal', type: 'bytes' })).toBe('AvroDecimal');
    expect(toTypeScriptType(['null', 'string'])).toBe('null | string');
    expect(toTypeScriptType({
      items: 'string',
      type: 'array'
    })).toBe('string[]');
    expect(toTypeScriptType({
      type: 'map',
      values: 'long'
    })).toBe('Record<string, number>');
    expect(toTypeScriptType({
      symbols: ['ACTIVE', 'DISABLED'],
      type: 'enum'
    })).toBe("'ACTIVE' | 'DISABLED'");
    expect(toTypeScriptType({
      fields: [
        {
          name: 'nestedId',
          type: 'string'
        }
      ],
      name: 'NestedRecord',
      type: 'record'
    })).toBe(`{
  nestedId: string;
}`);
  });

  it('supports safe semantic rendering for Avro long without changing logical type aliases', async () => {
    const output = await buildGeneratedOutput({
      generation: {
        avroSemanticMode: 'safe'
      },
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.statsCaptured',
              schemaPath: './user-stats.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain('count: bigint;');
    expect(contents).toContain('capturedAt: AvroTimestampMillis;');
  });

  it('supports safe semantic rendering in direct type conversion helpers', () => {
    expect(
      toTypeScriptType('long', { path: 'schema', semanticMode: 'safe' })
    ).toBe('bigint');
    expect(
      toTypeScriptType(
        {
          type: 'map',
          values: 'long'
        },
        { path: 'schema', semanticMode: 'safe' }
      )
    ).toBe('Record<string, bigint>');
  });

  it('fails loudly for unsupported Avro types', () => {
    expect(() => toTypeScriptType({ type: { nested: true } })).toThrowError(
      'Unsupported Avro complex type'
    );
    expect(() => toTypeScriptType(42)).toThrowError(
      "Unsupported Avro type definition at 'schema': 42."
    );
    expect(() => toTypeScriptType('not a valid type name')).toThrowError(
      "Unsupported Avro type 'not a valid type name' at 'schema'."
    );
  });
});
