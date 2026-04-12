import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  type IntegrationEnvironment,
  startIntegrationEnvironment
} from './environment.js';
import {
  buildWorkspace,
  createIntegrationWorkspace,
  removeIntegrationWorkspace,
  runCliCommand,
  runTypecheck,
  runWorkspaceScript
} from './workspace.js';

let environment: IntegrationEnvironment;
const workspaces: string[] = [];

beforeAll(async () => {
  environment = await startIntegrationEnvironment();
});

afterAll(async () => {
  await environment?.stop();

  for (const workspace of workspaces.splice(0, workspaces.length)) {
    await removeIntegrationWorkspace(workspace);
  }
});

describe('testcontainers integration', () => {
  it('runs sync dry-run, sync --apply, and verifies no drift after apply', async () => {
    const workspace = await createWorkspace(createConfigText());

    const dryRunResult = await runCliCommand(workspace, ['sync']);
    expect(dryRunResult.exitCode).toBe(0);
    expect(dryRunResult.stdout).toContain('[kafka] CREATE user.profile');
    expect(dryRunResult.stdout).toContain('[kafka] CREATE user.events');
    expect(dryRunResult.stdout).toContain('[registry] CREATE user.profile-user.profiled');
    expect(dryRunResult.stdout).toContain('[registry] CREATE user.events-user.created');

    const applyResult = await runCliCommand(workspace, ['sync', '--apply']);
    expect(applyResult.exitCode).toBe(0);
    expect(applyResult.stdout).toContain('Applied 5 sync operation(s).');

    const noDriftResult = await runCliCommand(workspace, ['sync', '--json']);
    expect(noDriftResult.exitCode).toBe(0);
    const noDriftSync = JSON.parse(noDriftResult.stdout) as {
      applied: boolean;
      operations: Array<{ action: string }>;
    };
    expect(noDriftSync.applied).toBe(false);
    if (!noDriftSync.operations.every((operation) => operation.action === 'noop')) {
      throw new Error(
        `Expected all post-apply sync operations to be noops.\n${JSON.stringify(noDriftSync.operations, null, 2)}`
      );
    }
  });

  it('compiles generated code and user code, then produces and consumes real Kafka messages', async () => {
    const workspace = await createBuiltWorkspace(createHappyPathAppFiles());

    const result = await runWorkspaceScript(workspace, 'happy-path.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('HAPPY_PATH_OK');
  });

  it('surfaces producer serialization failures for invalid payloads', async () => {
    const workspace = await createBuiltWorkspace({
      'invalid-produce.ts': createInvalidProduceSource()
    });

    const result = await runWorkspaceScript(workspace, 'invalid-produce.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('SEND_REJECTED:');
  });

  it('surfaces schema-registry decode failures through the consumer onError hook', async () => {
    const workspace = await createBuiltWorkspace({
      'decode-error.ts': createDecodeErrorSource()
    });

    const result = await runWorkspaceScript(workspace, 'decode-error.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('CONSUMER_ERROR:');
  });

  it('surfaces async handler failures through onError', async () => {
    const workspace = await createBuiltWorkspace({
      'handler-error.ts': createHandlerErrorSource()
    });

    const result = await runWorkspaceScript(workspace, 'handler-error.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('HANDLER_ERROR:handler failed');
  });

  it('fails deterministically for conflicting repeated topic subscriptions', async () => {
    const workspace = await createBuiltWorkspace({
      'conflicting-options.ts': createConflictingOptionsSource()
    });

    const result = await runWorkspaceScript(workspace, 'conflicting-options.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("CONFLICT:Topic 'user.events' is already subscribed with different consume options.");
  });

  it('fails sync when Kafka topic drift or Schema Registry subject drift is detected', async () => {
    const driftedKafkaWorkspace = await createWorkspace(createConfigText({
      kafkaPartitions: 2,
      kafkaDriftChecks: true
    }));
    const kafkaDriftResult = await runCliCommand(driftedKafkaWorkspace, ['sync']);

    expect(kafkaDriftResult.exitCode).toBe(1);
    expect(kafkaDriftResult.stderr).toContain('Kafka sync detected topic drift');

    const driftedRegistryWorkspace = await createWorkspace(createConfigText({
      profileSchemaPath: './user-created.avsc',
      registryDriftChecks: true
    }));
    const registryDriftResult = await runCliCommand(driftedRegistryWorkspace, ['sync']);

    expect(registryDriftResult.exitCode).toBe(1);
    expect(registryDriftResult.stderr).toContain('Schema Registry sync detected subject drift');
  });

  it('registers a new schema version on compatible subject drift during apply', async () => {
    const workspace = await createWorkspace(createConfigText());
    const firstApplyResult = await runCliCommand(workspace, ['sync', '--apply']);
    expect(firstApplyResult.exitCode).toBe(0);

    const evolvedWorkspace = await createSchemaEvolutionWorkspace(
      createEvolutionConfigText({
        compatibility: 'BACKWARD',
        onDrift: 'register'
      }),
      createCompatibleUserCreatedSchema()
    );
    const driftDryRunResult = await runCliCommand(evolvedWorkspace, ['sync', '--json']);
    expect(driftDryRunResult.exitCode).toBe(0);
    const parsedDriftPlan = JSON.parse(driftDryRunResult.stdout) as {
      operations: Array<{ action: string; target: string }>;
    };
    expect(parsedDriftPlan.operations).toEqual([
      expect.objectContaining({ action: 'update', target: 'registry' })
    ]);
    expect(driftDryRunResult.stdout).toContain('Compatibility BACKWARD will be applied.');

    const evolutionApplyResult = await runCliCommand(evolvedWorkspace, ['sync', '--apply']);
    expect(evolutionApplyResult.exitCode).toBe(0);
    expect(evolutionApplyResult.stdout).toContain('[registry] UPDATE user.events-user.created');
    expect(evolutionApplyResult.stdout).toContain('Compatibility BACKWARD was applied.');

    const noDriftResult = await runCliCommand(evolvedWorkspace, ['sync', '--json']);
    expect(noDriftResult.exitCode).toBe(0);
    const parsedNoDrift = JSON.parse(noDriftResult.stdout) as {
      operations: Array<{ action: string; target: string }>;
    };
    expect(
      parsedNoDrift.operations.filter((operation) => operation.target === 'registry')
    ).toEqual([expect.objectContaining({ action: 'noop' })]);
  });

  it('surfaces Schema Registry compatibility failures on incompatible evolution', async () => {
    const workspace = await createWorkspace(createConfigText());
    const firstApplyResult = await runCliCommand(workspace, ['sync', '--apply']);
    expect(firstApplyResult.exitCode).toBe(0);

    const evolvedWorkspace = await createSchemaEvolutionWorkspace(
      createEvolutionConfigText({
        compatibility: 'FULL',
        onDrift: 'register'
      }),
      createIncompatibleUserCreatedSchema()
    );
    const evolutionApplyResult = await runCliCommand(evolvedWorkspace, ['sync', '--apply']);

    expect(evolutionApplyResult.exitCode).toBe(1);
    expect(evolutionApplyResult.stderr).toContain(
      "Failed to register Schema Registry subject 'user.events-user.created' for event 'user.created'"
    );
    expect(evolutionApplyResult.stderr).toContain('Schema Registry request failed');
  });
});

async function createWorkspace(
  configContents: string,
  appFiles: Readonly<Record<string, string>> = {}
): Promise<string> {
  const workspace = await createIntegrationWorkspace(configContents, {
    'typecheck-app.ts': createTypecheckSource(),
    ...appFiles
  });
  workspaces.push(workspace);

  const generateResult = await runCliCommand(workspace, ['generate', '--config', 'kafka-typegen.config.mjs']);
  expect(generateResult.exitCode).toBe(0);

  return workspace;
}

async function createBuiltWorkspace(
  appFiles: Readonly<Record<string, string>>
): Promise<string> {
  const workspace = await createWorkspace(createConfigText(), appFiles);
  const applyResult = await runCliCommand(workspace, ['sync', '--apply']);
  expect(applyResult.exitCode).toBe(0);

  const typecheckResult = await runTypecheck(workspace);
  if (typecheckResult.exitCode !== 0) {
    throw new Error(
      `Generated workspace typecheck failed.\nstdout:\n${typecheckResult.stdout}\nstderr:\n${typecheckResult.stderr}`
    );
  }

  const buildResult = await buildWorkspace(workspace);
  if (buildResult.exitCode !== 0) {
    throw new Error(
      `Generated workspace build failed.\nstdout:\n${buildResult.stdout}\nstderr:\n${buildResult.stderr}`
    );
  }

  return workspace;
}

async function createSchemaEvolutionWorkspace(
  configContents: string,
  userCreatedSchemaContents: string
): Promise<string> {
  const workspace = await createIntegrationWorkspace(configContents, {
    'typecheck-app.ts': createTypecheckSource(),
    '../schemas/user-created.avsc': userCreatedSchemaContents
  });
  workspaces.push(workspace);

  const generateResult = await runCliCommand(workspace, [
    'generate',
    '--config',
    'kafka-typegen.config.mjs'
  ]);
  expect(generateResult.exitCode).toBe(0);

  return workspace;
}

function createConfigText(options: {
  kafkaDriftChecks?: boolean;
  kafkaPartitions?: number;
  profileSchemaPath?: string;
  registryDriftChecks?: boolean;
} = {}): string {
  return `export default {
  outputDir: './src/generated/kafka',
  runtime: {
    transport: '@platformatic/kafka'
  },
  schemaRegistry: {
    url: '${environment.schemaRegistryUrl}'
  },
  sync: {
    kafka: {
      brokers: ['${environment.kafkaBroker}'],
      clientId: 'kafka-typegen-integration-sync',
      failOnDrift: ${options.kafkaDriftChecks === true}
    },
    schemaRegistry: {
      failOnDrift: ${options.registryDriftChecks === true}
    }
  },
  sources: {
    rootDir: './schemas'
  },
  topics: [
    {
      name: 'user.profile',
      sync: {
        partitions: ${options.kafkaPartitions ?? 1},
        replicationFactor: 1
      },
      events: [
        {
          name: 'user.profiled',
          schemaPath: '${options.profileSchemaPath ?? './user-profile.avsc'}'
        }
      ]
    },
    {
      name: 'user.events',
      sync: {
        partitions: 1,
        replicationFactor: 1
      },
      events: [
        {
          name: 'user.created',
          schemaPath: './user-created.avsc'
        },
        {
          name: 'user.updated',
          schemaPath: './user-updated.avsc'
        }
      ]
    }
  ]
};
`;
}

function createEvolutionConfigText(options: {
  compatibility: 'BACKWARD' | 'FULL';
  onDrift: 'register';
}): string {
  return `export default {
  outputDir: './src/generated/kafka',
  runtime: {
    transport: '@platformatic/kafka'
  },
  schemaRegistry: {
    url: '${environment.schemaRegistryUrl}'
  },
  sync: {
    schemaRegistry: {
      compatibility: '${options.compatibility}',
      onDrift: '${options.onDrift}'
    }
  },
  sources: {
    rootDir: './schemas'
  },
  topics: [
    {
      name: 'user.events',
      events: [
        {
          name: 'user.created',
          schemaPath: './user-created.avsc'
        }
      ]
    }
  ]
};
`;
}

function createTypecheckSource(): string {
  return `import { Consumer, Producer } from '@platformatic/kafka';
import {
  createPlatformaticRuntimeClient,
  createPlatformaticRuntimeConsumer,
  createPlatformaticRuntimeProducer
} from 'kafka-typegen/runtime';
import {
  EventNames,
  SchemaRegistryConfig,
  TopicNames,
  createClient,
  createConsumer,
  createProducer,
  type UserCreatedPayload,
  type UserProfiledPayload
} from './generated/kafka/index.js';

const producerClient = new Producer({
  bootstrapBrokers: ['${environment.kafkaBroker}'],
  clientId: 'typecheck-producer'
});
const consumerClient = new Consumer({
  bootstrapBrokers: ['${environment.kafkaBroker}'],
  clientId: 'typecheck-consumer',
  groupId: 'typecheck-consumer-group'
});

const client = createClient(createPlatformaticRuntimeClient({
  producer: producerClient,
  consumer: consumerClient,
  schemaRegistry: SchemaRegistryConfig
}));
const producer = createProducer(createPlatformaticRuntimeProducer({
  producer: producerClient,
  schemaRegistry: SchemaRegistryConfig
}));
const consumer = createConsumer(createPlatformaticRuntimeConsumer({
  consumer: consumerClient,
  schemaRegistry: SchemaRegistryConfig
}));

const userCreated: UserCreatedPayload = {
  email: 'ada@example.com',
  id: 'user-1',
  isAdmin: true
};
const userProfiled: UserProfiledPayload = {
  balance: null,
  birthDate: 19800,
  id: '550e8400-e29b-41d4-a716-446655440000',
  primaryAddress: {
    createdAt: Date.now(),
    street: 'Main Street'
  },
  shippingAddress: {
    createdAt: Date.now(),
    street: 'Second Street'
  },
  status: 'ACTIVE'
};

void producer.events.userCreated.send(userCreated, { acks: -1 });
void client.producer.send(EventNames.UserProfiled, userProfiled);
void consumer.events.userCreated.on(async (message) => {
  message.payload.isAdmin;
}, { mode: 'latest', autocommit: true });
void consumer.onTopic(TopicNames.UserEvents, async (message) => {
  message.topic;
}, { mode: 'latest', autocommit: false });
void consumer.close(true);
void producer.close();
`;
}

function createHappyPathAppFiles(): Record<string, string> {
  return {
    'happy-path.ts': `import { Consumer, Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeClient } from 'kafka-typegen/runtime';
import {
  SchemaRegistryConfig,
  TopicNames,
  createClient
} from './generated/kafka/index.js';

const producerClient = new Producer({
  bootstrapBrokers: ['${environment.kafkaBroker}'],
  clientId: 'happy-path-producer'
});
const consumerClient = new Consumer({
  bootstrapBrokers: ['${environment.kafkaBroker}'],
  clientId: 'happy-path-consumer',
  groupId: 'happy-path-consumer-group'
});
const client = createClient(createPlatformaticRuntimeClient({
  producer: producerClient,
  consumer: consumerClient,
  schemaRegistry: SchemaRegistryConfig
}));

const receivedEvents: string[] = [];
let nextUserEventsOffset = 0n;
let userEventsPartition = 0;
let resolveAllMessages: (() => void) | undefined;
const allMessages = new Promise<void>((resolve, reject) => {
  resolveAllMessages = resolve;
  setTimeout(() => reject(new Error('Timed out waiting for consumed messages.')), 60_000).unref();
});

await client.consumer.events.userProfiled.on(async (message) => {
  receivedEvents.push(message.event);
  maybeResolve();
}, { autocommit: true, mode: 'latest' });
await client.consumer.events.userCreated.on(async (message) => {
  receivedEvents.push(message.event);
  userEventsPartition = message.partition ?? 0;
  nextUserEventsOffset = BigInt(message.offset ?? '0') + 1n;
  await client.consumer.commit({
    offsets: [{
      leaderEpoch: 0,
      offset: nextUserEventsOffset,
      partition: userEventsPartition,
      topic: TopicNames.UserEvents
    }]
  });
  maybeResolve();
}, { autocommit: false, mode: 'latest' });
await client.consumer.events.userUpdated.on(async (message) => {
  receivedEvents.push(message.event);
  userEventsPartition = message.partition ?? 0;
  nextUserEventsOffset = BigInt(message.offset ?? '0') + 1n;
  await client.consumer.commit({
    offsets: [{
      leaderEpoch: 0,
      offset: nextUserEventsOffset,
      partition: userEventsPartition,
      topic: TopicNames.UserEvents
    }]
  });
  maybeResolve();
}, { autocommit: false, mode: 'latest' });

await client.producer.events.userProfiled.send({
  balance: null,
  birthDate: 19800,
  id: '550e8400-e29b-41d4-a716-446655440000',
  primaryAddress: {
    createdAt: 1_700_000_000_000,
    street: 'Main Street'
  },
  shippingAddress: {
    createdAt: 1_700_000_100_000,
    street: 'Second Street'
  },
  status: 'ACTIVE'
}, { acks: -1 });
await client.producer.events.userCreated.send({
  email: 'ada@example.com',
  id: 'user-1',
  isAdmin: true
}, { acks: -1 });
await client.producer.events.userUpdated.send({
  displayName: 'Ada',
  id: 'user-1',
  metadata: {
    role: 'admin'
  }
}, { acks: -1 });

await allMessages;

if ([...receivedEvents].sort().join(',') !== 'user.created,user.profiled,user.updated') {
  throw new Error('Unexpected consumed events: ' + JSON.stringify(receivedEvents));
}

const committedOffsets = await client.consumer.listCommittedOffsets({
  topics: [{
    partitions: [userEventsPartition],
    topic: TopicNames.UserEvents
  }]
});

if ((committedOffsets.get(TopicNames.UserEvents)?.[userEventsPartition] ?? 0n) < nextUserEventsOffset) {
  throw new Error('Expected manual commit to persist the user.events offset.');
}

await client.producer.close();
await client.consumer.close(true);
console.log('HAPPY_PATH_OK');

function maybeResolve(): void {
  if (receivedEvents.length === 3) {
    resolveAllMessages?.();
  }
}
`
  };
}

function createInvalidProduceSource(): string {
  return `import { Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeProducer } from 'kafka-typegen/runtime';
import {
  SchemaRegistryConfig,
  createProducer,
  type UserCreatedPayload
} from './generated/kafka/index.js';

const producer = createProducer(createPlatformaticRuntimeProducer({
  producer: new Producer({
    bootstrapBrokers: ['${environment.kafkaBroker}'],
    clientId: 'invalid-produce'
  }),
  schemaRegistry: SchemaRegistryConfig
}));

let rejectionMessage: string | undefined;

try {
  await producer.events.userCreated.send({
    id: 'broken'
  } as unknown as UserCreatedPayload);
} catch (error) {
  rejectionMessage = String(error);
} finally {
  await producer.close();
}

if (rejectionMessage === undefined) {
  throw new Error('Expected invalid payload send to reject.');
}

console.log('SEND_REJECTED:' + rejectionMessage);
`;
}

function createDecodeErrorSource(): string {
  return `import { Buffer } from 'node:buffer';

import { Consumer, Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeConsumer } from 'kafka-typegen/runtime';
import {
  EventNames,
  SchemaRegistryConfig,
  TopicNames,
  createConsumer
} from './generated/kafka/index.js';

const producer = new Producer({
  bootstrapBrokers: ['${environment.kafkaBroker}'],
  clientId: 'decode-error-producer'
});
let resolveError: ((message: string) => void) | undefined;
const consumerError = new Promise<string>((resolve, reject) => {
  resolveError = resolve;
  setTimeout(() => reject(new Error('Timed out waiting for decode error.')), 60_000).unref();
});
const consumer = createConsumer(createPlatformaticRuntimeConsumer({
  consumer: new Consumer({
    bootstrapBrokers: ['${environment.kafkaBroker}'],
    clientId: 'decode-error-consumer',
    groupId: 'decode-error-consumer-group'
  }),
  onError(error) {
    resolveError?.(String(error));
  },
  schemaRegistry: SchemaRegistryConfig
}));

await consumer.events.userCreated.on(async () => {}, {
  autocommit: true,
  mode: 'latest'
});
const invalidPayload = Buffer.alloc(5);
invalidPayload[0] = 0;
invalidPayload.writeUInt32BE(999_999, 1);
await producer.send({
  acks: -1,
  messages: [{
    headers: new Map([
      [Buffer.from('x-kafka-typegen-event'), Buffer.from(EventNames.UserCreated)]
    ]),
    topic: TopicNames.UserEvents,
    value: invalidPayload
  }]
});

console.log('CONSUMER_ERROR:' + await consumerError);
await producer.close();
await consumer.close(true);
`;
}

function createHandlerErrorSource(): string {
  return `import { Consumer, Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeClient } from 'kafka-typegen/runtime';
import { SchemaRegistryConfig, createClient } from './generated/kafka/index.js';

const producer = new Producer({
  bootstrapBrokers: ['${environment.kafkaBroker}'],
  clientId: 'handler-error-producer'
});
let resolveError: ((message: string) => void) | undefined;
const handlerError = new Promise<string>((resolve, reject) => {
  resolveError = resolve;
  setTimeout(() => reject(new Error('Timed out waiting for handler error.')), 60_000).unref();
});
const client = createClient(createPlatformaticRuntimeClient({
  consumer: new Consumer({
    bootstrapBrokers: ['${environment.kafkaBroker}'],
    clientId: 'handler-error-consumer',
    groupId: 'handler-error-consumer-group'
  }),
  onError(error) {
    resolveError?.(error instanceof Error ? error.message : String(error));
  },
  producer,
  schemaRegistry: SchemaRegistryConfig
}));

await client.consumer.events.userCreated.on(async () => {
  throw new Error('handler failed');
}, {
  autocommit: true,
  mode: 'latest'
});
await client.producer.events.userCreated.send({
  email: 'ada@example.com',
  id: 'user-1',
  isAdmin: true
});

console.log('HANDLER_ERROR:' + await handlerError);
await client.producer.close();
await client.consumer.close(true);
`;
}

function createConflictingOptionsSource(): string {
  return `import { Consumer } from '@platformatic/kafka';
import { createPlatformaticRuntimeConsumer } from 'kafka-typegen/runtime';
import {
  SchemaRegistryConfig,
  TopicNames,
  createConsumer
} from './generated/kafka/index.js';

const consumer = createConsumer(createPlatformaticRuntimeConsumer({
  consumer: new Consumer({
    bootstrapBrokers: ['${environment.kafkaBroker}'],
    clientId: 'conflicting-options-consumer',
    groupId: 'conflicting-options-consumer-group'
  }),
  schemaRegistry: SchemaRegistryConfig
}));

await consumer.events.userCreated.on(async () => {}, {
  autocommit: true,
  mode: 'latest'
});

try {
  await consumer.onTopic(TopicNames.UserEvents, async () => {}, {
    autocommit: false,
    mode: 'latest'
  });
  throw new Error('Expected conflicting consume options to fail.');
} catch (error) {
  console.log('CONFLICT:' + (error instanceof Error ? error.message : String(error)));
} finally {
  await consumer.close(true);
}
`;
}

function createCompatibleUserCreatedSchema(): string {
  return JSON.stringify(
    {
      fields: [
        { name: 'id', type: 'string' },
        { name: 'email', type: 'string' },
        { name: 'isAdmin', type: 'boolean' },
        { default: null, name: 'displayName', type: ['null', 'string'] }
      ],
      name: 'UserCreated',
      namespace: 'com.example.users',
      type: 'record'
    },
    null,
    2
  );
}

function createIncompatibleUserCreatedSchema(): string {
  return JSON.stringify(
    {
      fields: [
        { name: 'id', type: 'string' },
        { name: 'email', type: 'string' },
        { name: 'isAdmin', type: 'boolean' },
        { name: 'displayName', type: 'string' }
      ],
      name: 'UserCreated',
      namespace: 'com.example.users',
      type: 'record'
    },
    null,
    2
  );
}
