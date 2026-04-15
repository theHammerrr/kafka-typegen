import { randomUUID } from 'node:crypto';

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
import {
  type IntegrationAppFixtureName,
  renderIntegrationAppFixture
} from './app-fixtures.js';

let environment: IntegrationEnvironment;
let runSequence = 0;
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
    const runId = createRunId();
    const workspace = await createWorkspace(createConfigText({ runId }), { runId });

    const dryRunResult = await runCliCommand(workspace, ['sync']);
    expect(dryRunResult.exitCode).toBe(0);
    expect(dryRunResult.stdout).toContain(`[kafka] CREATE ${runId}.user.profile`);
    expect(dryRunResult.stdout).toContain(`[kafka] CREATE ${runId}.user.events`);
    expect(dryRunResult.stdout).toContain(`[registry] CREATE ${runId}.user.profile-user.profiled`);
    expect(dryRunResult.stdout).toContain(`[registry] CREATE ${runId}.user.events-user.created`);

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
    expect(noDriftSync.operations).toHaveLength(5);
    if (!noDriftSync.operations.every((operation) => operation.action === 'noop')) {
      throw new Error(
        `Expected all post-apply sync operations to be noops.\n${JSON.stringify(noDriftSync.operations, null, 2)}`
      );
    }
  });

  it('compiles generated code and user code, then produces and consumes real Kafka messages', async () => {
    const workspace = await createBuiltWorkspace(['happy-path.ts']);

    const result = await runWorkspaceScript(workspace, 'happy-path.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('HAPPY_PATH_OK');
  });

  it('surfaces producer serialization failures for invalid payloads', async () => {
    const workspace = await createBuiltWorkspace(['invalid-produce.ts']);

    const result = await runWorkspaceScript(workspace, 'invalid-produce.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('SEND_REJECTED:');
  });

  it('surfaces schema-registry decode failures through the consumer onError hook', async () => {
    const workspace = await createBuiltWorkspace(['decode-error.ts']);

    const result = await runWorkspaceScript(workspace, 'decode-error.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('CONSUMER_ERROR:');
  });

  it('surfaces async handler failures through onError', async () => {
    const workspace = await createBuiltWorkspace(['handler-error.ts']);

    const result = await runWorkspaceScript(workspace, 'handler-error.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('HANDLER_ERROR:handler failed');
  });

  it('supports standalone producer-only and consumer-only generated factories', async () => {
    const workspace = await createBuiltWorkspace(['standalone-factories.ts']);

    const result = await runWorkspaceScript(workspace, 'standalone-factories.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('STANDALONE_FACTORIES_OK');
  });

  it('fails deterministically for conflicting repeated topic subscriptions', async () => {
    const workspace = await createBuiltWorkspace(['conflicting-options.ts']);

    const result = await runWorkspaceScript(workspace, 'conflicting-options.js');

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('CONFLICT:Topic ');
    expect(result.stdout).toContain(
      "user.events' is already subscribed with different consume options."
    );
  });

  it('fails sync when Kafka topic drift or Schema Registry subject drift is detected', async () => {
    const kafkaRunId = createRunId();
    const kafkaBaselineWorkspace = await createWorkspace(
      createConfigText({ runId: kafkaRunId }),
      { runId: kafkaRunId }
    );
    const kafkaBaselineApply = await runCliCommand(kafkaBaselineWorkspace, [
      'sync',
      '--apply'
    ]);
    expect(kafkaBaselineApply.exitCode).toBe(0);
    const driftedKafkaWorkspace = await createWorkspace(createConfigText({
      kafkaPartitions: 2,
      kafkaDriftChecks: true,
      runId: kafkaRunId
    }), { runId: kafkaRunId });
    const kafkaDriftResult = await runCliCommand(driftedKafkaWorkspace, ['sync']);

    expect(kafkaDriftResult.exitCode).toBe(1);
    expect(kafkaDriftResult.stderr).toContain('Kafka sync detected topic drift');

    const registryRunId = createRunId();
    const registryBaselineWorkspace = await createWorkspace(
      createConfigText({ runId: registryRunId }),
      { runId: registryRunId }
    );
    const registryBaselineApply = await runCliCommand(registryBaselineWorkspace, [
      'sync',
      '--apply'
    ]);
    expect(registryBaselineApply.exitCode).toBe(0);
    const driftedRegistryWorkspace = await createWorkspace(createConfigText({
      profileSchemaPath: './user-created.avsc',
      registryDriftChecks: true,
      runId: registryRunId
    }), { runId: registryRunId });
    const registryDriftResult = await runCliCommand(driftedRegistryWorkspace, ['sync']);

    expect(registryDriftResult.exitCode).toBe(1);
    expect(registryDriftResult.stderr).toContain('Schema Registry sync detected subject drift');
  });

  it('registers a new schema version on compatible subject drift during apply', async () => {
    const runId = createRunId();
    const workspace = await createWorkspace(createConfigText({ runId }), { runId });
    const firstApplyResult = await runCliCommand(workspace, ['sync', '--apply']);
    expect(firstApplyResult.exitCode).toBe(0);

    const evolvedWorkspace = await createSchemaEvolutionWorkspace(
      createEvolutionConfigText({
        compatibility: 'BACKWARD',
        onDrift: 'register',
        runId
      }),
      createCompatibleUserCreatedSchema(),
      runId
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
    expect(evolutionApplyResult.stdout).toContain(`[registry] UPDATE ${runId}.user.events-user.created`);
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
    const runId = createRunId();
    const workspace = await createWorkspace(createConfigText({ runId }), { runId });
    const firstApplyResult = await runCliCommand(workspace, ['sync', '--apply']);
    expect(firstApplyResult.exitCode).toBe(0);

    const evolvedWorkspace = await createSchemaEvolutionWorkspace(
      createEvolutionConfigText({
        compatibility: 'FULL',
        onDrift: 'register',
        runId
      }),
      createIncompatibleUserCreatedSchema(),
      runId
    );
    const evolutionApplyResult = await runCliCommand(evolvedWorkspace, ['sync', '--apply']);

    expect(evolutionApplyResult.exitCode).toBe(1);
    expect(evolutionApplyResult.stderr).toContain(
      `Failed to register Schema Registry subject '${runId}.user.events-user.created' for event 'user.created'`
    );
    expect(evolutionApplyResult.stderr).toContain('Schema Registry request failed');
    expect(evolutionApplyResult.stderr).toContain('Evolution hints:');
    expect(evolutionApplyResult.stderr).toContain(
      "Field 'displayName' was added without a default."
    );
  });
});

async function createWorkspace(
  configContents: string,
  options: {
    readonly appFixtures?: readonly IntegrationAppFixtureName[];
    readonly appFiles?: Readonly<Record<string, string>>;
    readonly runId: string;
  }
): Promise<string> {
  const appFiles = {
    'typecheck-app.ts': await renderIntegrationAppFixture('typecheck-app.ts', {
      kafkaBroker: environment.kafkaBroker,
      runId: options.runId
    }),
    ...(await renderAppFixtureFiles(options.appFixtures ?? [], options.runId)),
    ...(options.appFiles ?? {})
  };
  const workspace = await createIntegrationWorkspace(configContents, {
    ...appFiles
  });
  workspaces.push(workspace);

  const generateResult = await runCliCommand(workspace, ['generate', '--config', 'kafka-typegen.config.mjs']);
  expect(generateResult.exitCode).toBe(0);

  return workspace;
}

async function createBuiltWorkspace(
  appFixtures: readonly IntegrationAppFixtureName[]
): Promise<string> {
  const runId = createRunId();
  const workspace = await createWorkspace(createConfigText({ runId }), {
    appFixtures,
    runId
  });
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
  userCreatedSchemaContents: string,
  runId: string
): Promise<string> {
  const workspace = await createIntegrationWorkspace(configContents, {
    'typecheck-app.ts': await renderIntegrationAppFixture('typecheck-app.ts', {
      kafkaBroker: environment.kafkaBroker,
      runId
    }),
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
  runId?: string;
} = {}): string {
  const runId = options.runId ?? createRunId();

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
      clientId: '${runId}-kafka-typegen-integration-sync',
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
      name: '${runId}.user.profile',
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
      name: '${runId}.user.events',
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
  runId: string;
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
      name: '${options.runId}.user.events',
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

function createRunId(): string {
  runSequence += 1;
  return `ktg-${runSequence}-${randomUUID().slice(0, 8)}`;
}

async function renderAppFixtureFiles(
  fixtureNames: readonly IntegrationAppFixtureName[],
  runId: string
): Promise<Record<string, string>> {
  const files: Record<string, string> = {};

  for (const fixtureName of fixtureNames) {
    files[fixtureName] = await renderIntegrationAppFixture(fixtureName, {
      kafkaBroker: environment.kafkaBroker,
      runId
    });
  }

  return files;
}
