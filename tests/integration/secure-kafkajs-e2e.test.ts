import { randomUUID } from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  type IntegrationAppFixtureName,
  renderIntegrationAppFixture
} from './app-fixtures.js';
import {
  type SecureIntegrationEnvironment,
  startSecureIntegrationEnvironment
} from './secure-environment.js';
import {
  buildWorkspace,
  createIntegrationWorkspace,
  removeIntegrationWorkspace,
  runCliCommand,
  runTypecheck,
  runWorkspaceScript
} from './workspace.js';

let environment: SecureIntegrationEnvironment;
let runSequence = 0;
const workspaces: string[] = [];

beforeAll(async () => {
  environment = await startSecureIntegrationEnvironment();
});

afterAll(async () => {
  await environment?.stop();

  for (const workspace of workspaces.splice(0, workspaces.length)) {
    await removeIntegrationWorkspace(workspace);
  }
});

describe('secure Kafka integration', () => {
  it('applies Kafka sync with SCRAM auth and runs a generated KafkaJS app against the secured broker', async () => {
    const runId = createRunId();
    const workspace = await createWorkspace(createSecureConfigText({ runId }), {
      appFixtures: ['secure-happy-path.ts', 'secure-typecheck-app.ts'],
      runId
    });

    const dryRunResult = await runCliCommand(workspace, ['sync', '--target', 'kafka']);
    expect(dryRunResult.exitCode).toBe(0);
    expect(dryRunResult.stdout).toContain(`[kafka] CREATE ${runId}.user.events`);

    const applyResult = await runCliCommand(workspace, ['sync', '--apply', '--target', 'kafka']);
    expect(applyResult.exitCode).toBe(0);
    expect(applyResult.stdout).toContain('Applied 1 sync operation(s).');

    const noDriftResult = await runCliCommand(workspace, ['sync', '--json', '--target', 'kafka']);
    expect(noDriftResult.exitCode).toBe(0);
    const noDriftSync = JSON.parse(noDriftResult.stdout) as {
      applied: boolean;
      operations: Array<{ action: string; target: string }>;
    };
    expect(noDriftSync.applied).toBe(false);
    expect(noDriftSync.operations).toEqual([
      expect.objectContaining({ action: 'noop', target: 'kafka' })
    ]);

    const typecheckResult = await runTypecheck(workspace);
    if (typecheckResult.exitCode !== 0) {
      throw new Error(
        `Secure generated workspace typecheck failed.\nstdout:\n${typecheckResult.stdout}\nstderr:\n${typecheckResult.stderr}`
      );
    }

    const buildResult = await buildWorkspace(workspace);
    if (buildResult.exitCode !== 0) {
      throw new Error(
        `Secure generated workspace build failed.\nstdout:\n${buildResult.stdout}\nstderr:\n${buildResult.stderr}`
      );
    }

    const runResult = await runWorkspaceScript(workspace, 'secure-happy-path.js');
    expect(runResult.exitCode).toBe(0);
    expect(runResult.stdout).toContain('SECURE_HAPPY_PATH_OK');
  });

  it('fails Kafka sync clearly when SCRAM credentials are invalid', async () => {
    const workspace = await createWorkspace(
      createSecureConfigText({
        password: 'wrong-password',
        runId: createRunId()
      }),
      {
        runId: createRunId()
      }
    );

    const result = await runCliCommand(workspace, ['sync', '--target', 'kafka']);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toMatch(/sasl|authentication|authenticate/i);
  });
});

async function createWorkspace(
  configContents: string,
  options: {
    readonly appFixtures?: readonly IntegrationAppFixtureName[];
    readonly runId: string;
  }
): Promise<string> {
  const appFiles = await renderAppFixtureFiles(options.appFixtures ?? [], options.runId);
  const workspace = await createIntegrationWorkspace(configContents, appFiles);
  workspaces.push(workspace);

  const generateResult = await runCliCommand(workspace, ['generate', '--config', 'kafka-typegen.config.mjs']);
  expect(generateResult.exitCode).toBe(0);

  return workspace;
}

function createSecureConfigText(options: {
  readonly password?: string;
  readonly runId: string;
}): string {
  return `export default {
  outputDir: './src/generated/kafka',
  runtime: {
    transport: 'kafkajs'
  },
  sync: {
    kafka: {
      brokers: ['${environment.kafkaBroker}'],
      clientId: '${options.runId}-secure-sync',
      ssl: false,
      sasl: {
        mechanism: '${environment.sasl.mechanism}',
        username: '${environment.sasl.username}',
        password: '${options.password ?? environment.sasl.password}'
      }
    }
  },
  sources: {
    rootDir: './schemas'
  },
  topics: [
    {
      name: '${options.runId}.user.events',
      sync: {
        partitions: 1,
        replicationFactor: 1
      },
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

function createRunId(): string {
  runSequence += 1;
  return `ktg-secure-${runSequence}-${randomUUID().slice(0, 8)}`;
}

async function renderAppFixtureFiles(
  fixtureNames: readonly IntegrationAppFixtureName[],
  runId: string
): Promise<Record<string, string>> {
  const files: Record<string, string> = {};

  for (const fixtureName of fixtureNames) {
    files[fixtureName] = await renderIntegrationAppFixture(fixtureName, {
      kafkaBroker: environment.kafkaBroker,
      kafkaPassword: environment.sasl.password,
      kafkaUsername: environment.sasl.username,
      runId
    });
  }

  return files;
}
