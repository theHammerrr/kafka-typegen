import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve as resolvePath } from 'node:path';

import ts from 'typescript';
import { afterEach, describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createTypeGenerator,
  resolveConfig
} from '../src/index.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');
const tempDirs: string[] = [];

async function createWorkspace(): Promise<string> {
  const workspace = await mkdtemp(join(tmpdir(), 'kafka-typegen-package-'));
  tempDirs.push(workspace);
  return workspace;
}

async function writeGeneratedPackage(workspace: string): Promise<void> {
  const config = resolveConfig({
    generation: {
      packageName: '@acme/generated-kafka'
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
  const catalog = await createCatalogBuilder().build(config);
  const output = await createTypeGenerator().generate(catalog);
  const packageRoot = join(workspace, 'node_modules', '@acme', 'generated-kafka');

  for (const file of output.files) {
    const targetPath = join(packageRoot, file.filePath);
    await mkdir(dirname(targetPath), { recursive: true });
    await writeFile(targetPath, file.contents);
  }

  const runtimeRoot = join(workspace, 'node_modules', 'kafka-typegen', 'runtime');
  await mkdir(runtimeRoot, { recursive: true });
  await writeFile(
    join(workspace, 'node_modules', 'kafka-typegen', 'package.json'),
    JSON.stringify({
      exports: {
        './runtime': {
          default: './runtime/index.js',
          types: './runtime/index.d.ts'
        }
      },
      name: 'kafka-typegen',
      type: 'module'
    })
  );
  await writeFile(
    join(runtimeRoot, 'index.d.ts'),
    [
      'export interface RuntimeEventMetadata {',
      '  readonly eventName: string;',
      '  readonly payloadTypeName: string;',
      '  readonly schemaFilePath: string;',
      '  readonly schemaName: string;',
      '  readonly subjectName: string;',
      '  readonly topicName: string;',
      '}',
      'export interface RuntimeProducer<TSendOptions = unknown> {',
      '  send(metadata: RuntimeEventMetadata, payload: unknown, options?: TSendOptions): Promise<void>;',
      '}',
      'export interface RuntimeConsumer<TSubscriptionOptions = unknown> {',
      '  on<TPayload>(metadata: RuntimeEventMetadata, handler: (message: { payload: TPayload }) => Promise<void> | void, options?: TSubscriptionOptions): Promise<void>;',
      '  onTopic<TPayload>(topicName: string, metadataByEvent: Readonly<Record<string, RuntimeEventMetadata>>, handler: (message: { payload: TPayload }) => Promise<void> | void, options?: TSubscriptionOptions): Promise<void>;',
      '}',
      'export interface RuntimeClient<TSendOptions = unknown, TSubscriptionOptions = unknown> {',
      '  readonly consumer: RuntimeConsumer<TSubscriptionOptions>;',
      '  readonly producer: RuntimeProducer<TSendOptions>;',
      '}',
      ''
    ].join('\n')
  );
  await writeFile(join(runtimeRoot, 'index.js'), 'export {};\n');
}

afterEach(async () => {
  for (const tempDir of tempDirs.splice(0, tempDirs.length)) {
    await rm(tempDir, { force: true, recursive: true });
  }
});

describe('generated package wrapper', () => {
  it('can be consumed through package-name imports by a NodeNext TypeScript project', async () => {
    const workspace = await createWorkspace();
    await writeGeneratedPackage(workspace);

    const appPath = join(workspace, 'app.ts');
    await writeFile(
      join(workspace, 'package.json'),
      JSON.stringify({
        type: 'module'
      })
    );
    await writeFile(
      appPath,
      [
        "import { EventNames, createProducer, type UserCreatedPayload } from '@acme/generated-kafka';",
        '',
        'const producer = createProducer({',
        '  async send() {}',
        '});',
        '',
        'const payload: UserCreatedPayload = {',
        "  email: 'ada@example.com',",
        "  id: 'user_1',",
        '  isAdmin: true',
        '};',
        '',
        'await producer.send(EventNames.UserCreated, payload);',
        'await producer.events.userCreated.send(payload);',
        ''
      ].join('\n')
    );

    const program = ts.createProgram([appPath], {
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      noEmit: true,
      strict: true,
      target: ts.ScriptTarget.ES2022
    });

    expect(
      ts.formatDiagnosticsWithColorAndContext(
        ts.getPreEmitDiagnostics(program),
        {
          getCanonicalFileName: (fileName) => fileName,
          getCurrentDirectory: () => workspace,
          getNewLine: () => '\n'
        }
      )
    ).toBe('');
  });
});
