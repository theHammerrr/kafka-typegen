import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createRuntimeConsumer,
  createRuntimeClient,
  createRuntimeProducer,
  createSyncClients,
  createTypeGenerator,
  defineConfig,
  executeSync,
  resolveConfig,
  validateConfig,
  type CatalogBuilder,
  type RuntimeProducer,
  type SchemaLoader,
  type SubjectNameStrategy,
  type TypeGenerator
} from '../src/index.js';

describe('public entrypoint', () => {
  it('exposes the config helpers', () => {
    expect(createCatalogBuilder).toBeTypeOf('function');
    expect(createRuntimeConsumer).toBeTypeOf('function');
    expect(createRuntimeClient).toBeTypeOf('function');
    expect(createRuntimeProducer).toBeTypeOf('function');
    expect(createSyncClients).toBeTypeOf('function');
    expect(createTypeGenerator).toBeTypeOf('function');
    expect(defineConfig).toBeTypeOf('function');
    expect(executeSync).toBeTypeOf('function');
    expect(resolveConfig).toBeTypeOf('function');
    expect(validateConfig).toBeTypeOf('function');
  });

  it('exposes Step 2 config-oriented types alongside architecture contracts', () => {
    const strategy: SubjectNameStrategy = 'topic-event';

    const schemaLoader: SchemaLoader = {
      async load(source) {
        return {
          filePath: source.filePath,
          rawSchema: '{}',
          source
        };
      }
    };

    const runtimeProducer: RuntimeProducer = {
      async send() {}
    };

    const generator: TypeGenerator = {
      async generate() {
        return { files: [] };
      }
    };

    const catalogBuilder: CatalogBuilder = {
      async build(config) {
        return {
          config,
          events: [],
          topics: []
        };
      }
    };

    expect(strategy).toBe('topic-event');
    expect(schemaLoader.load).toBeTypeOf('function');
    expect(runtimeProducer.send).toBeTypeOf('function');
    expect(generator.generate).toBeTypeOf('function');
    expect(catalogBuilder.build).toBeTypeOf('function');
  });

  it('ships runtime subpath exports for generic and platformatic runtimes', async () => {
    const runtimeModule = await import('../src/runtime/index.js');
    const platformaticRuntimeModule = await import('../src/runtime/platformatic.js');
    const packageJson = JSON.parse(await readFile('package.json', 'utf8')) as {
      exports: Record<string, unknown>;
    };

    expect(runtimeModule.createRuntimeClient).toBeTypeOf('function');
    expect(runtimeModule.createRuntimeProducer).toBeTypeOf('function');
    expect(runtimeModule.createRuntimeConsumer).toBeTypeOf('function');
    expect(runtimeModule.createConfluentSchemaRegistryRuntimeClient).toBeTypeOf('function');
    expect(runtimeModule.createPlatformaticRuntimeClient).toBeTypeOf('function');
    expect(runtimeModule.createPlatformaticRuntimeProducer).toBeTypeOf('function');
    expect(runtimeModule.createPlatformaticRuntimeConsumer).toBeTypeOf('function');
    expect(runtimeModule.createPlatformaticProducerTransport).toBeTypeOf('function');
    expect(runtimeModule.createPlatformaticConsumerTransport).toBeTypeOf('function');
    expect(platformaticRuntimeModule.createPlatformaticRuntimeClient).toBeTypeOf('function');
    expect(platformaticRuntimeModule.createPlatformaticRuntimeProducer).toBeTypeOf('function');
    expect(platformaticRuntimeModule.createPlatformaticRuntimeConsumer).toBeTypeOf('function');
    expect(platformaticRuntimeModule.createPlatformaticProducerTransport).toBeTypeOf('function');
    expect(platformaticRuntimeModule.createPlatformaticConsumerTransport).toBeTypeOf('function');
    expect(packageJson.exports['./runtime']).toBeDefined();
    expect(packageJson.exports['./runtime/platformatic']).toBeDefined();
  });
});
