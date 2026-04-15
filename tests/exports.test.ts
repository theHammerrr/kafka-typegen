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
  it('exposes the config helpers', async () => {
    const rootModule = await import('../src/index.js');

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
    expect('createKafkaJsProducerTransport' in rootModule).toBe(false);
    expect('createKafkaJsConsumerTransport' in rootModule).toBe(false);
    expect('createPlatformaticProducerTransport' in rootModule).toBe(false);
    expect('createPlatformaticConsumerTransport' in rootModule).toBe(false);
    expect('createSchemaRegistrySerialization' in rootModule).toBe(false);
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
    const rootModule = await import('../src/index.js');
    const advancedRuntimeModule = await import('../src/runtime/advanced.js');
    const kafkaJsRuntimeModule = await import('../src/runtime/kafkajs.js');
    const platformaticRuntimeModule = await import('../src/runtime/platformatic.js');
    const packageJson = JSON.parse(await readFile('package.json', 'utf8')) as {
      exports: Record<string, unknown>;
    };

    expect(runtimeModule.createRuntimeClient).toBeTypeOf('function');
    expect(runtimeModule.createRuntimeProducer).toBeTypeOf('function');
    expect(runtimeModule.createRuntimeConsumer).toBeTypeOf('function');
    expect(runtimeModule.createConfluentSchemaRegistryRuntimeClient).toBeTypeOf('function');
    expect(runtimeModule.createKafkaJsRuntimeClient).toBeTypeOf('function');
    expect(runtimeModule.createKafkaJsRuntimeProducer).toBeTypeOf('function');
    expect(runtimeModule.createKafkaJsRuntimeConsumer).toBeTypeOf('function');
    expect(runtimeModule.createPlatformaticRuntimeClient).toBeTypeOf('function');
    expect(runtimeModule.createPlatformaticRuntimeProducer).toBeTypeOf('function');
    expect(runtimeModule.createPlatformaticRuntimeConsumer).toBeTypeOf('function');
    expect(rootModule.createKafkaJsRuntimeClient).toBeTypeOf('function');
    expect(rootModule.createPlatformaticRuntimeClient).toBeTypeOf('function');
    expect(advancedRuntimeModule.createKafkaJsProducerTransport).toBeTypeOf('function');
    expect(advancedRuntimeModule.createKafkaJsConsumerTransport).toBeTypeOf('function');
    expect(platformaticRuntimeModule.createPlatformaticRuntimeClient).toBeTypeOf('function');
    expect(platformaticRuntimeModule.createPlatformaticRuntimeProducer).toBeTypeOf('function');
    expect(platformaticRuntimeModule.createPlatformaticRuntimeConsumer).toBeTypeOf('function');
    expect(kafkaJsRuntimeModule.createKafkaJsRuntimeClient).toBeTypeOf('function');
    expect(kafkaJsRuntimeModule.createKafkaJsRuntimeProducer).toBeTypeOf('function');
    expect(kafkaJsRuntimeModule.createKafkaJsRuntimeConsumer).toBeTypeOf('function');
    expect(advancedRuntimeModule.createPlatformaticProducerTransport).toBeTypeOf('function');
    expect(advancedRuntimeModule.createPlatformaticConsumerTransport).toBeTypeOf('function');
    expect(advancedRuntimeModule.createSchemaRegistrySerialization).toBeTypeOf('function');
    expect('createPlatformaticProducerTransport' in runtimeModule).toBe(false);
    expect('createPlatformaticConsumerTransport' in runtimeModule).toBe(false);
    expect('createSchemaRegistrySerialization' in runtimeModule).toBe(false);
    expect('createKafkaJsProducerTransport' in rootModule).toBe(false);
    expect('createKafkaJsConsumerTransport' in rootModule).toBe(false);
    expect('createPlatformaticProducerTransport' in rootModule).toBe(false);
    expect('createPlatformaticConsumerTransport' in rootModule).toBe(false);
    expect('createSchemaRegistrySerialization' in rootModule).toBe(false);
    expect('createPlatformaticProducerTransport' in platformaticRuntimeModule).toBe(false);
    expect('createPlatformaticConsumerTransport' in platformaticRuntimeModule).toBe(false);
    expect(packageJson.exports['./runtime']).toBeDefined();
    expect(packageJson.exports['./runtime/advanced']).toBeDefined();
    expect(packageJson.exports['./runtime/kafkajs']).toBeDefined();
    expect(packageJson.exports['./runtime/platformatic']).toBeDefined();
  });
});
