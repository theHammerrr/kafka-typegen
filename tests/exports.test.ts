import { describe, expect, it } from 'vitest';

import {
  defineConfig,
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
    expect(defineConfig).toBeTypeOf('function');
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
          events: []
        };
      }
    };

    expect(strategy).toBe('topic-event');
    expect(schemaLoader.load).toBeTypeOf('function');
    expect(runtimeProducer.send).toBeTypeOf('function');
    expect(generator.generate).toBeTypeOf('function');
    expect(catalogBuilder.build).toBeTypeOf('function');
  });
});
