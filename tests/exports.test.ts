import { describe, expect, it } from 'vitest';

import {
  defineConfig,
  resolveConfig,
  validateConfig,
  type CatalogBuilder,
  type RuntimeProducer,
  type SchemaLoader,
  type TypeGenerator
} from '../src/index.js';

describe('public entrypoint', () => {
  it('exposes the Step 1 foundation exports', () => {
    expect(defineConfig).toBeTypeOf('function');
    expect(resolveConfig).toBeTypeOf('function');
    expect(validateConfig).toBeTypeOf('function');
  });

  it('supports importing architectural placeholder contracts', () => {
    const schemaLoader: SchemaLoader = {
      async load(source) {
        return {
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

    expect(schemaLoader.load).toBeTypeOf('function');
    expect(runtimeProducer.send).toBeTypeOf('function');
    expect(generator.generate).toBeTypeOf('function');
    expect(catalogBuilder.build).toBeTypeOf('function');
  });
});
