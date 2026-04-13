import type { EventCatalog } from '../catalog/index.js';

import { emitAdvancedGeneratedFile } from './advanced-generator.js';
import { emitGeneratedIndexFile } from './package-files.js';
import { emitMinimalGeneratedFile } from './minimal-generator.js';
import type { GeneratedFile, GeneratorOutput, TypeGenerator } from './types.js';

function emitGeneratedFile(catalog: EventCatalog): GeneratedFile {
  return catalog.config.generation.apiMode === 'advanced'
    ? emitAdvancedGeneratedFile(catalog)
    : emitMinimalGeneratedFile(catalog);
}

export class DefaultTypeGenerator implements TypeGenerator {
  public async generate(catalog: EventCatalog): Promise<GeneratorOutput> {
    const indexFile = emitGeneratedIndexFile(catalog);

    return {
      files: [
        emitGeneratedFile(catalog),
        ...(indexFile !== undefined ? [indexFile] : [])
      ]
    };
  }
}

export function createTypeGenerator(): TypeGenerator {
  return new DefaultTypeGenerator();
}
