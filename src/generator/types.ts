import type { EventCatalog } from '../catalog/index.js';

export interface GeneratedFile {
  readonly contents: string;
  readonly filePath: string;
}

export interface GeneratorOutput {
  readonly files: readonly GeneratedFile[];
}

export interface TypeGenerator {
  generate(catalog: EventCatalog): Promise<GeneratorOutput>;
}
