import type { EventCatalog } from '../catalog/index.js';

import type { GeneratedFile } from './types.js';

export function emitGeneratedIndexFile(catalog: EventCatalog): GeneratedFile | undefined {
  const filePath = catalog.config.generation.typesFileName;
  if (filePath === 'index.ts') {
    return undefined;
  }

  const exportPath = filePath.endsWith('.ts') ? `./${filePath.slice(0, -3)}.js` : `./${filePath}`;

  return {
    contents: `export * from '${exportPath}';\n`,
    filePath: 'index.ts'
  };
}
