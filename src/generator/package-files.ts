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

export function emitGeneratedPackageFile(catalog: EventCatalog): GeneratedFile | undefined {
  const packageName = catalog.config.generation.packageName;
  if (packageName === undefined) {
    return undefined;
  }

  return {
    contents: `${JSON.stringify(
      {
        name: packageName,
        private: true,
        type: 'module',
        types: './index.ts',
        exports: {
          '.': './index.ts'
        }
      },
      null,
      2
    )}\n`,
    filePath: 'package.json'
  };
}
