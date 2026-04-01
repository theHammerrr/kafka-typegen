import { readFile } from 'node:fs/promises';

import { SchemaLoadError } from './errors.js';
import type { SchemaLoadResult, SchemaLoader, SchemaSource } from './types.js';

export class FileSystemSchemaLoader implements SchemaLoader {
  public async load(source: SchemaSource): Promise<SchemaLoadResult> {
    try {
      const rawSchema = await readFile(source.filePath, 'utf8');

      return {
        filePath: source.filePath,
        rawSchema,
        source
      };
    } catch (error) {
      throw new SchemaLoadError(
        source.filePath,
        `Failed to load schema file '${source.filePath}': ${error instanceof Error ? error.message : 'Unknown error.'}`,
        { cause: error }
      );
    }
  }
}

export function createSchemaLoader(): SchemaLoader {
  return new FileSystemSchemaLoader();
}
