import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve as resolvePath } from 'node:path';

import type { GeneratedFile } from '../generator/types.js';

export async function writeGeneratedFiles(outputDir: string, files: readonly GeneratedFile[]): Promise<void> {
  await mkdir(outputDir, { recursive: true });

  await Promise.all(
    files.map(async (file) => {
      const targetPath = resolvePath(outputDir, file.filePath);
      await mkdir(dirname(targetPath), { recursive: true });
      await writeFile(targetPath, file.contents, 'utf8');
    })
  );
}
