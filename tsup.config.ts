import { defineConfig } from 'tsup';

export default defineConfig({
  clean: true,
  dts: true,
  entry: ['src/index.ts', 'src/cli.ts', 'src/runtime/index.ts', 'src/runtime/platformatic.ts'],
  format: ['esm', 'cjs'],
  sourcemap: true,
  target: 'node18'
});
