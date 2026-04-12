import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    hookTimeout: 600_000,
    include: ['tests/integration/**/*.test.ts'],
    testTimeout: 180_000
  }
});
