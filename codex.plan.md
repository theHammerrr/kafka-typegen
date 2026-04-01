# Execution Plan

## 1. Current Step

- Step 9: CLI / Generator Entry Point.
- This is the correct next step because the generator pipeline now exists end-to-end in-process, but users still cannot invoke it through a standard command that loads config, runs generation, writes output, and reports failures.

## 2. Repository Assessment

### What already exists

- Config normalization and validation.
- Schema loading/parsing, canonical catalog building, type generation, and runtime integration.
- Tests covering the internal pipeline.

### What is missing

- CLI executable entrypoint.
- Config discovery/loading from a user config file.
- File writing step for generated output.
- CLI-facing error handling and output messages.
- CLI integration tests.

### Constraints and risks

- Step 9 should stay focused on generation, not on expanding runtime capabilities further.
- The CLI should be predictable and explicit; config discovery can exist, but it should not depend only on magic.
- The config loader must support a simple authoring format without introducing a heavy bundling dependency.

## 3. Architecture Decisions (Step 9 only)

### Modules/files to introduce or update

- `package.json`
  - Add a `bin` entry and CLI-friendly scripts if needed.
- `src/cli.ts`
  - Implement the command entrypoint, argument parsing, config discovery/loading, pipeline execution, and file writes.
- `src/index.ts`
  - Export CLI-adjacent helpers only if needed; otherwise keep CLI private.
- `tests/cli.test.ts`
  - Add CLI integration tests for config loading, generation execution, invalid config, and output paths.
- `tests/fixtures/cli/`
  - Add a sample config file and isolated output workspace fixtures.

### Responsibilities

- CLI: parse `--config`, support default config discovery, run the existing pipeline, write generated files to `outputDir`, and print actionable errors.
- Tests: spawn the CLI in a temp workspace and verify written output and failure behavior.

### External libraries planned

- No new dependency by default. The CLI can use built-in Node APIs plus dynamic module import for a JavaScript config file.

## 4. Task Breakdown

- Task 1: define the CLI invocation shape and config discovery behavior.
- Task 2: implement config file loading and pipeline execution.
- Task 3: implement generated file writes and helpful CLI output/errors.
- Task 4: add CLI integration tests for explicit config loading, default discovery, invalid config failure, and output path handling.
- Task 5: run tests, typecheck, and build until the step is stable.

## 5. File Changes Plan

### Existing files to modify

- `codex.plan.md`
- `package.json`

### New files expected

- `src/cli.ts`
- `tests/cli.test.ts`
- `tests/fixtures/cli/kafka-typegen.config.mjs`

## 6. Testing Plan

- CLI integration tests for:
  - config loading via `--config`
  - generation execution writing output files
  - invalid config failure behavior
  - output path behavior using the config’s `outputDir`
- Verification:
  - `pnpm test`
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- Config file module format needs to be explicit. Assumption: support `kafka-typegen.config.mjs` exporting either a default object or `defineConfig(...)` result.
- Since this repo currently emits a library bundle, the CLI should remain simple and use built-in Node features rather than a separate bundling system.
