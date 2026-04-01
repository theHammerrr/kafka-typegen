# Execution Plan

## 1. Current Step

- Step 1: Project Foundation and Architecture.
- This is the correct starting point because the repository currently contains only the instruction files and a minimal `README.md`. There is no source tree, no package manifest, no TypeScript setup, no test runner, and no existing generator/runtime/config implementation to extend. Step 1 explicitly covers establishing those foundations and placeholder abstractions without prematurely implementing later-step behavior.

## 2. Repository Assessment

### What already exists

- Repository root with `.gitignore`, `README.md`, and `instructions/`.
- Full step-by-step implementation instructions from Step 1 through Step 10.

### What is missing

- Package manager manifest and scripts.
- TypeScript configuration and build output strategy.
- Test runner and test configuration.
- Source directory layout.
- Any implementation for config, schema loading, catalog/modeling, generator, runtime, or CLI.
- Public entrypoints and exports.

### Constraints and risks

- Step 1 must avoid deep implementation of generation, schema parsing, Kafka runtime logic, or CLI behavior.
- The initial architecture needs to make later steps straightforward without introducing speculative complexity.
- Since the repository is empty, the first pass must define conventions that will influence every later step: module boundaries, export surfaces, validation strategy, and test/build tooling.
- The instruction files are currently untracked in git; implementation should avoid disturbing unrelated repository state.

## 3. Architecture Decisions (Step 1 only)

### Modules/files to introduce

- `src/config/`
  - User config types, validation scaffold, normalization scaffold, and public `defineConfig(...)` helper.
- `src/schema/`
  - Interfaces for schema file loading/parsing and placeholder result types.
- `src/catalog/`
  - Internal normalized metadata shapes that future steps will populate.
- `src/generator/`
  - Generator context/output interfaces and a placeholder entrypoint.
- `src/runtime/`
  - Runtime contract types and minimal helper placeholders used by generated code later.
- `src/index.ts`
  - Root public exports.
- `tests/`
  - Config validation tests, import smoke tests, and architecture-level sanity coverage.

### Responsibilities

- Keep config validation isolated from schema and generation concerns.
- Keep schema contracts independent from concrete Avro parsing until Step 3.
- Keep catalog types focused on the internal canonical model, even if Step 1 only provides skeleton shapes.
- Keep runtime contracts separate from generated output so future codegen can target stable runtime interfaces.

### External libraries planned

- `typescript`: compiler and typechecking.
- `vitest`: unit/smoke testing with a lightweight TypeScript-friendly setup.
- `zod`: explicit runtime validation for the config scaffold, with room for clear error messages and normalization in later steps.
- `tsup`: simple library build pipeline for emitting distributable JavaScript and type declarations without overbuilding the toolchain.

These choices are justified because they keep the setup small, established, and compatible with the project’s TypeScript-first goals.

## 4. Task Breakdown

- Task 1: initialize the package manifest, scripts, and dependency set for build, test, and typecheck.
- Task 2: add base TypeScript and build configuration.
- Task 3: create the top-level source folder structure and module entrypoints.
- Task 4: implement the config scaffold with strong types, `defineConfig(...)`, and minimal runtime validation.
- Task 5: add placeholder contracts/interfaces for schema, catalog, generator, and runtime modules.
- Task 6: wire root exports to expose the intended public API surface for this step.
- Task 7: add tests covering config validation basics and import/entrypoint smoke behavior.
- Task 8: run tests and typecheck, then adjust any API or structure issues discovered.

## 5. File Changes Plan

### New files expected

- `package.json`
- `tsconfig.json`
- `tsconfig.build.json`
- `vitest.config.ts`
- `tsup.config.ts`
- `src/index.ts`
- `src/config/index.ts`
- `src/config/types.ts`
- `src/config/schema.ts`
- `src/config/define-config.ts`
- `src/schema/index.ts`
- `src/schema/types.ts`
- `src/catalog/index.ts`
- `src/catalog/types.ts`
- `src/generator/index.ts`
- `src/generator/types.ts`
- `src/runtime/index.ts`
- `src/runtime/types.ts`
- `tests/config.test.ts`
- `tests/exports.test.ts`

### Existing files likely to modify

- `README.md`
  - Only if a minimal usage/install note becomes necessary for clarity after the scaffold is in place.

## 6. Testing Plan

- Unit tests for config scaffold validation:
  - accepts a minimal valid config shape
  - rejects clearly invalid config input
  - preserves the `defineConfig(...)` helper ergonomics
- Smoke tests for architecture/public API:
  - root entrypoint exports expected modules/functions
  - placeholder subsystem contracts can be imported cleanly
- Typecheck validation:
  - run TypeScript compiler in no-emit mode to ensure the scaffold is strongly typed

For Step 1, integration tests and generated-output tests are unnecessary because generation/runtime behavior is intentionally not implemented yet.

## 7. Risks / Open Questions

- The exact long-term config shape will evolve in Step 2; Step 1 should only introduce the minimal validated scaffold needed to support that future work.
- `defineConfig(...)` is listed as a Step 2 deliverable, but Step 1 also requires a validated configuration scaffold. Assumption: introducing a minimal `defineConfig(...)` now is acceptable as a stable foundation, as long as the full user-facing config model is deferred to Step 2.
- Runtime and generator placeholders must be explicit enough to establish architecture without implying functionality that does not yet exist.
- `tsup` is a pragmatic build choice for now; if later steps need a different build strategy, the initial setup should remain easy to replace.
