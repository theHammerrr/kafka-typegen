# Execution Plan

## 1. Current Step

- Step 2: Configuration Model and Validation.
- This is the correct next step because Step 1 established the project structure, public entrypoints, and a minimal config scaffold. The next missing capability is the real user-facing configuration contract that later schema loading, catalog building, and generation steps will depend on.

## 2. Repository Assessment

### What already exists

- TypeScript build/test setup with `tsup`, `vitest`, and strict compiler settings.
- Public module boundaries for `config`, `schema`, `catalog`, `generator`, and `runtime`.
- A small config scaffold with `defineConfig(...)`, validation, normalization, and basic tests.

### What is missing

- Full config model for topics, events, generation, naming, schema registry, runtime options, and source-root handling.
- Validation for duplicate event names across topics.
- Validation for invalid subject strategy / naming values.
- Normalized config output rich enough to support later steps.
- Tests covering single-event and multi-event topic scenarios under the fuller config shape.

### Constraints and risks

- Step 2 should not implement schema parsing or catalog building logic from later steps.
- The config model should stay explicit and stable without locking the project into an unnecessarily large surface area.
- The existing minimal config shape will need to evolve; tests and exports must be updated without weakening types.

## 3. Architecture Decisions (Step 2 only)

### Modules/files to introduce or update

- `src/config/types.ts`
  - Expand the public config types and normalized config types.
- `src/config/schema.ts`
  - Implement richer Zod validation, cross-reference validation, normalized path handling, and subject-name derivation.
- `src/config/define-config.ts`
  - Preserve `defineConfig(...)` as the ergonomic authoring helper and route `resolveConfig(...)` through the richer normalizer.
- `tests/config.test.ts`
  - Replace the scaffold-only tests with Step 2 coverage for valid and invalid configurations.
- `tests/exports.test.ts`
  - Keep a narrow public API smoke test aligned with the updated config contract.

### Responsibilities

- Public config types should be explicit and pleasant to author.
- Validation should cover both structural issues and semantic issues such as duplicate event names.
- Normalization should produce deterministic ordering plus derived metadata needed later, such as resolved schema paths and subject names.

### External libraries planned

- Continue using `zod` for runtime validation because it already fits the project and gives precise field-level errors.
- No new dependencies are needed for Step 2.

## 4. Task Breakdown

- Task 1: redesign the public config shape to cover output, sources, topics, events, runtime, schema registry, generation, and naming.
- Task 2: implement Zod schemas and semantic validation for duplicate event names and invalid config states.
- Task 3: implement deterministic normalized output, including resolved schema paths and derived subject names.
- Task 4: update the exported config helpers/types to match the richer model.
- Task 5: add Step 2 tests for valid single-event and multi-event configs, duplicate event rejection, missing schema paths, invalid strategies, and normalized output shape.
- Task 6: run tests, typecheck, and build; fix issues until the step is stable.

## 5. File Changes Plan

### Existing files to modify

- `codex.plan.md`
- `src/config/types.ts`
- `src/config/schema.ts`
- `src/config/define-config.ts`
- `src/config/index.ts`
- `tests/config.test.ts`
- `tests/exports.test.ts`

### New files expected

- None required for this step unless a small config utility file becomes necessary during implementation.

## 6. Testing Plan

- Unit tests for valid config authoring:
  - valid single-event topic config
  - valid multi-event topic config
- Unit tests for validation failures:
  - duplicate event names across topics
  - missing schema path
  - invalid subject strategy
- Unit tests for normalization:
  - deterministic topic/event ordering
  - resolved schema path output
  - derived subject names and event-to-topic mapping fields
- Typecheck and build:
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- Duplicate topic detection is not meaningful if topics remain keyed by object name; the plan is to move topics/events to arrays so duplicates can be validated explicitly and metadata can grow cleanly.
- Relative path resolution needs a clear base; assumption: `sources.rootDir` will default to `process.cwd()` during resolution and can be overridden explicitly.
- Subject naming needs to be useful now without pre-committing to registry-specific edge cases. Assumption: support a small strategy enum now and allow per-event overrides.
