# Execution Plan

## 1. Current Step

- Step 4: Internal Event Catalog / Model.
- This is the correct next step because Step 2 provides normalized config and Step 3 provides parsed schema metadata. The next missing layer is the canonical in-memory catalog that generation can consume directly.

## 2. Repository Assessment

### What already exists

- Normalized config with deterministic topic/event ordering and derived subject names.
- Schema loader/parser flow that can load event schemas from normalized config.
- Placeholder catalog types that are still too shallow for later generation work.

### What is missing

- Concrete catalog builder implementation.
- Canonical topic/event model containing config-derived metadata, parsed schemas, identifiers, and runtime-facing metadata.
- Validation for generated identifier naming collisions.
- Catalog tests covering valid construction and failure scenarios.

### Constraints and risks

- Step 4 should not start generating files yet.
- The catalog must remain deterministic and easy to consume from future generation steps.
- Identifier derivation needs to be explicit and collision-safe without introducing speculative complexity.

## 3. Architecture Decisions (Step 4 only)

### Modules/files to introduce or update

- `src/catalog/types.ts`
  - Expand the catalog model to include topics, events, runtime metadata, and generated identifier fields.
- `src/catalog/errors.ts`
  - Add focused catalog validation errors.
- `src/catalog/builder.ts`
  - Implement the catalog builder on top of normalized config + Step 3 schema loading.
- `src/catalog/index.ts`
  - Export the concrete builder and new types.
- `tests/catalog.test.ts`
  - Add Step 4 catalog coverage.
- `tests/exports.test.ts`
  - Keep the public export smoke test aligned with the richer catalog module.

### Responsibilities

- Catalog builder: orchestrate schema loading, derive identifiers, validate collisions, and emit the stable canonical model.
- Catalog types: represent the source of truth for later code generation and runtime metadata emission.
- Catalog errors: surface actionable inconsistencies in config/schema-derived naming.

### External libraries planned

- No new dependencies are required for this step.

## 4. Task Breakdown

- Task 1: redesign catalog types around canonical topic/event/runtime metadata.
- Task 2: implement identifier derivation and collision validation.
- Task 3: implement the builder using the Step 3 event schema loader.
- Task 4: add tests for valid single-event and multi-event catalog construction, deterministic ordering, and naming collision failures.
- Task 5: run tests, typecheck, and build until the step is stable.

## 5. File Changes Plan

### Existing files to modify

- `codex.plan.md`
- `src/catalog/types.ts`
- `src/catalog/index.ts`
- `tests/exports.test.ts`

### New files expected

- `src/catalog/errors.ts`
- `src/catalog/builder.ts`
- `tests/catalog.test.ts`

## 6. Testing Plan

- Catalog construction from valid config + schema fixtures.
- Deterministic topic and event ordering.
- Naming collision detection for generated identifiers.
- Single-event and multi-event topic scenarios.
- Verification:
  - `pnpm test`
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- The exact identifier scheme may evolve later, but Step 4 needs stable derivation now. Assumption: PascalCase topic/event names plus configured suffixes are the right canonical identifiers for v1.
- Schema metadata validation is intentionally light here; stronger semantic schema checks can be layered on later if generation needs them.
