# Execution Plan

## 1. Current Step

- Step 5: Type Generation.
- This is the correct next step because Step 4 now provides a canonical catalog with stable identifiers, parsed schemas, and runtime metadata. The next missing capability is converting that catalog into deterministic generated TypeScript types.

## 2. Repository Assessment

### What already exists

- Normalized config and parsed Avro schema metadata.
- Canonical event catalog with payload type names, topic type names, runtime metadata, and deterministic ordering.
- A placeholder generator contract in `src/generator/types.ts`.

### What is missing

- Concrete type generator implementation.
- Generated payload types from Avro record schemas.
- Generated unions/helper types for events and topics.
- Deterministic file output with readable TypeScript source.
- Generator tests verifying output shape and ordering.

### Constraints and risks

- Step 5 should only generate types, not producer/consumer APIs.
- Generated source should stay readable and deterministic.
- Avro-to-TypeScript conversion needs to be useful now without overcommitting to every advanced Avro construct.

## 3. Architecture Decisions (Step 5 only)

### Modules/files to introduce or update

- `src/generator/types.ts`
  - Expand generator contracts if needed for concrete implementation.
- `src/generator/type-generator.ts`
  - Implement the catalog-to-TypeScript generator.
- `src/generator/index.ts`
  - Export the concrete generator.
- `tests/generator.test.ts`
  - Add golden-output tests for single-event and multi-event catalogs.
- `tests/fixtures/generated/`
  - Store expected generated TypeScript output.

### Responsibilities

- Generator: map parsed Avro schema fields into TypeScript property types, emit named exports, union/helper types, and runtime metadata typings.
- Tests: verify exact output shape, deterministic ordering, and single/multi-event scenarios.

### External libraries planned

- No new dependency is necessary for Step 5. The output is small enough to generate with disciplined string emission.

## 4. Task Breakdown

- Task 1: design the generated artifact shape and helper types based on the catalog.
- Task 2: implement Avro field-type to TypeScript type conversion for the schema constructs already covered by fixtures.
- Task 3: implement deterministic file emission in the generator module.
- Task 4: add exact-output tests for single-event and multi-event catalogs.
- Task 5: run tests, typecheck, and build until the step is stable.

## 5. File Changes Plan

### Existing files to modify

- `codex.plan.md`
- `src/generator/types.ts`
- `src/generator/index.ts`
- `tests/exports.test.ts`

### New files expected

- `src/generator/type-generator.ts`
- `tests/generator.test.ts`
- `tests/fixtures/generated/single-event.ts`
- `tests/fixtures/generated/multi-event.ts`

## 6. Testing Plan

- Golden/exact output verification for:
  - single-event topic generation
  - multi-event topic generation
- Deterministic ordering checks.
- Verification:
  - `pnpm test`
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- Avro logical types and advanced named-type references are not fully solved in this step; assumption: current generation should support the schema shapes already parsed and remain extensible.
- Formatting is manual for now; if emission complexity grows in later steps, we may want to switch to an AST/code-formatting helper then.
