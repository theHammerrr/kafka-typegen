# Execution Plan

## 1. Current Step

- Step 3: Avro Schema Loading and Parsing.
- This is the correct next step because Step 2 now produces a normalized config with resolved schema paths for events. The next dependency for generation is the ability to load those schema files, parse them safely, and normalize their metadata into a generator-friendly shape.

## 2. Repository Assessment

### What already exists

- Strict TypeScript build/test setup and verified Step 1/2 branches.
- A normalized config model with resolved event schema paths and deterministic ordering.
- Placeholder schema interfaces in `src/schema/types.ts` and exports in `src/schema/index.ts`.

### What is missing

- Concrete schema file loader implementation.
- Concrete Avro parser integration.
- Normalized schema metadata rich enough for later catalog and generation steps.
- Schema-specific error types for missing files and malformed/unsupported schemas.
- Tests and fixtures covering load/parse success and failure cases.

### Constraints and risks

- Step 3 must keep loading separate from parsing.
- Scope should stay on event value schemas; key schemas and references only need future-safe contracts, not full support.
- Top-level schema support should be intentionally limited to Avro records with clear failure messages for unsupported roots.

## 3. Architecture Decisions (Step 3 only)

### Modules/files to introduce or update

- `src/schema/types.ts`
  - Expand the schema contracts to include field metadata, schema collection results, and structured errors.
- `src/schema/errors.ts`
  - Define focused error classes for load and parse failures.
- `src/schema/loader.ts`
  - Filesystem-backed schema loader that reads UTF-8 schema files.
- `src/schema/parser.ts`
  - Avro parser integration plus normalization into record/field metadata.
- `src/schema/index.ts`
  - Export the new contracts and concrete helpers.
- `tests/schema.test.ts`
  - Step 3 behavioral tests.
- `tests/fixtures/schemas/`
  - Valid and invalid schema fixtures.

### Responsibilities

- Loader: resolve file reading concerns and surface actionable file-path errors.
- Parser: validate Avro shape, enforce top-level record support, and normalize field metadata.
- Schema module facade: provide a simple entrypoint for loading one schema or many schemas from normalized event config.

### External libraries planned

- `avsc`: mature Avro library for parsing/validating schema definitions instead of handwritten parsing logic.

## 4. Task Breakdown

- Task 1: add the Avro dependency and extend schema types/contracts.
- Task 2: implement schema loader error handling and UTF-8 file loading.
- Task 3: implement Avro parsing and normalized record metadata extraction.
- Task 4: add helper flow(s) for loading/parsing one schema and multiple event schemas.
- Task 5: add fixture-backed tests for valid load, malformed schema, missing file, normalized metadata extraction, and multiple-schema loading.
- Task 6: run tests, typecheck, and build until the step is stable.

## 5. File Changes Plan

### Existing files to modify

- `codex.plan.md`
- `package.json`
- `pnpm-lock.yaml`
- `src/schema/types.ts`
- `src/schema/index.ts`

### New files expected

- `src/schema/errors.ts`
- `src/schema/loader.ts`
- `src/schema/parser.ts`
- `tests/schema.test.ts`
- `tests/fixtures/schemas/user-created.avsc`
- `tests/fixtures/schemas/user-updated.avsc`
- `tests/fixtures/schemas/invalid-json.avsc`
- `tests/fixtures/schemas/invalid-root.avsc`

## 6. Testing Plan

- Unit/integration-style schema tests:
  - successful load of a valid schema file
  - malformed schema handling
  - missing file handling
  - normalized metadata extraction for record name, namespace, and fields
  - multiple schemas across multiple topics using normalized Step 2 config
- Verification:
  - `pnpm test`
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- `avsc` error messages can be low-level; I will wrap them in project-specific errors while preserving the root cause text.
- Field type normalization needs to stay useful without solving every Avro edge case; assumption: a readable string summary plus the raw field type is enough for Step 3.
- Reference/import support is intentionally deferred, but the parser contracts should not prevent adding it later.
