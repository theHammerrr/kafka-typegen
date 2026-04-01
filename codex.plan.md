# Execution Plan

## 1. Current Step

- Step 10: End-to-End Quality Pass.
- This is the correct next step because the core config, schema, catalog, generation, runtime, and CLI flows already exist. The remaining work is to harden the full developer experience, close critical test gaps, and polish rough edges rather than introducing another major subsystem.

## 2. Repository Assessment

### What already exists

- Config validation and normalization with deterministic ordering.
- Schema loading/parsing, catalog building, generated types, producer/consumer/client APIs, runtime integration, and a CLI.
- Unit and integration-style coverage for most individual subsystems.

### What is missing

- True end-to-end coverage that exercises config -> schema load -> catalog -> generation as one coherent flow.
- Multi-event CLI/output coverage and stronger failure-path assertions around user-facing errors.
- Final polish for generated output readability and CLI/build ergonomics.

### Constraints and risks

- Step 10 should not introduce speculative new features; it should harden the current surface.
- Cleanup needs to stay targeted so we do not destabilize previously completed steps.
- Any API polish must preserve the existing typed-client direction and keep output deterministic.

## 3. Architecture Decisions (Step 10 only)

### Modules/files to introduce or update

- `codex.plan.md`
  - Record the scoped quality-pass plan before implementation.
- `tests/e2e.test.ts`
  - Add full pipeline end-to-end coverage for single-event, multi-event, and failure flows.
- `tests/cli.test.ts`
  - Expand CLI assertions where current user-facing behavior is underspecified.
- `src/cli.ts`
  - Improve argument validation and error messages where the current UX is weak.
- `src/generator/type-generator.ts`
  - Apply generated output readability or configurability polish if the end-to-end tests expose issues.
- `tsup.config.ts`
  - Remove the current CLI build rough edge if a minimal packaging adjustment can resolve it cleanly.

### Responsibilities

- End-to-end tests: assert that the complete generation pipeline is coherent for both single-event and multi-event topics.
- CLI: provide clearer failure messages and predictable invocation behavior.
- Generator/build polish: keep generated output readable and packaging stable without changing the product scope.

### External libraries planned

- No new dependencies are planned. The existing stack is sufficient for the Step 10 hardening pass.

## 4. Task Breakdown

- Task 1: add end-to-end tests for single-event and multi-event full-pipeline generation.
- Task 2: add failure-path end-to-end coverage with clear message assertions.
- Task 3: tighten CLI argument validation and user-facing errors based on those tests.
- Task 4: clean up any generator or packaging rough edges revealed by the quality pass.
- Task 5: run `pnpm test`, `pnpm typecheck`, and `pnpm build` until the step is stable.

## 5. File Changes Plan

### Existing files to modify

- `codex.plan.md`
- `src/cli.ts`
- `src/generator/type-generator.ts`
- `tests/cli.test.ts`
- `tsup.config.ts`

### New files expected

- `tests/e2e.test.ts`

## 6. Testing Plan

- End-to-end tests for:
  - full config -> schema -> catalog -> generation flow
  - single-event topic output
  - multi-event topic output
  - failure paths with actionable messages
- Verification:
  - `pnpm test`
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- The current CLI accepts `--config` without validating a missing value; this likely needs tightening.
- The generated file currently hardcodes the runtime import module. If the configurable runtime module is meant to affect generated imports, Step 10 is the last reasonable place to align that behavior.
- The current build emits a non-blocking `import.meta` warning for the CLI CJS output. I will only change packaging if it can be fixed without complicating the distribution model.
