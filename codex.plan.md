# Execution Plan

## 1. Current Step

- Step 6: Generated Producer API.
- This is the correct next step because Step 5 now emits payload and metadata typings from the catalog. The next missing capability is an actual producer-facing client API that uses those generated types and wires event metadata into the runtime producer contract.

## 2. Repository Assessment

### What already exists

- Canonical event catalog with payload type names and runtime event metadata.
- Deterministic type generation into a single TypeScript file.
- Minimal runtime producer contract in `src/runtime/types.ts`.

### What is missing

- Event-first producer API generation.
- Typed `send(event, payload)` surface.
- Optional grouped event helpers.
- Generated runtime metadata constants wiring each event to topic/schema metadata.
- Tests that verify generated API shape and runtime metadata routing.

### Constraints and risks

- Step 6 should not implement consumer APIs yet.
- Runtime coupling should remain narrow so Step 8 can evolve the transport layer later.
- The generated file should stay readable rather than collapsing into opaque helper code.

## 3. Architecture Decisions (Step 6 only)

### Modules/files to introduce or update

- `src/runtime/types.ts`
  - Expand the producer contract to carry richer generated metadata while keeping the interface small.
- `src/runtime/index.ts`
  - Re-export the updated runtime producer types.
- `src/generator/type-generator.ts`
  - Extend the generated file with producer metadata constants and producer client factory/types.
- `tests/generator.test.ts`
  - Replace the Step 5 golden fixtures with Step 6 producer-aware output checks.
- `tests/runtime-producer.test.ts`
  - Execute the generated producer API against a mocked runtime producer to verify metadata routing and event-to-topic behavior.
- `tests/fixtures/generated/`
  - Update expected generated output fixtures.

### Responsibilities

- Runtime types: define the minimal generated/runtime handshake.
- Generator: emit producer metadata plus the event-first API and grouped helper API.
- Runtime test: verify that calling generated producer methods routes the expected metadata and payload.

### External libraries planned

- No new dependencies are required. The generated file can be transpiled in tests using the existing `typescript` dependency.

## 4. Task Breakdown

- Task 1: expand the runtime producer metadata contract to match the catalog data already available.
- Task 2: extend the generated file with event metadata constants and producer client types/factory.
- Task 3: update golden fixtures for single-event and multi-event producer output.
- Task 4: add runtime execution tests that transpile and invoke the generated producer API.
- Task 5: run tests, typecheck, and build until the step is stable.

## 5. File Changes Plan

### Existing files to modify

- `codex.plan.md`
- `src/runtime/types.ts`
- `src/runtime/index.ts`
- `src/generator/type-generator.ts`
- `tests/generator.test.ts`
- `tests/exports.test.ts`
- `tests/fixtures/generated/single-event.ts`
- `tests/fixtures/generated/multi-event.ts`

### New files expected

- `tests/runtime-producer.test.ts`

## 6. Testing Plan

- Golden/exact output verification for generated producer API shape.
- Runtime execution tests for:
  - `producer.send('event', payload)` metadata routing
  - grouped `producer.events.someEvent.send(payload)` behavior
  - event-to-topic mapping correctness
- Verification:
  - `pnpm test`
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- Generating grouped helper property names requires a stable identifier transform. Assumption: camelCase event names derived from the existing event identifiers is sufficient for v1.
- The runtime contract will likely grow for headers/keys later, but Step 6 should keep the current payload-only API narrow and explicit.
