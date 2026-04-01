# Execution Plan

## 1. Current Step

- Step 7: Generated Consumer API.
- This is the correct next step because Step 6 already emits the producer-facing generated client and runtime metadata constants. The next missing capability is the consumer-facing API that uses the same generated metadata and payload typings.

## 2. Repository Assessment

### What already exists

- Canonical catalog with event and topic metadata.
- Generated payload types, metadata maps, and producer API.
- Minimal runtime consumer contract.

### What is missing

- Event-first generated consumer API.
- Topic-based generated consumer API with discriminated unions.
- Typed decoded message shapes carrying useful metadata.
- Tests proving the generated API shape and runtime metadata routing.

### Constraints and risks

- Step 7 should not implement transport deserialization logic yet.
- The generated consumer API should stay readable and align with the producer style from Step 6.
- Topic-based unions should be useful without overcomplicating the first version.

## 3. Architecture Decisions (Step 7 only)

### Modules/files to introduce or update

- `src/runtime/types.ts`
  - Expand the runtime consumer message shape to include useful metadata fields and align naming with generated code.
- `src/runtime/index.ts`
  - Re-export the updated runtime consumer types.
- `src/generator/type-generator.ts`
  - Extend the generated file with consumer message types and consumer factory/types.
- `tests/generator.test.ts`
  - Update golden-output expectations for the generated consumer API.
- `tests/runtime-consumer.test.ts`
  - Add execution tests covering event-first and topic-based consumer registration.
- `tests/fixtures/generated/`
  - Refresh expected generated output fixtures.

### Responsibilities

- Runtime types: define the narrow generated/runtime handshake for consumer handlers.
- Generator: emit event-first `on(event, handler)` plus topic-based `onTopic(topic, handler)` APIs and typed message shapes.
- Runtime tests: verify metadata routing and handler registration against a mocked runtime consumer.

### External libraries planned

- No new dependencies are required.

## 4. Task Breakdown

- Task 1: extend runtime consumer metadata/message contracts for generated handlers.
- Task 2: extend the generated file with consumer message types and factory APIs.
- Task 3: update golden fixtures for single-event and multi-event consumer-aware output.
- Task 4: add runtime execution tests for event-first and topic-based consumer registration.
- Task 5: run tests, typecheck, and build until the step is stable.

## 5. File Changes Plan

### Existing files to modify

- `codex.plan.md`
- `src/runtime/types.ts`
- `src/runtime/index.ts`
- `src/generator/type-generator.ts`
- `tests/generator.test.ts`
- `tests/fixtures/generated/single-event.ts`
- `tests/fixtures/generated/multi-event.ts`
- `tests/exports.test.ts`

### New files expected

- `tests/runtime-consumer.test.ts`

## 6. Testing Plan

- Golden/exact output verification for generated consumer API shape.
- Runtime execution tests for:
  - `consumer.on('event', handler)` metadata routing
  - `consumer.onTopic('topic', handler)` discriminated union behavior
- Verification:
  - `pnpm test`
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- Some message metadata fields like partition/offset/schemaId are placeholders until Step 8 runtime integration. Assumption: include them in the generated types now as optional fields so the API stays forward-compatible.
- Topic-based unions rely on the current event/topic catalog ordering; that ordering is already deterministic, so the generated union should stay stable.
