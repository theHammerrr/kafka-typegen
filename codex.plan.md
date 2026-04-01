# Execution Plan

## 1. Current Step

- Issue 14: sync script for Kafka topics and Schema Registry subjects/schemas.
- This is the correct next step because generation, schema loading, catalog building, and runtime adapters already exist. What is missing is an operational sync path that provisions infrastructure from the same typed config.

## 2. Repository Assessment

### What already exists

- Config validation and normalization for topics, events, schema registry subject naming, and runtime settings.
- Schema loading/parsing with raw Avro schema preservation.
- A deterministic event catalog derived from normalized config and parsed schemas.
- A CLI that currently performs generation only.
- Tests covering config, schema, catalog, generator, runtime, CLI, and end-to-end generation flows.

### What is missing

- No sync-specific config for topic provisioning or schema registry synchronization.
- No Kafka admin or Schema Registry client abstractions.
- No CLI command for planning or applying infrastructure changes.
- README still states that the package does not create topics.

### Constraints and risks

- Keep generation and sync as separate commands so generation stays side-effect free.
- Default behavior should be dry-run and create-only; do not silently mutate existing remote resources.
- The implementation should stay strongly typed and avoid coupling sync to one runtime transport.

## 3. Architecture Decisions (Issue 14 only)

### Modules/files to introduce

- `src/sync/types.ts`
  - Shared sync config, plan, diff, and client interfaces.
- `src/sync/config.ts`
  - Sync config normalization helpers.
- `src/sync/topic-plan.ts`
  - Convert normalized config topics into desired Kafka topic definitions.
- `src/sync/schema-registry-plan.ts`
  - Convert catalog events into desired schema registry subject definitions.
- `src/sync/executor.ts`
  - Diff desired state against remote state and execute apply/dry-run behavior.
- `src/sync/clients.ts`
  - Factory and interfaces for Kafka admin and Schema Registry HTTP clients.
- `src/sync/index.ts`
  - Public sync exports.
- `src/cli.ts`
  - Add `generate` and `sync` command routing.
- `src/cli/args.ts`
  - Parse sync flags and command selection.
- `README.md`
  - Document sync behavior, config, and caveats.

### Existing files to modify

- `src/config/types.ts`
- `src/config/schemas.ts`
- `src/config/normalize.ts`
- `src/config/index.ts`
- `src/index.ts`
- `package.json`
- `tests/config.test.ts`
- `tests/cli.test.ts`
- `tests/e2e.test.ts`
- `tests/exports.test.ts`

### Responsibilities

- Config layer: validate and normalize optional sync settings without changing the generation flow.
- Sync planner: derive desired topic and schema registry state from normalized config and catalog.
- Sync executor: produce clear dry-run/apply results and fail explicitly on drift when requested.
- CLI: expose sync as a first-class command with safe defaults.

### External libraries planned

- `kafkajs`
  - Use its admin client for topic inspection and creation.
- Native `fetch`
  - Use simple HTTP calls for Schema Registry interaction; no extra registry client is necessary for the initial create-only workflow.

## 4. Task Breakdown

- Task 1: extend config types, validation, and normalization with optional sync settings.
- Task 2: add sync domain types and desired-state planners for topics and schema registry subjects.
- Task 3: add Kafka admin and Schema Registry client abstractions plus default implementations.
- Task 4: implement sync executor with dry-run, apply, target selection, and drift handling.
- Task 5: update CLI parsing and command routing to support `generate` and `sync`.
- Task 6: add config, planner, executor, CLI, and end-to-end tests.
- Task 7: update the README to document setup, usage, and current sync limitations.

## 5. File Changes Plan

### New files to create

- `src/sync/types.ts`
- `src/sync/config.ts`
- `src/sync/topic-plan.ts`
- `src/sync/schema-registry-plan.ts`
- `src/sync/executor.ts`
- `src/sync/clients.ts`
- `src/sync/index.ts`
- `tests/sync.test.ts`

### Existing files to modify

- `codex.plan.md`
- `src/cli.ts`
- `src/cli/args.ts`
- `src/config/types.ts`
- `src/config/schemas.ts`
- `src/config/normalize.ts`
- `src/config/index.ts`
- `src/index.ts`
- `package.json`
- `README.md`
- `tests/config.test.ts`
- `tests/cli.test.ts`
- `tests/e2e.test.ts`
- `tests/exports.test.ts`

## 6. Testing Plan

- Unit tests:
  - sync config validation/defaulting
  - topic desired-state planning
  - schema registry subject planning
  - dry-run/apply diff behavior
- Integration-style tests:
  - mocked Kafka admin topic creation/no-op/drift reporting
  - mocked Schema Registry subject creation/no-op/drift reporting
- CLI tests:
  - `sync` default dry-run
  - `sync --apply`
  - `sync --target kafka|registry|all`
  - invalid sync config failures
- Verification:
  - `pnpm lint`
  - `pnpm test`
  - `pnpm typecheck`
  - `pnpm build`

## 7. Risks / Open Questions

- Topic reconciliation is intentionally limited. Some Kafka topic properties are not safe to auto-update, so the first version should report drift instead of mutating existing topics.
- Schema Registry compatibility updates are not included unless explicitly configured later; initial support should focus on subject/schema existence.
- The CLI currently defaults to generation with no command name. I will preserve that for backward compatibility and add `sync` as an explicit command.
