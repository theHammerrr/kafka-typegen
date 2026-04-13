# 1.0.0 Issue Drafts

## Define 1.0.0 compatibility guarantees

Clarify which surfaces are stable for `1.0.0` and what kinds of changes require a major version bump.

Scope:
- generated API shape
- config schema and defaults
- runtime subpath exports
- sync behavior and failure contracts
- explicit experimental/non-stable areas, if any

Expected outcome:
- a documented compatibility policy in the README or dedicated versioning doc
- tests aligned with the stable surface we intend to support

## Freeze generator and config contracts with fixture coverage

Before `1.0.0`, treat generated output and config validation as public contracts with strong regression coverage.

Scope:
- `generation.apiMode` minimal vs advanced
- Avro root types and external mappings
- semantic mode behavior
- generated client/producer/consumer shapes
- error messages for invalid or unsupported schema shapes

Expected outcome:
- fixture and snapshot coverage that makes contract regressions obvious
- clear approval bar for future generated-output changes

## Expand end-to-end coverage for primary user flows

Add more real user-flow coverage around generation, sync, and runtime usage.

Scope:
- producer-only generated app
- consumer-only generated app
- combined producer/consumer app
- schema registry happy path
- multi-topic and multi-event flows

Expected outcome:
- confidence that the documented happy paths remain stable
- better protection against integration regressions across adapters

## Strengthen failure-mode guarantees

Stability is not just happy-path behavior. We should define and test how the tool fails in common operational problems.

Scope:
- broker auth failures
- schema registry auth and connection failures
- invalid external type mappings
- incompatible schema references
- sync partial failures
- handler and serialization failures

Expected outcome:
- explicit tests for important failure cases
- clearer, path-aware, user-facing error messages

## Tighten release and upgrade discipline for 1.0.0

Add the release-process pieces that make a `1.0.0` credible for users adopting it in production.

Scope:
- release checklist
- versioning policy
- upgrade guidance from `0.x`
- changelog expectations for breaking vs non-breaking changes

Expected outcome:
- documented release procedure
- lightweight upgrade guide for early adopters moving to `1.0.0`

## Improve happy-path documentation for 1.0.0

Make the default onboarding path extremely clear and keep advanced topics separate.

Scope:
- define config
- run generate
- use `createProducer`, `createConsumer`, `createClient`
- explain when to run sync
- move advanced runtime/schema-registry/Avro topics behind dedicated sections

Expected outcome:
- README optimized for first success
- less ambiguity about which path is recommended vs advanced

## Audit and trim accidental public surface

Do a final pass over exported modules and types before `1.0.0` to ensure the public surface is intentional.

Scope:
- root exports
- runtime subpath exports
- advanced-only escape hatches
- internal helper types that may be leaking publicly

Expected outcome:
- deliberate export surface for `1.0.0`
- fewer accidental maintenance commitments

## Add performance tests for large schema catalogs

We should measure generation and parsing performance on larger schema sets before `1.0.0`.

Scope:
- large schema catalogs with many topics/events
- large nested Avro schemas
- cross-file reference-heavy catalogs
- memory and runtime cost for generation

Expected outcome:
- repeatable performance benchmarks
- guardrails for major regressions in parse/generate time

## Prepare the 1.0.0 release

Track the final checklist for promoting the package from `0.x` to `1.0.0`.

Scope:
- close the release-blocking issues
- finalize docs and compatibility statements
- confirm release workflow and npm metadata
- update version and changelog for `1.0.0`

Expected outcome:
- one release-tracking issue for the final cutover

## Tag published npm versions in GitHub Actions

Ensure the npm publish workflow tags each published version in Git so users can trace published packages back to an exact repository state.

Scope:
- create and push `v<version>` tags after successful publish
- skip duplicate tags on reruns
- document the relationship between npm versions and Git tags

Expected outcome:
- every published npm version has a matching Git tag
