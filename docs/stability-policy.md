# Stability Policy

This document defines the intended compatibility guarantees for `kafka-typegen` `1.0.0`.

## Stable Surface for 1.0.0

The following areas are intended to be stable across `1.x` releases unless a future major version explicitly changes them:

- config shape validated by `defineConfig(...)`, including current option names and default behaviors
- generated default `minimal` API shape built around `createProducer(...)`, `createConsumer(...)`, and `createClient(...)`
- generated topic-first access patterns such as `producer.userEvents.userCreated.send(...)` and `consumer.userEvents.userCreated.on(...)`
- generated support for current Avro root kinds and type-mapping behavior documented in the README
- runtime subpath entrypoints:
  - `kafka-typegen/runtime`
  - `kafka-typegen/runtime/kafkajs`
  - `kafka-typegen/runtime/platformatic`
  - `kafka-typegen/runtime/advanced`
- CLI command structure:
  - `kafka-typegen`
  - `kafka-typegen generate`
  - `kafka-typegen sync`
- documented sync semantics for dry-run vs `--apply`, drift reporting, and Schema Registry compatibility handling

## Allowed in Minor Releases

The following changes are considered acceptable in `1.x` minor releases:

- additive config options
- additive generated helpers or types that do not break existing generated code
- additive runtime exports on existing documented entrypoints
- additive observability hooks or non-breaking metadata fields
- new Avro features or adapters that do not change existing documented behavior
- internal performance improvements and refactors
- better diagnostics and more specific error messages when the documented failure contract stays the same

## Requires a Major Release

The following changes should require a major version bump:

- renaming or removing documented config fields
- changing default config behavior in a way that changes generated output or sync behavior materially
- changing the default generated public API shape
- removing or renaming documented package entrypoints
- changing current Avro type mappings in a way that breaks consumer code
- changing CLI command semantics or required arguments incompatibly
- changing sync behavior from plan-only to mutate-by-default, or changing apply semantics incompatibly

## Explicitly Non-Stable or Limited Areas

The following areas are intentionally more limited or should not be treated as open-ended compatibility commitments:

- undocumented internal types, metadata constants, and implementation details of generated files
- exact generated formatting, ordering, or helper structure beyond the documented API contract
- contents of `kafka-typegen/runtime/advanced` beyond its documented purpose as a lower-level escape hatch
- performance characteristics before dedicated regression benchmarks are added
- end-to-end TLS certificate material handling in sync/runtime config
  - current secure integration coverage focuses on SASL/SCRAM broker auth
  - the public sync config currently exposes only `ssl: boolean`, not full CA/key/cert wiring

## Support Expectations

For `1.0.0`, bug fixes and documentation updates should prefer preserving the stable surface above. When there is tension between implementation cleanup and compatibility, compatibility wins unless there is a strong correctness or security reason to break it in a future major release.
