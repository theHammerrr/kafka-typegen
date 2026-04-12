# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Added a first-party KafkaJS runtime adapter with producer, consumer, and client helpers.
- Added `kafka-typegen/runtime/advanced` for low-level runtime transport adapters and transport-facing
  types.
- Added top-level Avro `enum` and `fixed` root support in schema parsing and type generation.
- Added `generation.avroExternalTypes` for explicit external named-type mappings during generation.
- Added `generation.avroSemanticMode: 'safe'` to render plain Avro `long` values as `bigint`.

### Changed

- Trimmed `kafka-typegen/runtime` and `kafka-typegen/runtime/platformatic` to the stable high-level
  runtime API. Import transport adapter internals from `kafka-typegen/runtime/advanced`.

## 0.2.0 - 2026-04-03

### Added

- Documented the exact Avro schema constructs currently supported by the generator.
- Added generation support for nested named Avro `record`, `enum`, and `fixed` declarations.
- Added generation support for Avro logical types `date`, `time-millis`, `timestamp-millis`,
  `timestamp-micros`, and `decimal`.
- Added a Docker-backed Testcontainers integration suite for real Kafka, Schema Registry, generated-app
  typechecking, runtime happy paths, and consumer error propagation.
- Added Schema Registry schema-evolution support in `sync --apply`, including registering new subject
  versions on drift and optional subject compatibility policy updates.

### Changed

- Unsupported Avro schema constructs now fail generation with explicit path-aware errors instead of
  silently emitting `unknown`.
- Generated imports are documented as direct relative imports from generated source files.
- Schema Registry sync drift detection now compares canonicalized Avro schema definitions instead of raw
  JSON text.
- `sync.schemaRegistry` now uses an explicit `onDrift` policy with default `register`; legacy
  `failOnDrift: true` still maps to `onDrift: 'fail'`.

### Removed

- Removed `generation.clientName` because it was never used by the generator.
- Removed `generation.packageName` and generated package-wrapper emission because import resolution still
  required app-side package setup.

## 0.1.0 - 2026-04-03

### Added

- Added config validation and normalization for topic, event, runtime, Schema Registry, and sync settings.
- Added Avro schema loading/parsing and deterministic catalog construction.
- Added TypeScript client generation for typed producer, consumer, and composed client APIs.
- Added `EventNames`, `TopicNames`, event payload mappings, topic/event metadata, and schema registry
  config generation.
- Added a generic runtime layer and a first-party `@platformatic/kafka` runtime adapter.
- Added first-party Confluent Schema Registry serialization/deserialization support.
- Added a `kafka-typegen` CLI for code generation and a `sync` command for Kafka topic and Schema
  Registry provisioning.
- Added canary publishing from `main`, repository license metadata, and a runnable demo app.
