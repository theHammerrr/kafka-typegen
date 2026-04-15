# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

- No unreleased changes yet.

## 1.0.0 - 2026-04-15

### Added

- Added config validation and normalization for topic, event, runtime, Schema Registry, and sync settings.
- Added Avro schema loading/parsing and deterministic catalog construction.
- Added a `kafka-typegen` CLI for code generation and a `sync` command for Kafka topic and Schema
  Registry provisioning.
- Added a generic runtime layer, plus first-party KafkaJS and `@platformatic/kafka` runtime adapters.
- Added first-party Confluent Schema Registry serialization/deserialization support.
- Added a first-party KafkaJS runtime adapter with producer, consumer, and client helpers.
- Added `kafka-typegen/runtime/advanced` for low-level runtime transport adapters and transport-facing
  types.
- Added generation support for nested named Avro `record`, `enum`, and `fixed` declarations.
- Added generation support for Avro logical types `date`, `time-millis`, `timestamp-millis`,
  `timestamp-micros`, and `decimal`.
- Added a dedicated secure Testcontainers suite for Kafka SASL/SCRAM coverage, including authenticated
  `sync --target kafka` and KafkaJS runtime end-to-end tests.
- Added a Docker-backed Testcontainers integration suite for real Kafka, Schema Registry, generated-app
  typechecking, runtime happy paths, and consumer error propagation.
- Added top-level Avro `enum` and `fixed` root support in schema parsing and type generation.
- Added `generation.avroExternalTypes` for explicit external named-type mappings during generation.
- Added `generation.apiMode` with `minimal` as the default and `advanced` as an explicit escape hatch.
- Added `generation.avroSemanticMode: 'safe'` to render plain Avro `long` values as `bigint`.
- Added Schema Registry schema-evolution support in `sync --apply`, including registering new subject
  versions on drift and optional subject compatibility policy updates.
- Added a tagged npm publish flow for stable releases.

### Changed

- Generated imports are documented as direct relative imports from generated source files.
- Schema Registry sync drift detection now compares canonicalized Avro schema definitions instead of raw
  JSON text.
- `sync.schemaRegistry` now uses an explicit `onDrift` policy with default `register`; legacy
  `failOnDrift: true` still maps to `onDrift: 'fail'`.
- Reworked the default generated API around topic-first producer and consumer helpers such as
  `producer.userEvents.userCreated.send(...)` and `consumer.userEvents.userCreated.on(...)`.
- Removed metadata-heavy constants and generic event/topic maps from the default generated public
  surface; they now remain available only through `generation.apiMode: 'advanced'`.
- Trimmed `kafka-typegen/runtime` and `kafka-typegen/runtime/platformatic` to the stable high-level
  runtime API. Import transport adapter internals from `kafka-typegen/runtime/advanced`.

### Removed

- Removed `generation.clientName` because it was never used by the generator.
- Removed `generation.packageName` and generated package-wrapper emission because import resolution still
  required app-side package setup.

### Notes

- The previously planned `0.2.0` changes were not published to npm and are included in `1.0.0`.

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
