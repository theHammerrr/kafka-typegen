# Demo App

This app lives outside the published library package and shows a real Platformatic-style usage path:

1. generate a typed Kafka client from an Avro schema
2. create a Platformatic runtime with `kafka-typegen/runtime`
3. produce typed events with the generated producer
4. consume typed events with the generated consumer

## Run It

From the repository root:

```bash
pnpm install
cd examples/demo-app
pnpm install
pnpm demo
pnpm consume
# in another terminal
pnpm produce
```

`pnpm demo` rebuilds the library from the current repo checkout, reinstalls the local `file:../..` package snapshot, regenerates the demo client into `examples/demo-app/src/generated/kafka`, and typechecks the example app.

`pnpm consume` and `pnpm produce` expect Kafka and Schema Registry to be available at the endpoints configured in `kafka-typegen.config.mjs` and `src/producer.ts` / `src/consumer.ts`.

## Files

- `kafka-typegen.config.mjs`
  - demo generator config
- `schemas/user-created.avsc`
  - Avro schema used for code generation
- `schemas/user-deleted.avsc`
  - second Avro schema used to demonstrate multiple typed events on one topic
- `src/producer.ts`
  - example producer using `producer.events.userCreated.send(...)` and `producer.events.userDeleted.send(...)`
- `src/consumer.ts`
  - example consumer using `consumer.events.userCreated.on(...)` and `consumer.events.userDeleted.on(...)`

## Notes

- The demo installs `@platformatic/kafka` because real Platformatic usage requires it.
- The demo uses the `kafka-typegen` CLI and runtime package like a normal consumer project.
- Producer and consumer lifecycle is user-managed, so the examples call `producer.close()` and `consumer.close()` explicitly.

The generated files are intentionally not committed. They are written to `src/generated/kafka`, which is ignored by git.
