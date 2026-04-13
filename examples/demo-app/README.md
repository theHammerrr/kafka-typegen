# Demo App

This app lives outside the published library package and shows a real Platformatic-style usage path:

1. generate a typed Kafka client from an Avro schema
2. create a Platformatic runtime with `kafka-typegen/runtime`
3. produce typed events with the generated topic-first producer API
4. consume typed events with the generated topic-first consumer API

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
  - demo generator config for `user.events` and `product.events`
- `schemas/user-created.avsc`
  - Avro schema for `user.created`
- `schemas/user-deleted.avsc`
  - Avro schema for `user.deleted`
- `schemas/product-created.avsc`
  - Avro schema for `product.created`, including a `timestamp-millis` logical type
- `src/producer.ts`
  - example producer using `producer.userEvents.userCreated.send(...)`, `producer.userEvents.userDeleted.send(...)`, and `producer.productEvents.productCreated.send(...)`
- `src/consumer.ts`
  - example consumer using `consumer.userEvents.*` and `consumer.productEvents.*`
- `src/generated/kafka`
  - generated client checked in for inspection and example stability

## Notes

- The demo installs `@platformatic/kafka` because real Platformatic usage requires it.
- The demo uses the `kafka-typegen` CLI and runtime package like a normal consumer project.
- Producer and consumer lifecycle is user-managed, so the examples call `producer.close()` and `consumer.close()` explicitly.
- The generated producer/consumer API is topic-first in the default `minimal` mode, so topic groups like `userEvents` and `productEvents` are the primary entrypoints.
