# Demo App

This app lives outside the published library package and shows the shortest end-to-end usage path:

1. generate a typed Kafka client from an Avro schema
2. create a runtime with `kafka-typegen/runtime/platformatic`
3. provide mock `@platformatic/kafka`-shaped producer and consumer objects
3. send and consume a typed event through the generated client

## Run It

From the repository root:

```bash
pnpm install
cd examples/demo-app
pnpm install
pnpm demo
pnpm start
```

`pnpm demo` rebuilds the library, regenerates the demo client into `examples/demo-app/generated`, and typechecks the example app.

## Files

- `kafka-typegen.config.mjs`
  - demo generator config
- `schemas/user-created.avsc`
  - Avro schema used for code generation
- `src/main.ts`
  - example application code that wires the generated client to the Platformatic runtime adapter

## Notes

- The demo installs `@platformatic/kafka` because real Platformatic usage requires it.
- The demo does not connect to a live broker. It uses mock objects that implement the `send` and `consume` methods expected by the Platformatic adapter.
- To move from the demo to a real application, replace the mocked producer and consumer with real `Producer` and `Consumer` instances from `@platformatic/kafka`.

The generated files are intentionally not committed. They are written to `generated/`, which is ignored by git.
