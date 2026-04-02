# kafka-typegen

`kafka-typegen` generates a type-safe TypeScript client from Kafka topic definitions and Avro schemas.

The goal is to make Kafka event contracts feel like a real application API instead of a loose collection of topic names and JSON blobs. You declare topics, events, and schema files once, and the generator produces:

- payload interfaces derived from Avro
- typed event and topic unions
- event-to-payload and topic-to-event metadata maps
- typed producer helpers
- typed consumer helpers
- a composed client API on top of a small runtime abstraction

It supports both single-event and multi-event topics.

## What It Generates

Given a config like:

```ts
import { defineConfig } from 'kafka-typegen';

export default defineConfig({
  outputDir: './generated',
  sources: {
    rootDir: './schemas'
  },
  topics: [
    {
      name: 'user.events',
      events: [
        {
          name: 'user.created',
          schemaPath: './user-created.avsc'
        }
      ]
    }
  ]
});
```

`kafka-typegen` generates a TypeScript module with a shape similar to:

```ts
import type {
  RuntimeClient,
  RuntimeConsumer,
  RuntimeEventMetadata,
  RuntimeProducer
} from 'kafka-typegen/runtime';

export interface UserCreatedPayload {
  id: string;
  email: string;
  isAdmin: boolean;
}

export type EventName = 'user.created';
export type TopicName = 'user.events';

export const EventNames = {
  UserCreated: 'user.created'
} as const;

export const TopicNames = {
  UserEvents: 'user.events'
} as const;

export function createProducer(runtimeProducer: RuntimeProducer) { /* ... */ }
export function createConsumer(runtimeConsumer: RuntimeConsumer) { /* ... */ }
export function createClient(runtime: RuntimeClient) { /* ... */ }
```

That generated module gives you:

- `producer.send('user.created', payload)`
- `producer.events.userCreated.send(payload)`
- `consumer.on('user.created', handler)`
- `consumer.events.userCreated.on(handler)`
- `consumer.onTopic('user.events', handler)`
- optional native transport options as the last argument, for example `producer.events.userCreated.send(payload, { acks: -1 })` and `consumer.onTopic('user.events', handler, { autocommit: false })`
- `EventNames.UserCreated`
- `TopicNames.UserEvents`
- `createClient(runtime)` to bind producer and consumer together

## Generated Import Ergonomics

The generated client is still application-specific code, so it is not exported from the published `kafka-typegen` package itself. Instead, `kafka-typegen` can generate a small local package wrapper so your app imports the generated symbols from a stable package-like path.

Configure a package name:

```ts
import { defineConfig } from 'kafka-typegen';

export default defineConfig({
  outputDir: './src/generated/kafka',
  generation: {
    packageName: '@app/kafka'
  },
  sources: {
    rootDir: './schemas'
  },
  topics: [
    {
      name: 'user.events',
      events: [
        {
          name: 'user.created',
          schemaPath: './user-created.avsc'
        }
      ]
    }
  ]
});
```

With `generation.packageName` set, the generator emits:

- your generated client file
- an `index.ts` re-export file
- a generated `package.json`

That lets your application code import the generated API from one stable local package path:

```ts
import {
  EventNames,
  TopicNames,
  createClient,
  createProducer,
  type EventName,
  type TopicName
} from '@app/kafka';
```

This is the closest equivalent to the Prisma-style experience in this package today: the library generates a local package for your app, rather than trying to ship app-specific types from `kafka-typegen` itself.

## Install

```bash
pnpm add kafka-typegen
```

If you want to use the first-party Platformatic adapter:

```bash
pnpm add kafka-typegen @platformatic/kafka
```

## Release Pipeline

The repository includes `.github/workflows/publish.yml`, which runs on every push to `main` and:

- installs dependencies
- runs `pnpm lint`, `pnpm typecheck`, `pnpm test`, and `pnpm build`
- rewrites the package version to a unique canary version like `0.1.0-main.<run>.<sha>`
- publishes that build to npm with the `main` dist-tag

This is intentionally a canary flow rather than a stable `latest` release flow. npm will not accept re-publishing the same version for every commit, so each `main` push must publish a unique prerelease version.

To enable publishing, set the `NPM_TOKEN` GitHub Actions secret for the repository.

## Quick Start

### 1. Create a config file

Create `kafka-typegen.config.mjs` in your project root:

```js
import { defineConfig } from 'kafka-typegen';

export default defineConfig({
  outputDir: './generated',
  sources: {
    rootDir: './schemas'
  },
  topics: [
    {
      name: 'user.events',
      events: [
        {
          name: 'user.created',
          schemaPath: './user-created.avsc'
        },
        {
          name: 'user.updated',
          schemaPath: './user-updated.avsc'
        }
      ]
    }
  ]
});
```

### 2. Add your Avro schemas

Example `schemas/user-created.avsc`:

```json
{
  "type": "record",
  "name": "UserCreated",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "email", "type": "string" },
    { "name": "isAdmin", "type": "boolean" }
  ]
}
```

### 3. Generate the client

```bash
pnpm kafka-typegen
```

Or point at a specific config file:

```bash
pnpm kafka-typegen --config ./kafka-typegen.config.mjs
```

The generated file is written to `outputDir`. By default the filename is `kafka-client.ts`.

### 4. Plan or apply infrastructure sync

`kafka-typegen` can now plan or apply Kafka topic creation and Schema Registry subject creation from the same config:

```bash
pnpm kafka-typegen sync
pnpm kafka-typegen sync --apply
pnpm kafka-typegen sync --target kafka
```

The default `sync` mode is a dry-run. It reports what will be created and any detected drift for existing resources.

## Config Reference

The config is intentionally explicit. Important fields:

```ts
interface KafkaTypegenConfig {
  outputDir: string;
  sources?: {
    rootDir?: string;
  };
  schemaRegistry?: {
    url: string;
    subjectStrategy?: 'event-name' | 'topic-name' | 'topic-event';
  };
  runtime?: {
    transport?: 'kafkajs' | '@platformatic/kafka';
    module?: string;
  };
  sync?: {
    kafka?: {
      brokers: string[];
      clientId?: string;
      ssl?: boolean;
      sasl?: {
        mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
        username: string;
        password: string;
      };
      failOnDrift?: boolean;
    };
    schemaRegistry?: {
      url?: string;
      username?: string;
      password?: string;
      failOnDrift?: boolean;
    };
  };
  generation?: {
    clientName?: string;
    packageName?: string;
    typesFileName?: string;
  };
  naming?: {
    eventTypeSuffix?: string;
    topicTypeSuffix?: string;
  };
  topics: Array<{
    name: string;
    keySchemaPath?: string;
    subjectStrategy?: 'event-name' | 'topic-name' | 'topic-event';
    sync?: {
      partitions?: number;
      replicationFactor?: number;
      configEntries?: Record<string, string>;
    };
    events: Array<{
      name: string;
      schemaPath: string;
      keySchemaPath?: string;
      subject?: string;
    }>;
  }>;
}
```

### Notes

- `sources.rootDir` is the base directory used to resolve schema paths.
- Topic and event ordering is normalized deterministically before generation.
- Event names must be unique across all topics.
- `runtime.module` controls which runtime package the generated file imports its runtime types from.
- If `runtime.module` is omitted:
  - `kafkajs` defaults to `kafka-typegen/runtime`
  - `@platformatic/kafka` defaults to `kafka-typegen/runtime/platformatic`
- `generation.packageName`, when set, emits a local package wrapper so the generated client can be imported from a stable package path.
- `sync.kafka` config is used only by the `sync` CLI command.
- `sync.schemaRegistry.url` defaults to `schemaRegistry.url` when omitted.
- Topic sync defaults are `partitions: 1`, `replicationFactor: 1`, and empty `configEntries`.

### Sync Config Example

```js
import { defineConfig } from 'kafka-typegen';

export default defineConfig({
  outputDir: './generated',
  schemaRegistry: {
    url: process.env.SCHEMA_REGISTRY_URL
  },
  sync: {
    kafka: {
      brokers: [process.env.KAFKA_BROKER ?? 'localhost:9092'],
      clientId: 'kafka-typegen-sync'
    },
    schemaRegistry: {
      username: process.env.SCHEMA_REGISTRY_USERNAME,
      password: process.env.SCHEMA_REGISTRY_PASSWORD
    }
  },
  sources: {
    rootDir: './schemas'
  },
  topics: [
    {
      name: 'user.events',
      sync: {
        partitions: 3,
        replicationFactor: 2,
        configEntries: {
          'cleanup.policy': 'delete'
        }
      },
      events: [
        {
          name: 'user.created',
          schemaPath: './user-created.avsc'
        }
      ]
    }
  ]
});
```

## Runtime Usage

The generated client is transport-agnostic. It talks to a runtime interface, and this package ships two runtime entrypoints.

### Generic runtime

Import from `kafka-typegen/runtime` when you want to provide your own transport adapters:

```ts
import { createRuntimeClient } from 'kafka-typegen/runtime';
```

You provide:

- `producerTransport.send(message)`
- `consumerTransport.onTopic(topicName, handler)`
- either `serialization.serialize(metadata, payload)` / `serialization.deserialize(metadata, message)`
- or `schemaRegistry`, which can be either:
  - direct Confluent Schema Registry connection options
  - an already-created registry client that satisfies the runtime registry interface

### Platformatic runtime

Platformatic helpers are available from both `kafka-typegen/runtime` and `kafka-typegen/runtime/platformatic`. The shorter import is supported:

```ts
import { Producer, Consumer } from '@platformatic/kafka';
import {
  createPlatformaticRuntimeClient
} from 'kafka-typegen/runtime';
```

Example:

```ts
import { Consumer, Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeClient } from 'kafka-typegen/runtime';
import { createClient } from './generated/kafka-client.js';

const producer = new Producer({
  clientId: 'app-producer',
  bootstrapBrokers: ['localhost:9092']
});

const consumer = new Consumer({
  clientId: 'app-consumer',
  groupId: 'app-group',
  bootstrapBrokers: ['localhost:9092']
});

const runtime = createPlatformaticRuntimeClient({
  producer,
  consumer,
  serialization: {
    async serialize(_metadata, payload) {
      return {
        value: new TextEncoder().encode(JSON.stringify(payload))
      };
    },
    async deserialize(_metadata, message) {
      return JSON.parse(new TextDecoder().decode(message.value));
    }
  }
});

const client = createClient(runtime);
```

### Runtime Schema Registry Support

Runtime helpers now support two mutually exclusive serialization modes:

- `serialization`
- `schemaRegistry`

You must provide exactly one of them. Passing both is rejected as ambiguous, and passing neither is rejected because the runtime cannot encode or decode payloads without one.

The `schemaRegistry` path supports two forms:

- direct Confluent-compatible options:

```ts
schemaRegistry: {
  url: 'http://localhost:8081',
  auth: {
    username: '...',
    password: '...'
  }
}
```

- an explicit runtime registry client:

```ts
schemaRegistry: createConfluentSchemaRegistryRuntimeClient({
  url: 'http://localhost:8081'
})
```

The direct config form is the simplest default and is the recommended path unless you already need to manage the registry client yourself.

At runtime, the library uses generated event metadata such as `subjectName`, `eventName`, and `topicName` to:

- resolve the latest schema for a subject on the producer side
- encode payloads with Avro
- prepend Confluent wire format with the schema id
- read schema ids from incoming messages on the consumer side
- resolve schemas by id
- decode Avro payloads back into the typed generated message payload

The runtime keeps internal caches for subject and schema-id lookups so repeated sends and receives do not refetch the same schema information on every message.

The expected registry client shape is intentionally small and runtime-focused:

```ts
interface SchemaRegistryRuntimeClient {
  getLatestSchema(subjectName: string): Promise<{
    schemaId: number;
    schema: string | Record<string, unknown>;
    subjectName?: string;
  }>;
  getSchemaById(schemaId: number): Promise<{
    schemaId: number;
    schema: string | Record<string, unknown>;
    subjectName?: string;
  }>;
}
```

Generic runtime example:

```ts
import { createRuntimeClient } from 'kafka-typegen/runtime';

const runtime = createRuntimeClient({
  producerTransport,
  consumerTransport,
  schemaRegistry: {
    url: 'http://localhost:8081'
  }
});
```

Platformatic example:

```ts
import { Consumer, Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeClient } from 'kafka-typegen/runtime';
import { createClient } from '@app/kafka';

const runtime = createPlatformaticRuntimeClient({
  producer: new Producer({
    clientId: 'app-producer',
    bootstrapBrokers: ['localhost:9092']
  }),
  consumer: new Consumer({
    clientId: 'app-consumer',
    groupId: 'app-group',
    bootstrapBrokers: ['localhost:9092']
  }),
  schemaRegistry: {
    url: 'http://localhost:8081'
  }
});

const client = createClient(runtime);
```

The Platformatic adapter:

- wraps `producer.send({ messages: [...] })`
- wraps `consumer.consume({ topics: [...] })`
- creates at most one consume stream per topic
- fans messages out to all registered handlers for that topic
- does not manage producer, consumer, or stream shutdown for you

`runtime.transport` in `kafka-typegen.config.mjs` is still useful even if you import Platformatic helpers manually. It controls which runtime module path the generated client uses for its type imports. If you omit it, generated code defaults to `kafka-typegen/runtime`; if you set `transport: '@platformatic/kafka'`, generated code defaults to `kafka-typegen/runtime/platformatic`.

If you import from `kafka-typegen/runtime` directly, you can omit `runtime.transport` and keep an explicit `runtime.module` only when you want to override the generated import path.

### Producer-only and consumer-only runtime helpers

If your application only needs one side of the API, you do not need to build a full runtime client first.

Generic runtime:

```ts
import { createRuntimeConsumer, createRuntimeProducer } from 'kafka-typegen/runtime';
import { createConsumer, createProducer } from '@app/kafka';

const runtimeProducer = createRuntimeProducer({
  producerTransport: {
    async send(message) {
      // send to your transport
    }
  },
  serialization: {
    async serialize(_metadata, payload) {
      return {
        value: new TextEncoder().encode(JSON.stringify(payload))
      };
    },
    async deserialize() {
      throw new Error('Not used by producer-only runtime.');
    }
  }
});

const producer = createProducer(runtimeProducer);
await producer.events.userCreated.send({
  id: 'user_1',
  email: 'ada@example.com',
  isAdmin: true
}, {
  acks: -1
});
```

The same helpers also support Schema Registry directly:

```ts
const runtimeProducer = createRuntimeProducer({
  producerTransport,
  schemaRegistry: {
    url: 'http://localhost:8081'
  }
});
```

Platformatic runtime:

```ts
import { Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeProducer } from 'kafka-typegen/runtime';
import { createProducer } from '@app/kafka';

const runtimeProducer = createPlatformaticRuntimeProducer({
  producer: new Producer({
    clientId: 'app-producer',
    bootstrapBrokers: ['localhost:9092']
  }),
  serialization: {
    async serialize(_metadata, payload) {
      return {
        value: new TextEncoder().encode(JSON.stringify(payload))
      };
    },
    async deserialize() {
      throw new Error('Not used by producer-only runtime.');
    }
  }
});

const producer = createProducer(runtimeProducer);
await producer.events.userCreated.send({
  id: 'user_1',
  email: 'ada@example.com',
  isAdmin: true
});
```

And the consumer-only path works the same way:

```ts
import { Consumer } from '@platformatic/kafka';
import { createPlatformaticRuntimeConsumer } from 'kafka-typegen/runtime';
import { createConsumer } from '@app/kafka';

const runtimeConsumer = createPlatformaticRuntimeConsumer({
  consumer: new Consumer({
    clientId: 'app-consumer',
    groupId: 'app-group',
    bootstrapBrokers: ['localhost:9092']
  }),
  schemaRegistry: {
    url: 'http://localhost:8081'
  }
});

const consumer = createConsumer(runtimeConsumer);
await consumer.events.userCreated.on(async (message) => {
  message.payload.isAdmin;
}, {
  autocommit: false
});
```

## CLI

The package exposes a CLI binary:

```bash
kafka-typegen
```

Supported behavior:

- default config discovery via `kafka-typegen.config.mjs`
- explicit config loading via `--config <path>`
- generation output written to the configured `outputDir`
- actionable validation and loading errors
- `sync` command with dry-run by default
- `sync --apply` to create missing Kafka topics and Schema Registry subjects
- `sync --target kafka|registry|all`
- `sync --json` for machine-readable sync output

Examples:

```bash
kafka-typegen
kafka-typegen generate --config ./kafka-typegen.config.mjs
kafka-typegen sync --config ./kafka-typegen.config.mjs
kafka-typegen sync --apply --target registry
```

## Demo App

A standalone usage example lives in `examples/demo-app`. It is not part of the published library package because the package `files` list only ships `dist/`.

The demo shows:

- a local `kafka-typegen.config.mjs`
- an Avro schema under `schemas/`
- generated client output under `generated/`
- a small application that uses `createPlatformaticRuntimeClient(...)` from `kafka-typegen/runtime/platformatic`

Run it with:

```bash
cd examples/demo-app
pnpm install
pnpm demo
pnpm start
```

## Package Exports

- `kafka-typegen`
  - config helpers
  - schema loading and catalog builder interfaces
  - generator interfaces
  - generic runtime interfaces
- `kafka-typegen/runtime`
  - generic runtime client and runtime types
- `kafka-typegen/runtime/platformatic`
  - Platformatic runtime adapter
  - generic runtime types re-exported for generated imports

## Development

Useful commands in this repository:

```bash
pnpm test
pnpm typecheck
pnpm build
```

## Current Scope

What this package does today:

- validates and normalizes Kafka typegen config
- loads Avro record schemas from disk
- builds a deterministic internal event catalog
- generates typed producer, consumer, and client APIs
- supports a generic runtime abstraction
- ships a first-party `@platformatic/kafka` runtime adapter
- can plan or create Kafka topics through the `sync` command
- can plan or create Schema Registry subjects through the `sync` command

What it does not do automatically:

- manage runtime client lifecycle for you
- generate multiple output files per config
- mutate existing Kafka topics to reconcile drift
- mutate existing Schema Registry subjects when schemas differ

## Status

This repository is building toward a Prisma-style developer experience for Kafka event contracts. The current implementation already supports end-to-end generation and runtime integration, but the public API should still be treated as early-stage.
