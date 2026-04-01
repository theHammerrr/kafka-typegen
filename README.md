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
- `createClient(runtime)` to bind producer and consumer together

## Install

```bash
pnpm add kafka-typegen
```

If you want to use the first-party Platformatic adapter:

```bash
pnpm add kafka-typegen @platformatic/kafka
```

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
- `serialization.serialize(metadata, payload)`
- `serialization.deserialize(metadata, message)`

### Platformatic runtime

Import from `kafka-typegen/runtime/platformatic` when you already have `@platformatic/kafka` producer and consumer instances:

```ts
import { Producer, Consumer } from '@platformatic/kafka';
import {
  createPlatformaticRuntimeClient
} from 'kafka-typegen/runtime/platformatic';
```

Example:

```ts
import { Consumer, Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeClient } from 'kafka-typegen/runtime/platformatic';
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

The Platformatic adapter:

- wraps `producer.send({ messages: [...] })`
- wraps `consumer.consume({ topics: [...] })`
- creates at most one consume stream per topic
- fans messages out to all registered handlers for that topic
- does not manage producer, consumer, or stream shutdown for you

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
- own schema-registry serialization logic out of the box
- generate multiple output files per config
- mutate existing Kafka topics to reconcile drift
- mutate existing Schema Registry subjects when schemas differ

## Status

This repository is building toward a Prisma-style developer experience for Kafka event contracts. The current implementation already supports end-to-end generation and runtime integration, but the public API should still be treated as early-stage.
