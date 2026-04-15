# Upgrade Guide to 1.0.0

This guide is for adopters moving from the `0.x` line to the stable `1.0.0` release.

## Main API Change

The default generated API is now the `minimal` topic-first surface.

That means generated code is centered around:

- `createProducer(...)`
- `createConsumer(...)`
- `createClient(...)`
- topic/event access such as:
  - `producer.userEvents.userCreated.send(...)`
  - `consumer.userEvents.userCreated.on(...)`
  - `consumer.userEvents.on(...)` for multi-event topics

## What Changed from the Older Generated Surface

The previous metadata-heavy generated surface is no longer the default.

Notable differences:

- default generation no longer exposes the old generic `events` wrapper surface as the primary API
- metadata-heavy constants and maps are no longer part of the default generated public API
- the older surface remains available only with:

```js
generation: {
  apiMode: 'advanced'
}
```

## Recommended Migration Path

1. Regenerate your client with the current generator.
2. Update producer calls from generic event-based helpers to topic-first helpers.
3. Update consumer subscriptions to topic-first helpers.
4. If you still depend on the older generated metadata-heavy surface, switch temporarily to:

```js
generation: {
  apiMode: 'advanced'
}
```

5. Migrate off `advanced` mode before treating it as your long-term default.

## Avro Notes

The current documented Avro behavior is part of the `1.0.0` contract:

- top-level `record`, `enum`, and `fixed` roots are supported
- external named references can use `generation.avroExternalTypes`
- `generation.avroSemanticMode: 'safe'` renders plain `long` as `bigint`
- logical types remain type-level mappings, not automatic runtime object conversion

## Runtime Notes

The stable runtime entrypoints for `1.0.0` are:

- `kafka-typegen/runtime`
- `kafka-typegen/runtime/kafkajs`
- `kafka-typegen/runtime/platformatic`
- `kafka-typegen/runtime/advanced`

Use `runtime/advanced` only when you intentionally need lower-level transport adapter internals.
