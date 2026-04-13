# Small Kafka Producer / Consumer Example

This is a minimal example using the generated `createProducer` and `createConsumer` helpers with the Platformatic runtime.

Adjust the generated import path to match your app, for example `./generated/kafka/kafka-client.js`.

## Producer

```ts
import { Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeProducer } from 'kafka-typegen/runtime';
import { createProducer } from './generated/kafka/kafka-client.js';

const runtime = createPlatformaticRuntimeProducer({
  producer: new Producer({
    clientId: 'example-producer',
    bootstrapBrokers: ['localhost:19092']
  }),
  schemaRegistry: {
    url: 'http://localhost:18081'
  }
});

const producer = createProducer(runtime);

await producer.events.userCreated.send({
  id: '123',
  email: 'user@example.com',
  isAdmin: false
});

await producer.close();
```

## Consumer

```ts
import { Consumer } from '@platformatic/kafka';
import { createPlatformaticRuntimeConsumer } from 'kafka-typegen/runtime';
import { createConsumer } from './generated/kafka/kafka-client.js';

const runtime = createPlatformaticRuntimeConsumer({
  consumer: new Consumer({
    clientId: 'example-consumer',
    groupId: 'example-group',
    bootstrapBrokers: ['localhost:19092']
  }),
  schemaRegistry: {
    url: 'http://localhost:18081'
  }
});

const consumer = createConsumer(runtime);

consumer.events.userCreated.on(async (message) => {
  console.log('received:', message.payload);
});

process.once('SIGINT', async () => {
  await consumer.close(true);
  process.exit(0);
});
```

## Run Order

1. Generate your typed client with `kafka-typegen`.
2. Start the consumer.
3. Run the producer.
4. The consumer will receive the typed `userCreated` event.
