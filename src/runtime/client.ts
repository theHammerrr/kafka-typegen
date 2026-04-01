import type {
  RuntimeClient,
  RuntimeClientOptions,
  RuntimeConsumer,
  RuntimeConsumerMessage,
  RuntimeEventMetadata,
  RuntimeIncomingMessage,
  RuntimeOutgoingMessage,
  RuntimeProducer
} from './types.js';

const RUNTIME_EVENT_HEADER = 'x-kafka-typegen-event';

function toConsumerMessage<TPayload>(
  metadata: RuntimeEventMetadata,
  message: RuntimeIncomingMessage,
  payload: TPayload
): RuntimeConsumerMessage<TPayload> {
  return {
    eventName: metadata.eventName,
    payload,
    topicName: metadata.topicName,
    ...(message.headers !== undefined ? { headers: message.headers } : {}),
    ...(message.key !== undefined ? { key: message.key } : {}),
    ...(message.offset !== undefined ? { offset: message.offset } : {}),
    ...(message.partition !== undefined ? { partition: message.partition } : {}),
    ...(message.schemaId !== undefined ? { schemaId: message.schemaId } : {}),
    ...(message.timestamp !== undefined ? { timestamp: message.timestamp } : {})
  };
}

class DefaultRuntimeProducer implements RuntimeProducer {
  public constructor(private readonly options: RuntimeClientOptions) {}

  public async send(metadata: RuntimeEventMetadata, payload: unknown): Promise<void> {
    const serialized = await this.options.serialization.serialize(metadata, payload);

    const outgoingMessage: RuntimeOutgoingMessage = {
      topicName: metadata.topicName,
      value: serialized.value,
      headers: {
        ...(serialized.headers ?? {}),
        [RUNTIME_EVENT_HEADER]: metadata.eventName
      },
      ...(serialized.key !== undefined ? { key: serialized.key } : {}),
      ...(serialized.schemaId !== undefined ? { schemaId: serialized.schemaId } : {})
    };

    await this.options.producerTransport.send(outgoingMessage);
  }
}

class DefaultRuntimeConsumer implements RuntimeConsumer {
  public constructor(private readonly options: RuntimeClientOptions) {}

  public async on<TPayload>(
    metadata: RuntimeEventMetadata,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void
  ): Promise<void> {
    await this.options.consumerTransport.onTopic(metadata.topicName, async (message) => {
      const receivedEventName = message.headers?.[RUNTIME_EVENT_HEADER];

      if (receivedEventName !== metadata.eventName) {
        return;
      }

      const payload = await this.options.serialization.deserialize<TPayload>(metadata, message);

      await handler(toConsumerMessage(metadata, message, payload));
    });
  }

  public async onTopic<TPayload>(
    topicName: string,
    metadataByEvent: Readonly<Record<string, RuntimeEventMetadata>>,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void
  ): Promise<void> {
    await this.options.consumerTransport.onTopic(topicName, async (message) => {
      const receivedEventName = message.headers?.[RUNTIME_EVENT_HEADER];

      if (receivedEventName === undefined) {
        return;
      }

      const metadata = metadataByEvent[receivedEventName];

      if (metadata === undefined) {
        return;
      }

      const payload = await this.options.serialization.deserialize<TPayload>(metadata, message);

      await handler(toConsumerMessage(metadata, message, payload));
    });
  }
}

export function createRuntimeClient(options: RuntimeClientOptions): RuntimeClient {
  return {
    consumer: new DefaultRuntimeConsumer(options),
    producer: new DefaultRuntimeProducer(options)
  };
}
