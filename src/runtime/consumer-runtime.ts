import { toConsumerMessage } from './consumer-message.js';
import { RUNTIME_EVENT_HEADER } from './producer-runtime.js';
import type {
  ResolvedRuntimeClientOptions,
  RuntimeConsumer,
  RuntimeConsumerMessage,
  RuntimeEventMetadata
} from './types.js';

export class DefaultRuntimeConsumer implements RuntimeConsumer {
  public constructor(private readonly options: ResolvedRuntimeClientOptions) {}

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
