import { handleObservedConsumerMessage } from './consumer-observability.js';
import { RUNTIME_EVENT_HEADER } from './producer-runtime.js';
import type {
  ResolvedRuntimeClientOptions,
  RuntimeConsumer,
  RuntimeConsumerMessage,
  RuntimeEventMetadata
} from './types.js';

export class DefaultRuntimeConsumer<TSubscriptionOptions = unknown>
  implements RuntimeConsumer<TSubscriptionOptions> {
  public constructor(
    private readonly options: ResolvedRuntimeClientOptions<
      unknown,
      TSubscriptionOptions
    >
  ) {}

  public async on<TPayload>(
    metadata: RuntimeEventMetadata,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void,
    options?: TSubscriptionOptions
  ): Promise<void> {
    await this.options.consumerTransport.onTopic(metadata.topicName, async (message) => {
      const receivedEventName = message.headers?.[RUNTIME_EVENT_HEADER];
      if (receivedEventName !== metadata.eventName) {
        return;
      }

      await handleObservedConsumerMessage(this.options, metadata, message, handler);
    }, options);
  }

  public async onTopic<TPayload>(
    topicName: string,
    metadataByEvent: Readonly<Record<string, RuntimeEventMetadata>>,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void,
    options?: TSubscriptionOptions
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

      await handleObservedConsumerMessage(this.options, metadata, message, handler);
    }, options);
  }
}
