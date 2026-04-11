import { emitObservedEvent, toErrorString } from '../observability.js';
import type {
  ResolvedRuntimeClientOptions,
  RuntimeEventMetadata,
  RuntimeOutgoingMessage,
  RuntimeProducer
} from './types.js';

const RUNTIME_EVENT_HEADER = 'x-kafka-typegen-event';

export class DefaultRuntimeProducer<TSendOptions = unknown>
  implements RuntimeProducer<TSendOptions> {
  public constructor(
    private readonly options: ResolvedRuntimeClientOptions<TSendOptions>
  ) {}

  public async send(
    metadata: RuntimeEventMetadata,
    payload: unknown,
    options?: TSendOptions
  ): Promise<void> {
    await emitObservedEvent(this.options.observability, {
      eventName: metadata.eventName,
      topicName: metadata.topicName,
      type: 'runtime.producer.send.start'
    });

    try {
      const serialized = await this.options.serialization.serialize(metadata, payload);

      const outgoingMessage: RuntimeOutgoingMessage = {
        topicName: metadata.topicName,
        value: serialized.value,
        headers: { ...(serialized.headers ?? {}), [RUNTIME_EVENT_HEADER]: metadata.eventName },
        ...(serialized.key !== undefined ? { key: serialized.key } : {}),
        ...(serialized.schemaId !== undefined ? { schemaId: serialized.schemaId } : {})
      };

      await this.options.producerTransport.send(outgoingMessage, options);
      await emitObservedEvent(this.options.observability, {
        eventName: metadata.eventName,
        topicName: metadata.topicName,
        type: 'runtime.producer.send.success'
      });
    } catch (error) {
      this.options.observability.logger.error('Runtime producer send failed.', {
        error: toErrorString(error),
        eventName: metadata.eventName,
        topicName: metadata.topicName
      });
      await emitObservedEvent(this.options.observability, {
        error: toErrorString(error),
        eventName: metadata.eventName,
        topicName: metadata.topicName,
        type: 'runtime.producer.send.failure'
      });
      throw error;
    }
  }
}

export { RUNTIME_EVENT_HEADER };
