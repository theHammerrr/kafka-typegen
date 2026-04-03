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
    const serialized = await this.options.serialization.serialize(metadata, payload);

    const outgoingMessage: RuntimeOutgoingMessage = {
      topicName: metadata.topicName,
      value: serialized.value,
      headers: { ...(serialized.headers ?? {}), [RUNTIME_EVENT_HEADER]: metadata.eventName },
      ...(serialized.key !== undefined ? { key: serialized.key } : {}),
      ...(serialized.schemaId !== undefined ? { schemaId: serialized.schemaId } : {})
    };

    await this.options.producerTransport.send(outgoingMessage, options);
  }
}

export { RUNTIME_EVENT_HEADER };
