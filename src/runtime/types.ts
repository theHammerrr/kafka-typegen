export interface RuntimeEventMetadata {
  readonly eventName: string;
  readonly payloadTypeName: string;
  readonly schemaFilePath: string;
  readonly schemaName: string;
  readonly subjectName: string;
  readonly topicName: string;
}

export interface RuntimeOutgoingMessage {
  readonly headers?: Readonly<Record<string, string>>;
  readonly key?: unknown;
  readonly schemaId?: string | number;
  readonly topicName: string;
  readonly value: Uint8Array;
}

export interface RuntimeIncomingMessage {
  readonly headers?: Readonly<Record<string, string>>;
  readonly key?: unknown;
  readonly offset?: string;
  readonly partition?: number;
  readonly schemaId?: string | number;
  readonly timestamp?: string;
  readonly topicName: string;
  readonly value: Uint8Array;
}

export interface RuntimeSerializationResult {
  readonly headers?: Readonly<Record<string, string>>;
  readonly key?: unknown;
  readonly schemaId?: string | number;
  readonly value: Uint8Array;
}

export interface RuntimeSerializationHooks {
  deserialize<TPayload>(
    metadata: RuntimeEventMetadata,
    message: RuntimeIncomingMessage
  ): Promise<TPayload>;
  serialize(
    metadata: RuntimeEventMetadata,
    payload: unknown
  ): Promise<RuntimeSerializationResult>;
}

export interface RuntimeTransportProducer {
  send(message: RuntimeOutgoingMessage): Promise<void>;
}

export interface RuntimeTransportConsumer {
  onTopic(
    topicName: string,
    handler: (message: RuntimeIncomingMessage) => Promise<void> | void
  ): Promise<void>;
}

export interface RuntimeProducer {
  send(metadata: RuntimeEventMetadata, payload: unknown): Promise<void>;
}

export interface RuntimeConsumerMessage<TPayload = unknown> {
  readonly eventName: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly key?: unknown;
  readonly offset?: string;
  readonly partition?: number;
  readonly payload: TPayload;
  readonly schemaId?: string | number;
  readonly timestamp?: string;
  readonly topicName: string;
}

export interface RuntimeConsumer {
  on<TPayload>(
    metadata: RuntimeEventMetadata,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void
  ): Promise<void>;
  onTopic<TPayload>(
    topicName: string,
    metadataByEvent: Readonly<Record<string, RuntimeEventMetadata>>,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void
  ): Promise<void>;
}

export interface RuntimeClient {
  readonly consumer: RuntimeConsumer;
  readonly producer: RuntimeProducer;
}

export interface RuntimeClientOptions {
  readonly consumerTransport: RuntimeTransportConsumer;
  readonly serialization: RuntimeSerializationHooks;
  readonly producerTransport: RuntimeTransportProducer;
}
