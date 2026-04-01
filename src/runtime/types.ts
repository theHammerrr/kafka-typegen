export interface RuntimeEventMetadata {
  readonly eventName: string;
  readonly schemaPath: string;
  readonly topicName: string;
}

export interface RuntimeProducer {
  send(metadata: RuntimeEventMetadata, payload: unknown): Promise<void>;
}

export interface RuntimeConsumerMessage<TPayload = unknown> {
  readonly eventName: string;
  readonly payload: TPayload;
  readonly topicName: string;
}

export interface RuntimeConsumer {
  on<TPayload>(
    metadata: RuntimeEventMetadata,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void
  ): Promise<void>;
}
