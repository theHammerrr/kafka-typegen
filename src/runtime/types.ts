export interface RuntimeEventMetadata {
  readonly eventName: string;
  readonly payloadTypeName: string;
  readonly schemaFilePath: string;
  readonly schemaName: string;
  readonly subjectName: string;
  readonly topicName: string;
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
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void
  ): Promise<void>;
}
