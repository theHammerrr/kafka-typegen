import type {
  KafkaTypegenObservabilityOptions,
  ResolvedKafkaTypegenObservability
} from '../observability.js';

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

export interface SchemaRegistryRuntimeSchema {
  readonly schema: string | Record<string, unknown>;
  readonly schemaId: number;
  readonly subjectName?: string;
}

export interface SchemaRegistryRuntimeClient {
  getLatestSchema(subjectName: string): Promise<SchemaRegistryRuntimeSchema>;
  getSchemaById(schemaId: number): Promise<SchemaRegistryRuntimeSchema>;
}

export interface ConfluentSchemaRegistryRuntimeAuth {
  readonly password?: string;
  readonly token?: string;
  readonly username?: string;
}

export interface ConfluentSchemaRegistryRuntimeOptions {
  readonly auth?: ConfluentSchemaRegistryRuntimeAuth;
  readonly url: string;
}

export type RuntimeSchemaRegistry =
  | ConfluentSchemaRegistryRuntimeOptions
  | SchemaRegistryRuntimeClient;

export type RuntimeSerializationOptions =
  | {
      readonly schemaRegistry: RuntimeSchemaRegistry;
      readonly serialization?: never;
    }
  | {
      readonly schemaRegistry?: never;
      readonly serialization: RuntimeSerializationHooks;
    };

export interface RuntimeTransportProducer<TSendOptions = unknown> {
  send(message: RuntimeOutgoingMessage, options?: TSendOptions): Promise<void>;
}

export interface RuntimeTransportConsumer<TSubscriptionOptions = unknown> {
  onTopic(
    topicName: string,
    handler: (message: RuntimeIncomingMessage) => Promise<void> | void,
    options?: TSubscriptionOptions
  ): Promise<void>;
}

export interface RuntimeProducer<TSendOptions = unknown> {
  send(
    metadata: RuntimeEventMetadata,
    payload: unknown,
    options?: TSendOptions
  ): Promise<void>;
}

export interface RuntimeConsumerMessage<TPayload = unknown> {
  readonly event: string;
  readonly eventName: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly key?: unknown;
  readonly offset?: string;
  readonly partition?: number;
  readonly payload: TPayload;
  readonly schemaId?: string | number;
  readonly timestamp?: string;
  readonly topic: string;
  readonly topicName: string;
}

export interface RuntimeConsumer<TSubscriptionOptions = unknown> {
  on<TPayload>(
    metadata: RuntimeEventMetadata,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void,
    options?: TSubscriptionOptions
  ): Promise<void>;
  onTopic<TPayload>(
    topicName: string,
    metadataByEvent: Readonly<Record<string, RuntimeEventMetadata>>,
    handler: (message: RuntimeConsumerMessage<TPayload>) => Promise<void> | void,
    options?: TSubscriptionOptions
  ): Promise<void>;
}

export interface RuntimeClient<
  TSendOptions = unknown,
  TSubscriptionOptions = unknown
> {
  readonly consumer: RuntimeConsumer<TSubscriptionOptions>;
  readonly producer: RuntimeProducer<TSendOptions>;
}

export interface ResolvedRuntimeClientOptions<
  TSendOptions = unknown,
  TSubscriptionOptions = unknown
> {
  readonly consumerTransport: RuntimeTransportConsumer<TSubscriptionOptions>;
  readonly observability: ResolvedKafkaTypegenObservability;
  readonly serialization: RuntimeSerializationHooks;
  readonly producerTransport: RuntimeTransportProducer<TSendOptions>;
}

export type RuntimeClientOptions<
  TSendOptions = unknown,
  TSubscriptionOptions = unknown
> = {
  readonly consumerTransport: RuntimeTransportConsumer<TSubscriptionOptions>;
  readonly producerTransport: RuntimeTransportProducer<TSendOptions>;
} & RuntimeSerializationOptions &
  KafkaTypegenObservabilityOptions;

export type RuntimeProducerOptions<TSendOptions = unknown> = {
  readonly producerTransport: RuntimeTransportProducer<TSendOptions>;
} & RuntimeSerializationOptions &
  KafkaTypegenObservabilityOptions;

export type RuntimeConsumerOptions<TSubscriptionOptions = unknown> = {
  readonly consumerTransport: RuntimeTransportConsumer<TSubscriptionOptions>;
} & RuntimeSerializationOptions &
  KafkaTypegenObservabilityOptions;
