export interface KafkaTypegenLogger {
  debug(message: string, context?: Readonly<Record<string, unknown>>): void;
  error(message: string, context?: Readonly<Record<string, unknown>>): void;
  info(message: string, context?: Readonly<Record<string, unknown>>): void;
  warn(message: string, context?: Readonly<Record<string, unknown>>): void;
}

export interface KafkaTypegenObserver {
  onEvent(event: KafkaTypegenObservedEvent): void | Promise<void>;
}

export interface KafkaTypegenObservabilityOptions {
  readonly logger?: Partial<KafkaTypegenLogger>;
  readonly observer?: KafkaTypegenObserver;
}

export type KafkaTypegenObservedEvent =
  | {
      readonly type: 'runtime.consumer.background-error';
      readonly error: string;
      readonly source: 'platformatic-handler' | 'platformatic-stream';
      readonly topicName: string;
    }
  | {
      readonly type:
        | 'runtime.consumer.handle.failure'
        | 'runtime.consumer.handle.start'
        | 'runtime.consumer.handle.success';
      readonly error?: string;
      readonly eventName: string;
      readonly topicName: string;
    }
  | {
      readonly type:
        | 'runtime.producer.send.failure'
        | 'runtime.producer.send.start'
        | 'runtime.producer.send.success';
      readonly error?: string;
      readonly eventName: string;
      readonly topicName: string;
    }
  | {
      readonly type:
        | 'runtime.schema-registry.deserialize.failure'
        | 'runtime.schema-registry.deserialize.start'
        | 'runtime.schema-registry.deserialize.success'
        | 'runtime.schema-registry.serialize.failure'
        | 'runtime.schema-registry.serialize.start'
        | 'runtime.schema-registry.serialize.success';
      readonly error?: string;
      readonly eventName: string;
      readonly schemaId?: number;
      readonly subjectName: string;
    }
  | {
      readonly type: 'sync.complete' | 'sync.start';
      readonly apply: boolean;
      readonly operationCount?: number;
      readonly target: 'all' | 'kafka' | 'registry';
    }
  | {
      readonly type: 'sync.failure';
      readonly apply: boolean;
      readonly error: string;
      readonly target: 'all' | 'kafka' | 'registry';
    }
  | {
      readonly type: 'sync.operation';
      readonly action: 'create' | 'drift' | 'noop' | 'update';
      readonly details: string;
      readonly resourceName: string;
      readonly target: 'kafka' | 'registry';
    };

export interface ResolvedKafkaTypegenObservability {
  readonly logger: KafkaTypegenLogger;
  readonly observer?: KafkaTypegenObserver;
}
