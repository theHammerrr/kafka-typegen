import type {
  KafkaTypegenLogger,
  KafkaTypegenObservedEvent,
  KafkaTypegenObservabilityOptions,
  ResolvedKafkaTypegenObservability
} from './observability-types.js';

const noopLoggerMethod = () => undefined;

function bindLoggerMethod(
  logger: Partial<KafkaTypegenLogger> | undefined,
  level: keyof KafkaTypegenLogger
): KafkaTypegenLogger[typeof level] {
  const loggerMethod = logger?.[level];
  if (typeof loggerMethod === 'function') {
    return loggerMethod.bind(logger) as KafkaTypegenLogger[typeof level];
  }

  const consoleMethod = (console as Partial<KafkaTypegenLogger>)[level];
  if (typeof consoleMethod === 'function') {
    return consoleMethod.bind(console) as KafkaTypegenLogger[typeof level];
  }

  return noopLoggerMethod as KafkaTypegenLogger[typeof level];
}

export function resolveObservability(
  options: KafkaTypegenObservabilityOptions = {}
): ResolvedKafkaTypegenObservability {
  return {
    logger: {
      debug: bindLoggerMethod(options.logger, 'debug'),
      error: bindLoggerMethod(options.logger, 'error'),
      info: bindLoggerMethod(options.logger, 'info'),
      warn: bindLoggerMethod(options.logger, 'warn')
    },
    ...(options.observer !== undefined ? { observer: options.observer } : {})
  };
}

export async function emitObservedEvent(
  observability: ResolvedKafkaTypegenObservability,
  event: KafkaTypegenObservedEvent
): Promise<void> {
  try {
    await observability.observer?.onEvent(event);
  } catch (error) {
    observability.logger.warn('Kafka typegen observer callback failed.', {
      error: toErrorString(error),
      eventType: event.type
    });
  }
}

export function toErrorString(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export type {
  KafkaTypegenLogger,
  KafkaTypegenObservedEvent,
  KafkaTypegenObservabilityOptions,
  KafkaTypegenObserver,
  ResolvedKafkaTypegenObservability
} from './observability-types.js';
