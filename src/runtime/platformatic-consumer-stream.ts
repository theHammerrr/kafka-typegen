import { emitObservedEvent, type ResolvedKafkaTypegenObservability, toErrorString } from '../observability.js';
import { reportPlatformaticConsumerError } from './platformatic-consumer-options.js';
import { toRuntimeIncomingMessage } from './platformatic-message.js';
import type { RuntimeIncomingMessage } from './types.js';
import type { PlatformaticMessage } from './platformatic-types.js';

type PlatformaticTopicHandler = (message: RuntimeIncomingMessage) => Promise<void> | void;

export function attachPlatformaticConsumerStream<TKey>(
  stream: NodeJS.EventEmitter,
  topicName: string,
  topicHandlers: Set<PlatformaticTopicHandler>,
  onError: ((error: unknown) => void) | undefined,
  observability?: ResolvedKafkaTypegenObservability
): void {
  stream.on('data', (message) => {
    const runtimeMessage = toRuntimeIncomingMessage(message as PlatformaticMessage<TKey>);
    for (const topicHandler of topicHandlers) {
      void Promise.resolve(topicHandler(runtimeMessage)).catch(async (error) => {
        observability?.logger.error('Platformatic topic handler failed.', { error: toErrorString(error), topicName });
        if (observability !== undefined) {
          await emitObservedEvent(observability, {
            error: toErrorString(error),
            source: 'platformatic-handler',
            topicName,
            type: 'runtime.consumer.background-error'
          });
        }
        reportPlatformaticConsumerError(error, onError);
      });
    }
  });

  stream.on('error', (error) => {
    observability?.logger.error('Platformatic consumer stream error.', { error: toErrorString(error), topicName });
    if (observability !== undefined) {
      void emitObservedEvent(observability, {
        error: toErrorString(error),
        source: 'platformatic-stream',
        topicName,
        type: 'runtime.consumer.background-error'
      });
    }
    reportPlatformaticConsumerError(error, onError);
  });
}
