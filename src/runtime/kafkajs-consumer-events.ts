import type {
  KafkaJsConsumerLike,
  KafkaJsConsumerTransportOptions
} from './kafkajs-types.js';

import { reportKafkaJsConsumerError } from './kafkajs-consumer-options.js';

interface KafkaJsConsumerEventSource {
  readonly events?: {
    readonly CRASH?: string;
  };
  on?: (
    eventName: string,
    listener: (event: { payload?: { error?: unknown } }) => void
  ) => unknown;
}

export function bindKafkaJsCrashHandler(
  consumer: KafkaJsConsumerLike,
  options: KafkaJsConsumerTransportOptions
): void {
  const crashEvent = (consumer as KafkaJsConsumerEventSource).events?.CRASH;
  const nativeOn = (consumer as KafkaJsConsumerEventSource).on?.bind(consumer);

  if (crashEvent === undefined || nativeOn === undefined) {
    return;
  }

  nativeOn(crashEvent, (event) => {
    reportKafkaJsConsumerError(event.payload?.error ?? event, options.onError);
  });
}
