import { Buffer } from 'node:buffer';

import type { KafkaMessage } from 'kafkajs';

import type { RuntimeIncomingMessage } from './types.js';

function toRuntimeHeaderValue(value: Buffer | string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  return Buffer.isBuffer(value) ? value.toString('utf8') : value;
}

function toRuntimeHeaders(
  headers: KafkaMessage['headers']
): Readonly<Record<string, string>> | undefined {
  if (headers === undefined) {
    return undefined;
  }

  const runtimeHeaders: Record<string, string> = {};

  for (const [key, value] of Object.entries(headers)) {
    const headerValue = Array.isArray(value)
      ? toRuntimeHeaderValue(value[0])
      : toRuntimeHeaderValue(value);

    if (headerValue !== undefined) {
      runtimeHeaders[key] = headerValue;
    }
  }

  return Object.keys(runtimeHeaders).length > 0 ? runtimeHeaders : undefined;
}

function toRuntimeValue(value: Buffer | string | null): Uint8Array {
  if (value === null) {
    return new Uint8Array();
  }

  return Buffer.isBuffer(value) ? value : Buffer.from(value);
}

export function toRuntimeIncomingMessage(
  payload: {
    readonly message: KafkaMessage;
    readonly partition: number;
    readonly topic: string;
  }
): RuntimeIncomingMessage {
  const headers = toRuntimeHeaders(payload.message.headers);

  return {
    topicName: payload.topic,
    value: toRuntimeValue(payload.message.value),
    ...(headers !== undefined ? { headers } : {}),
    ...(payload.message.key !== undefined && payload.message.key !== null
      ? { key: payload.message.key }
      : {}),
    ...(payload.message.offset !== undefined
      ? { offset: payload.message.offset }
      : {}),
    ...(payload.partition !== undefined ? { partition: payload.partition } : {}),
    ...(payload.message.timestamp !== undefined
      ? { timestamp: payload.message.timestamp }
      : {})
  };
}
