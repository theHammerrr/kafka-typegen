import { Buffer } from 'node:buffer';

export function toPlatformaticHeaders(
  headers?: Readonly<Record<string, string>>
): Record<string, Buffer> | undefined {
  if (headers === undefined) {
    return undefined;
  }

  return Object.fromEntries(Object.entries(headers).map(([key, value]) => [key, Buffer.from(value)]));
}

export function toRuntimeHeaders(
  headers: Map<Buffer, Buffer> | Record<string, Buffer> | undefined
): Readonly<Record<string, string>> | undefined {
  if (headers === undefined) {
    return undefined;
  }

  if (headers instanceof Map) {
    return Object.fromEntries(
      Array.from(headers.entries()).map(([key, value]) => [key.toString('utf8'), value.toString('utf8')])
    );
  }

  return Object.fromEntries(Object.entries(headers).map(([key, value]) => [key, value.toString('utf8')]));
}
