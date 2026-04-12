export function reportKafkaJsConsumerError(
  error: unknown,
  onError: ((error: unknown) => void) | undefined
): void {
  if (onError !== undefined) {
    onError(error);
    return;
  }

  queueMicrotask(() => {
    throw error;
  });
}

function stringifyOptionValue(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stringifyOptionValue(item)).join(',')}]`;
  }

  return `{${Object.entries(value as Record<string, unknown>)
    .sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey))
    .map(([key, entryValue]) => `${JSON.stringify(key)}:${stringifyOptionValue(entryValue)}`)
    .join(',')}}`;
}

export function buildKafkaJsConsumerOptionsSignature(options: unknown): string {
  return stringifyOptionValue(options ?? {});
}
