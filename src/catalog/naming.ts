export function toPascalCase(value: string): string {
  const segments = value
    .split(/[^a-zA-Z0-9]+/u)
    .flatMap((segment) => segment.split(/(?=[A-Z])/u))
    .filter((segment) => segment.length > 0);

  return segments
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1).toLowerCase())
    .join('');
}

export function buildPayloadTypeName(eventName: string, suffix: string): string {
  return `${toPascalCase(eventName)}${suffix}`;
}

export function buildTopicTypeName(topicName: string, suffix: string): string {
  return `${toPascalCase(topicName)}${suffix}`;
}
