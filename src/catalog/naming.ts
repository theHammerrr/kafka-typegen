export function toPascalCase(value: string): string {
  const segments = value
    .split(/[^a-zA-Z0-9]+/u)
    .flatMap((segment) => segment.split(/(?=[A-Z])/u))
    .filter((segment) => segment.length > 0);

  return segments
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1).toLowerCase())
    .join('');
}

export function toCamelCase(value: string): string {
  const pascalCaseValue = toPascalCase(value);

  return pascalCaseValue.length > 0
    ? `${pascalCaseValue.charAt(0).toLowerCase()}${pascalCaseValue.slice(1)}`
    : 'topic';
}

export function buildPayloadTypeName(eventName: string, suffix: string): string {
  return `${toPascalCase(eventName)}${suffix}`;
}

export function buildTopicTypeName(topicName: string, suffix: string): string {
  return `${toPascalCase(topicName)}${suffix}`;
}

export function buildTopicPropertyName(topicName: string): string {
  return toCamelCase(topicName);
}
