import { relative as relativePath } from 'node:path';

import type { EventCatalog } from '../catalog/index.js';

export function indent(value: string, depth = 1): string {
  const prefix = '  '.repeat(depth);

  return value
    .split('\n')
    .map((line) => (line.length > 0 ? `${prefix}${line}` : line))
    .join('\n');
}

export function formatPropertyName(name: string): string {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/u.test(name) ? name : `'${name}'`;
}

export function formatLiteral(value: string): string {
  return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`;
}

export function toGeneratedSchemaPath(catalog: EventCatalog, schemaFilePath: string): string {
  return relativePath(catalog.config.sources.rootDir, schemaFilePath);
}

export function toCamelCase(value: string): string {
  const pascalCaseValue = value
    .split(/[^a-zA-Z0-9]+/u)
    .flatMap((segment) => segment.split(/(?=[A-Z])/u))
    .filter((segment) => segment.length > 0)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1).toLowerCase())
    .join('');

  return pascalCaseValue.length > 0
    ? `${pascalCaseValue.charAt(0).toLowerCase()}${pascalCaseValue.slice(1)}`
    : 'event';
}
