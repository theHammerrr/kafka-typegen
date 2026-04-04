import type { Type } from 'avsc';

export function cloneAvroRegistry(
  registry: Readonly<Record<string, Type>>
): Record<string, Type> {
  return { ...registry };
}

export function restoreAvroRegistry(
  registry: Record<string, Type>,
  snapshot: Readonly<Record<string, Type>>
): void {
  for (const key of Object.keys(registry)) {
    delete registry[key];
  }

  Object.assign(registry, snapshot);
}
