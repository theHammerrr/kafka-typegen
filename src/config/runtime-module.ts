import type { RuntimeTransport } from './types.js';

export function getRuntimeModule(transport: RuntimeTransport, explicitModule?: string): string {
  if (explicitModule !== undefined) {
    return explicitModule;
  }

  return transport === '@platformatic/kafka' ? 'kafka-typegen/runtime/platformatic' : 'kafka-typegen/runtime';
}
