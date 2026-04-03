import { Buffer } from 'node:buffer';

import type { NormalizedSyncSchemaRegistryConfig } from '../config/index.js';

export function buildSchemaRegistryHeaders(config: NormalizedSyncSchemaRegistryConfig): Record<string, string> {
  const headers: Record<string, string> = { Accept: 'application/json' };

  if (
    config.auth?.username !== undefined &&
    config.auth.password !== undefined
  ) {
    headers.Authorization = `Basic ${Buffer.from(
      `${config.auth.username}:${config.auth.password}`
    ).toString('base64')}`;
  } else if (config.auth?.token !== undefined) {
    headers.Authorization = `Bearer ${config.auth.token}`;
  }

  return headers;
}

export async function readJsonResponse(response: Response): Promise<unknown> {
  if (response.status === 404) {
    return undefined;
  }

  if (!response.ok) {
    throw new Error(`Schema Registry request failed with ${response.status} ${response.statusText}.`);
  }

  return response.json();
}
