import { Buffer } from 'node:buffer';

import type {
  ConfluentSchemaRegistryRuntimeOptions,
  SchemaRegistryRuntimeClient,
  SchemaRegistryRuntimeSchema
} from './types.js';

function buildHeaders(
  options: ConfluentSchemaRegistryRuntimeOptions
): Record<string, string> {
  const headers: Record<string, string> = { Accept: 'application/json' };

  if (options.auth?.token !== undefined) {
    headers.Authorization = `Bearer ${options.auth.token}`;
    return headers;
  }

  if (options.auth?.username !== undefined && options.auth.password !== undefined) {
    headers.Authorization = `Basic ${Buffer.from(
      `${options.auth.username}:${options.auth.password}`
    ).toString('base64')}`;
  }

  return headers;
}

async function readRegistryResponse(
  response: Response,
  resourceName: string
): Promise<unknown> {
  if (!response.ok) {
    throw new Error(
      `Schema Registry request for ${resourceName} failed with ${response.status} ${response.statusText}.`
    );
  }

  return response.json();
}

export function createConfluentSchemaRegistryRuntimeClient(
  options: ConfluentSchemaRegistryRuntimeOptions
): SchemaRegistryRuntimeClient {
  const headers = buildHeaders(options);
  const baseUrl = options.url.replace(/\/+$/, '');

  return {
    async getLatestSchema(subjectName: string): Promise<SchemaRegistryRuntimeSchema> {
      const response = await fetch(
        `${baseUrl}/subjects/${encodeURIComponent(subjectName)}/versions/latest`,
        { headers }
      );
      const payload = (await readRegistryResponse(response, `subject '${subjectName}'`)) as {
        id: number;
        schema: string;
        subject?: string;
      };

      return {
        schema: payload.schema,
        schemaId: payload.id,
        ...(payload.subject !== undefined ? { subjectName: payload.subject } : {})
      };
    },
    async getSchemaById(schemaId: number): Promise<SchemaRegistryRuntimeSchema> {
      const response = await fetch(`${baseUrl}/schemas/ids/${schemaId}`, { headers });
      const payload = (await readRegistryResponse(response, `schema id '${schemaId}'`)) as {
        schema: string;
      };

      return {
        schema: payload.schema,
        schemaId
      };
    }
  };
}
