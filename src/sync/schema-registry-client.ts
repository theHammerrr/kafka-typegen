import type {
  NormalizedSyncSchemaRegistryConfig,
  SchemaRegistryCompatibility
} from '../config/index.js';

import { buildSchemaRegistryHeaders, readJsonResponse } from './schema-registry-http.js';
import type { DesiredSchemaRegistrySubject, RemoteSchemaRegistrySubject, SchemaRegistryClient } from './types.js';

export class HttpSchemaRegistryClient implements SchemaRegistryClient {
  public constructor(private readonly config: NormalizedSyncSchemaRegistryConfig) {}

  public async getLatestSubject(subjectName: string): Promise<RemoteSchemaRegistrySubject | undefined> {
    const response = await fetch(
      `${this.config.url}/subjects/${encodeURIComponent(subjectName)}/versions/latest`,
      { headers: buildSchemaRegistryHeaders(this.config) }
    );
    const payload = await readJsonResponse(response);

    return payload === undefined
      ? undefined
      : { schemaText: String((payload as { schema: string }).schema), subjectName };
  }

  public async registerSubject(subject: DesiredSchemaRegistrySubject): Promise<void> {
    const response = await fetch(`${this.config.url}/subjects/${encodeURIComponent(subject.subjectName)}/versions`, {
      body: JSON.stringify({ schema: subject.schemaText, schemaType: 'AVRO' }),
      headers: {
        ...buildSchemaRegistryHeaders(this.config),
        'Content-Type': 'application/vnd.schemaregistry.v1+json'
      },
      method: 'POST'
    });

    await readJsonResponse(response);
  }

  public async updateSubjectCompatibility(
    subjectName: string,
    compatibility: SchemaRegistryCompatibility
  ): Promise<void> {
    const response = await fetch(
      `${this.config.url}/config/${encodeURIComponent(subjectName)}`,
      {
        body: JSON.stringify({ compatibility }),
        headers: {
          ...buildSchemaRegistryHeaders(this.config),
          'Content-Type': 'application/vnd.schemaregistry.v1+json'
        },
        method: 'PUT'
      }
    );

    await readJsonResponse(response);
  }
}
