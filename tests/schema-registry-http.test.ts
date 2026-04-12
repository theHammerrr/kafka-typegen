import { describe, expect, it } from 'vitest';

import {
  buildSchemaRegistryHeaders,
  readJsonResponse
} from '../src/sync/schema-registry-http.js';

describe('schema registry HTTP helpers', () => {
  it('builds basic auth headers when username and password are configured', () => {
    expect(
      buildSchemaRegistryHeaders({
        auth: {
          password: 'registry-password',
          username: 'registry-user'
        },
        onDrift: 'register',
        url: 'http://localhost:8081'
      })
    ).toEqual({
      Accept: 'application/json',
      Authorization: 'Basic cmVnaXN0cnktdXNlcjpyZWdpc3RyeS1wYXNzd29yZA=='
    });
  });

  it('builds bearer auth headers when token auth is configured', () => {
    expect(
      buildSchemaRegistryHeaders({
        auth: {
          token: 'registry-token'
        },
        onDrift: 'register',
        url: 'http://localhost:8081'
      })
    ).toEqual({
      Accept: 'application/json',
      Authorization: 'Bearer registry-token'
    });
  });

  it('surfaces response bodies in registry HTTP failures', async () => {
    await expect(
      readJsonResponse(
        new Response('{"message":"compatibility rejected"}', {
          status: 409,
          statusText: 'Conflict'
        })
      )
    ).rejects.toThrowError(
      'Schema Registry request failed with 409 Conflict: {"message":"compatibility rejected"}'
    );
  });
});
