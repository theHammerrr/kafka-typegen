import { join, resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  AvroSchemaParser,
  FileSystemSchemaLoader,
  SchemaLoadError,
  SchemaParseError,
  createEventSchemaLoader,
  resolveConfig
} from '../src/index.js';

const fixturesDir = resolvePath('tests', 'fixtures', 'schemas');

describe('schema loading', () => {
  it('loads a valid schema file from disk', async () => {
    const loader = new FileSystemSchemaLoader();
    const filePath = join(fixturesDir, 'user-created.avsc');

    const result = await loader.load({ filePath });

    expect(result.filePath).toBe(filePath);
    expect(result.source.filePath).toBe(filePath);
    expect(result.rawSchema).toContain('"UserCreated"');
  });

  it('reports missing schema files with the requested path', async () => {
    const loader = new FileSystemSchemaLoader();
    const filePath = join(fixturesDir, 'missing.avsc');

    await expect(loader.load({ filePath })).rejects.toMatchObject({
      filePath,
      name: 'SchemaLoadError'
    });
  });
});

describe('schema parsing', () => {
  it('extracts normalized metadata from a valid Avro record schema', async () => {
    const loader = new FileSystemSchemaLoader();
    const parser = new AvroSchemaParser();
    const filePath = join(fixturesDir, 'user-created.avsc');

    const parsedSchema = await parser.parse(await loader.load({ filePath }));

    expect(parsedSchema.filePath).toBe(filePath);
    expect(parsedSchema.name).toBe('UserCreated');
    expect(parsedSchema.namespace).toBe('com.example.users');
    expect(parsedSchema.rootType).toBe('record');
    expect(parsedSchema.fields).toEqual([
      {
        name: 'id',
        path: 'UserCreated.id',
        rawType: 'string',
        type: 'string'
      },
      {
        name: 'email',
        path: 'UserCreated.email',
        rawType: 'string',
        type: 'string'
      },
      {
        name: 'isAdmin',
        path: 'UserCreated.isAdmin',
        rawType: 'boolean',
        type: 'boolean'
      }
    ]);
  });

  it('accepts top-level Avro enum roots', async () => {
    const loader = new FileSystemSchemaLoader();
    const parser = new AvroSchemaParser();
    const filePath = join(fixturesDir, 'user-status.avsc');

    const parsedSchema = await parser.parse(await loader.load({ filePath }));

    expect(parsedSchema.filePath).toBe(filePath);
    expect(parsedSchema.name).toBe('UserStatus');
    expect(parsedSchema.namespace).toBe('com.example.users');
    expect(parsedSchema.rootType).toBe('enum');
    expect(parsedSchema.fields).toEqual([]);
  });

  it('accepts top-level Avro fixed roots', async () => {
    const loader = new FileSystemSchemaLoader();
    const parser = new AvroSchemaParser();
    const filePath = join(fixturesDir, 'session-token.avsc');

    const parsedSchema = await parser.parse(await loader.load({ filePath }));

    expect(parsedSchema.filePath).toBe(filePath);
    expect(parsedSchema.name).toBe('SessionToken');
    expect(parsedSchema.namespace).toBe('com.example.users');
    expect(parsedSchema.rootType).toBe('fixed');
    expect(parsedSchema.fields).toEqual([]);
  });

  it('rejects malformed JSON schema content', async () => {
    const loader = new FileSystemSchemaLoader();
    const parser = new AvroSchemaParser();
    const filePath = join(fixturesDir, 'invalid-json.avsc');

    await expect(parser.parse(await loader.load({ filePath }))).rejects.toMatchObject({
      filePath,
      name: 'SchemaParseError'
    });
  });

  it('rejects unsupported top-level Avro schema roots', async () => {
    const loader = new FileSystemSchemaLoader();
    const parser = new AvroSchemaParser();
    const filePath = join(fixturesDir, 'invalid-root.avsc');

    await expect(parser.parse(await loader.load({ filePath }))).rejects.toThrow(
      'must define a top-level Avro record, enum, or fixed'
    );
  });

  it('rejects malformed top-level Avro enum roots', async () => {
    const loader = new FileSystemSchemaLoader();
    const parser = new AvroSchemaParser();
    const filePath = join(fixturesDir, 'invalid-enum.avsc');

    await expect(parser.parse(await loader.load({ filePath }))).rejects.toThrow(
      "must define a 'symbols' array for the enum"
    );
  });

  it('rejects malformed top-level Avro fixed roots', async () => {
    const loader = new FileSystemSchemaLoader();
    const parser = new AvroSchemaParser();
    const filePath = join(fixturesDir, 'invalid-fixed.avsc');

    await expect(parser.parse(await loader.load({ filePath }))).rejects.toThrow(
      "must define a positive integer 'size' for the fixed type"
    );
  });
});

describe('event schema integration', () => {
  it('loads multiple schemas from normalized config events', async () => {
    const normalizedConfig = resolveConfig({
      outputDir: './generated',
      sources: {
        rootDir: fixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.updated',
              schemaPath: './user-updated.avsc'
            }
          ],
          name: 'z.topic'
        },
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'a.topic'
        }
      ]
    });

    const eventSchemaLoader = createEventSchemaLoader();
    const schemas = await eventSchemaLoader.loadEventSchemas(normalizedConfig.events);

    expect(schemas.map((schema) => `${schema.topicName}:${schema.eventName}:${schema.schema.name}`)).toEqual([
      'a.topic:user.created:UserCreated',
      'z.topic:user.updated:UserUpdated'
    ]);
    expect(schemas[1]?.schema.fields[1]).toMatchObject({
      name: 'displayName',
      path: 'UserUpdated.displayName',
      type: 'null | string'
    });
  });

  it('resolves cross-file Avro named references through a shared schema registry', async () => {
    const normalizedConfig = resolveConfig({
      outputDir: './generated',
      sources: {
        rootDir: fixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.addressReferenced',
              schemaPath: './user-address-referenced.avsc'
            },
            {
              name: 'address.shared',
              schemaPath: './shared-address.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const schemas = await createEventSchemaLoader().loadEventSchemas(
      normalizedConfig.events
    );

    expect(schemas.map((schema) => schema.schema.name)).toEqual([
      'SharedAddress',
      'UserAddressReferenced'
    ]);
    expect(
      schemas.find((schema) => schema.schema.name === 'UserAddressReferenced')
        ?.schema.fields
    ).toEqual([
      {
        name: 'primaryAddress',
        path: 'UserAddressReferenced.primaryAddress',
        rawType: 'SharedAddress',
        type: 'SharedAddress'
      },
      {
        name: 'shippingAddress',
        path: 'UserAddressReferenced.shippingAddress',
        rawType: 'com.example.users.SharedAddress',
        type: 'com.example.users.SharedAddress'
      }
    ]);
  });

  it('rejects duplicate top-level Avro schema names across files', async () => {
    const normalizedConfig = resolveConfig({
      outputDir: './generated',
      sources: {
        rootDir: fixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            },
            {
              name: 'user.createdDuplicate',
              schemaPath: './duplicate-user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    await expect(
      createEventSchemaLoader().loadEventSchemas(normalizedConfig.events)
    ).rejects.toThrow(
      "Duplicate top-level Avro schema name 'com.example.users.UserCreated'"
    );
  });

  it('rejects duplicate top-level Avro schema names across mixed root kinds', async () => {
    const normalizedConfig = resolveConfig({
      outputDir: './generated',
      sources: {
        rootDir: fixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.statusEnum',
              schemaPath: './user-status.avsc'
            },
            {
              name: 'user.statusDuplicate',
              schemaPath: './duplicate-user-status.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    await expect(
      createEventSchemaLoader().loadEventSchemas(normalizedConfig.events)
    ).rejects.toThrow(
      "Duplicate top-level Avro schema name 'com.example.users.UserStatus'"
    );
  });
});
