import { readFile } from 'node:fs/promises';
import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  createCatalogBuilder,
  createTypeGenerator,
  resolveConfig
} from '../src/index.js';
import { toTypeScriptType } from '../src/generator/avro-type-renderer.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');
const generatedFixturesDir = resolvePath('tests', 'fixtures', 'generated');

async function buildGeneratedOutput(configInput: Parameters<typeof resolveConfig>[0]) {
  const config = resolveConfig(configInput);
  const catalog = await createCatalogBuilder().build(config);
  return createTypeGenerator().generate(catalog);
}

function normalizeLineEndings(value: string): string {
  return value.replaceAll('\r\n', '\n');
}

describe('type generation', () => {
  it('matches the single-event generated output', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents = output.files.find((file) => file.filePath === 'kafka-client.ts')?.contents ?? '';
    const expected = await readFile(resolvePath(generatedFixturesDir, 'single-event.ts'), 'utf8');

    expect(normalizeLineEndings(contents)).toBe(normalizeLineEndings(expected));
    expect(output.files.map((file) => file.filePath)).toEqual(['kafka-client.ts', 'index.ts']);
  });

  it('matches the multi-event generated output deterministically', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.updated',
              schemaPath: './user-updated.avsc'
            }
          ],
          name: 'user.lifecycle'
        },
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents = output.files.find((file) => file.filePath === 'kafka-client.ts')?.contents ?? '';
    const expected = await readFile(resolvePath(generatedFixturesDir, 'multi-event.ts'), 'utf8');

    expect(normalizeLineEndings(contents)).toBe(normalizeLineEndings(expected));
  });

  it('emits only a source-file and index re-export for direct relative imports', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    expect(output.files.map((file) => file.filePath)).toEqual([
      'kafka-client.ts',
      'index.ts'
    ]);
    expect(output.files.find((file) => file.filePath === 'index.ts')?.contents)
      .toBe("export * from './kafka-client.js';\n");
  });

  it('emits a schema registry config constant with url only', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      schemaRegistry: {
        auth: {
          password: 'secret-password',
          username: 'registry-user'
        },
        url: 'http://localhost:8081'
      },
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain("export const SchemaRegistryConfig = {\n  url: 'http://localhost:8081'\n} as const;");
    expect(contents).not.toContain('secret-password');
    expect(contents).not.toContain('registry-user');
  });

  it('emits nested named declarations and logical types from Avro schemas', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'user.profiled',
              schemaPath: './user-profile.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain(`export type UserStatus = 'ACTIVE' | 'DISABLED';`);
    expect(contents).toContain(`export interface Address {
  street: string;
  createdAt: AvroTimestampMillis;
}`);
    expect(contents).toContain(`export interface UserProfiledPayload {
  id: string;
  status: UserStatus;
  primaryAddress: Address;
  shippingAddress: Address;
  birthDate: AvroDate;
  balance: null | AvroDecimal;
}`);
  });

  it('resolves cross-file named references and recursive record references', async () => {
    const output = await buildGeneratedOutput({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'address.shared',
              schemaPath: './shared-address.avsc'
            },
            {
              name: 'user.addressReferenced',
              schemaPath: './user-address-referenced.avsc'
            },
            {
              name: 'user.nodeLinked',
              schemaPath: './user-node.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });
    const contents =
      output.files.find((file) => file.filePath === 'kafka-client.ts')
        ?.contents ?? '';

    expect(contents).toContain(`export interface AddressSharedPayload {
  street: string;
}`);
    expect(contents).toContain(`export interface UserAddressReferencedPayload {
  primaryAddress: AddressSharedPayload;
  shippingAddress: AddressSharedPayload;
}`);
    expect(contents).toContain(`export interface UserNodeLinkedPayload {
  id: string;
  next: null | UserNodeLinkedPayload;
}`);
  });

  it('renders Avro references, logical types, nested records, unions, arrays, maps, and enums', () => {
    expect(toTypeScriptType('com.example.UserCreated')).toBe('UserCreated');
    expect(toTypeScriptType({ logicalType: 'uuid', type: 'string' })).toBe('string');
    expect(toTypeScriptType({ logicalType: 'date', type: 'int' })).toBe('AvroDate');
    expect(toTypeScriptType({ logicalType: 'timestamp-millis', type: 'long' })).toBe('AvroTimestampMillis');
    expect(toTypeScriptType({ logicalType: 'decimal', type: 'bytes' })).toBe('AvroDecimal');
    expect(toTypeScriptType(['null', 'string'])).toBe('null | string');
    expect(toTypeScriptType({
      items: 'string',
      type: 'array'
    })).toBe('string[]');
    expect(toTypeScriptType({
      type: 'map',
      values: 'long'
    })).toBe('Record<string, number>');
    expect(toTypeScriptType({
      symbols: ['ACTIVE', 'DISABLED'],
      type: 'enum'
    })).toBe("'ACTIVE' | 'DISABLED'");
    expect(toTypeScriptType({
      fields: [
        {
          name: 'nestedId',
          type: 'string'
        }
      ],
      name: 'NestedRecord',
      type: 'record'
    })).toBe(`{
  nestedId: string;
}`);
  });

  it('fails loudly for unsupported Avro types', () => {
    expect(() => toTypeScriptType({ type: { nested: true } })).toThrowError(
      'Unsupported Avro complex type'
    );
    expect(() => toTypeScriptType(42)).toThrowError(
      "Unsupported Avro type definition at 'schema': 42."
    );
    expect(() => toTypeScriptType('not a valid type name')).toThrowError(
      "Unsupported Avro type 'not a valid type name' at 'schema'."
    );
  });
});
