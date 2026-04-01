import { resolve as resolvePath } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  CatalogValidationError,
  createCatalogBuilder,
  resolveConfig
} from '../src/index.js';

const schemaFixturesDir = resolvePath('tests', 'fixtures', 'schemas');

describe('event catalog', () => {
  it('builds a valid catalog for a single-event topic', async () => {
    const config = resolveConfig({
      naming: {
        eventTypeSuffix: 'Payload',
        topicTypeSuffix: 'Topic'
      },
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

    const catalog = await createCatalogBuilder().build(config);

    expect(catalog.topics).toHaveLength(1);
    expect(catalog.events).toHaveLength(1);
    expect(catalog.events[0]).toMatchObject({
      eventName: 'user.created',
      payloadTypeName: 'UserCreatedPayload',
      schemaName: 'UserCreated',
      subjectName: 'user.events-user.created',
      topicName: 'user.events',
      topicTypeName: 'UserEventsTopic'
    });
    expect(catalog.events[0]?.runtime).toMatchObject({
      eventName: 'user.created',
      schemaFilePath: resolvePath(schemaFixturesDir, 'user-created.avsc'),
      subjectName: 'user.events-user.created',
      topicName: 'user.events'
    });
  });

  it('builds a deterministic catalog for multi-event topics', async () => {
    const config = resolveConfig({
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
          name: 'z.topic'
        },
        {
          events: [
            {
              name: 'user.deleted',
              schemaPath: './user-created.avsc'
            },
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'a.topic'
        }
      ]
    });

    const catalog = await createCatalogBuilder().build(config);

    expect(catalog.topics.map((topic) => topic.topicName)).toEqual(['a.topic', 'z.topic']);
    expect(catalog.events.map((event) => event.eventName)).toEqual([
      'user.created',
      'user.deleted',
      'user.updated'
    ]);
    expect(catalog.topics[0]?.eventNames).toEqual(['user.created', 'user.deleted']);
    expect(catalog.topics[1]?.eventNames).toEqual(['user.updated']);
  });

  it('rejects naming collisions in generated payload identifiers', async () => {
    const config = resolveConfig({
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
            },
            {
              name: 'user-created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    await expect(createCatalogBuilder().build(config)).rejects.toThrow(CatalogValidationError);
    await expect(createCatalogBuilder().build(config)).rejects.toThrow(
      "Generated payload type 'UserCreatedPayload' collides"
    );
  });

  it('supports single-event and multi-event topic scenarios together', async () => {
    const config = resolveConfig({
      outputDir: './generated',
      sources: {
        rootDir: schemaFixturesDir
      },
      topics: [
        {
          events: [
            {
              name: 'account.created',
              schemaPath: './user-created.avsc'
            }
          ],
          name: 'account.events'
        },
        {
          events: [
            {
              name: 'user.created',
              schemaPath: './user-created.avsc'
            },
            {
              name: 'user.updated',
              schemaPath: './user-updated.avsc'
            }
          ],
          name: 'user.events'
        }
      ]
    });

    const catalog = await createCatalogBuilder().build(config);
    const accountTopic = catalog.topics.find((topic) => topic.topicName === 'account.events');
    const userTopic = catalog.topics.find((topic) => topic.topicName === 'user.events');

    expect(accountTopic?.events).toHaveLength(1);
    expect(userTopic?.events).toHaveLength(2);
    expect(userTopic?.events[1]?.schema.fields[1]).toMatchObject({
      name: 'displayName',
      type: 'null | string'
    });
  });
});
