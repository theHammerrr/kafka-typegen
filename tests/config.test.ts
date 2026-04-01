import { describe, expect, it } from 'vitest';

import {
  ConfigValidationError,
  defineConfig,
  resolveConfig,
  validateConfig,
  type KafkaTypegenConfig
} from '../src/index.js';

describe('config scaffold', () => {
  it('accepts a minimal valid config', () => {
    const config = defineConfig({
      outputDir: './generated',
      topics: {
        'user.events': {
          events: {
            'user.created': {
              schemaPath: './schemas/user-created.avsc'
            }
          }
        }
      }
    } satisfies KafkaTypegenConfig);

    const validatedConfig = validateConfig(config);

    expect(validatedConfig).toEqual(config);
  });

  it('normalizes topics and events deterministically', () => {
    const normalized = resolveConfig({
      outputDir: './generated',
      topics: {
        'z.topic': {
          events: {
            'event.beta': {
              schemaPath: './schemas/beta.avsc'
            }
          }
        },
        'a.topic': {
          events: {
            'event.gamma': {
              schemaPath: './schemas/gamma.avsc'
            },
            'event.alpha': {
              keySchemaPath: './schemas/alpha-key.avsc',
              schemaPath: './schemas/alpha.avsc'
            }
          }
        }
      }
    });

    expect(normalized.topics.map((topic) => topic.topicName)).toEqual(['a.topic', 'z.topic']);
    expect(normalized.events.map((event) => event.eventName)).toEqual([
      'event.alpha',
      'event.gamma',
      'event.beta'
    ]);
    expect(normalized.events[0]).toMatchObject({
      keySchemaPath: './schemas/alpha-key.avsc',
      topicName: 'a.topic'
    });
  });

  it('rejects invalid config input with actionable paths', () => {
    expect(() =>
      validateConfig({
        outputDir: '',
        topics: {
          'user.events': {
            events: {}
          }
        }
      })
    ).toThrowError(ConfigValidationError);

    try {
      validateConfig({
        outputDir: '',
        topics: {
          'user.events': {
            events: {}
          }
        }
      });
    } catch (error) {
      expect(error).toBeInstanceOf(ConfigValidationError);

      const validationError = error as ConfigValidationError;

      expect(validationError.issues).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ path: 'outputDir' }),
          expect.objectContaining({ path: 'topics.user.events.events' })
        ])
      );
    }
  });
});
