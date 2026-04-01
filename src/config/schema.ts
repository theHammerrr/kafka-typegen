import { resolve as resolvePath } from 'node:path';

import { z } from 'zod';

import type {
  ConfigValidationIssue,
  KafkaTypegenConfig,
  KafkaTypegenEventConfig,
  KafkaTypegenTopicConfig,
  NormalizedEventConfig,
  NormalizedKafkaTypegenConfig,
  NormalizedTopicConfig,
  SubjectNameStrategy
} from './types.js';
import { ConfigValidationError } from './types.js';

const nonEmptyStringSchema = z.string().trim().min(1, 'Expected a non-empty string.');
const subjectNameStrategySchema = z.enum(['event-name', 'topic-name', 'topic-event']);
const runtimeTransportSchema = z.enum(['kafkajs']);

const eventConfigSchema = z.object({
  keySchemaPath: nonEmptyStringSchema.optional(),
  name: nonEmptyStringSchema,
  schemaPath: nonEmptyStringSchema,
  subject: nonEmptyStringSchema.optional()
});

const topicConfigSchema = z.object({
  events: z.array(eventConfigSchema).min(1, 'Each topic must define at least one event.'),
  keySchemaPath: nonEmptyStringSchema.optional(),
  name: nonEmptyStringSchema,
  subjectStrategy: subjectNameStrategySchema.optional()
});

const kafkaTypegenConfigSchema = z.object({
  generation: z
    .object({
      clientName: nonEmptyStringSchema.optional(),
      typesFileName: nonEmptyStringSchema.optional()
    })
    .optional(),
  naming: z
    .object({
      eventTypeSuffix: nonEmptyStringSchema.optional(),
      topicTypeSuffix: nonEmptyStringSchema.optional()
    })
    .optional(),
  outputDir: nonEmptyStringSchema,
  runtime: z
    .object({
      module: nonEmptyStringSchema.optional(),
      transport: runtimeTransportSchema.optional()
    })
    .optional(),
  schemaRegistry: z
    .object({
      subjectStrategy: subjectNameStrategySchema.optional(),
      url: nonEmptyStringSchema
    })
    .optional(),
  sources: z
    .object({
      rootDir: nonEmptyStringSchema.optional()
    })
    .optional(),
  topics: z.array(topicConfigSchema).min(1, 'At least one topic must be configured.')
});

function formatIssuePath(path: readonly (string | number)[]): string {
  if (path.length === 0) {
    return 'config';
  }

  return path.reduce<string>((currentPath, segment) => {
    if (typeof segment === 'number') {
      return `${currentPath}[${segment}]`;
    }

    return currentPath.length === 0 ? segment : `${currentPath}.${segment}`;
  }, '');
}

function buildValidationIssue(path: readonly (string | number)[], message: string): ConfigValidationIssue {
  return {
    message,
    path: formatIssuePath(path)
  };
}

function deriveSubjectName(
  topicName: string,
  eventName: string,
  strategy: SubjectNameStrategy,
  explicitSubject?: string
): string {
  if (explicitSubject !== undefined) {
    return explicitSubject;
  }

  switch (strategy) {
    case 'event-name':
      return eventName;
    case 'topic-name':
      return topicName;
    case 'topic-event':
      return `${topicName}-${eventName}`;
  }
}

function validateSemanticConfig(config: KafkaTypegenConfig): void {
  const issues: ConfigValidationIssue[] = [];
  const topicNameIndexes = new Map<string, number>();
  const eventNameIndexes = new Map<string, { topicIndex: number; eventIndex: number }>();

  config.topics.forEach((topic, topicIndex) => {
    const existingTopicIndex = topicNameIndexes.get(topic.name);

    if (existingTopicIndex !== undefined) {
      issues.push(
        buildValidationIssue(
          ['topics', topicIndex, 'name'],
          `Duplicate topic name '${topic.name}'. First defined at topics[${existingTopicIndex}].name.`
        )
      );
    } else {
      topicNameIndexes.set(topic.name, topicIndex);
    }

    topic.events.forEach((event, eventIndex) => {
      const existingEvent = eventNameIndexes.get(event.name);

      if (existingEvent !== undefined) {
        issues.push(
          buildValidationIssue(
            ['topics', topicIndex, 'events', eventIndex, 'name'],
            `Duplicate event name '${event.name}'. First defined at topics[${existingEvent.topicIndex}].events[${existingEvent.eventIndex}].name.`
          )
        );
      } else {
        eventNameIndexes.set(event.name, { eventIndex, topicIndex });
      }
    });
  });

  if (issues.length > 0) {
    throw new ConfigValidationError(issues);
  }
}

function normalizeTopic(
  topic: KafkaTypegenTopicConfig,
  sourceRoot: string,
  defaultSubjectStrategy: SubjectNameStrategy
): NormalizedTopicConfig {
  const subjectStrategy = topic.subjectStrategy ?? defaultSubjectStrategy;
  const topicKeySchemaPath = topic.keySchemaPath;
  const resolvedTopicKeySchemaPath =
    topicKeySchemaPath !== undefined ? resolvePath(sourceRoot, topicKeySchemaPath) : undefined;

  const events: NormalizedEventConfig[] = [...topic.events]
    .sort((leftEvent, rightEvent) => leftEvent.name.localeCompare(rightEvent.name))
    .map((event): NormalizedEventConfig => normalizeEvent(event, topic, sourceRoot, subjectStrategy));

  return {
    events,
    topicName: topic.name,
    subjectStrategy,
    ...(topicKeySchemaPath !== undefined ? { keySchemaPath: topicKeySchemaPath } : {}),
    ...(resolvedTopicKeySchemaPath !== undefined
      ? { resolvedKeySchemaPath: resolvedTopicKeySchemaPath }
      : {})
  };
}

function normalizeEvent(
  event: KafkaTypegenEventConfig,
  topic: KafkaTypegenTopicConfig,
  sourceRoot: string,
  subjectStrategy: SubjectNameStrategy
): NormalizedEventConfig {
  const keySchemaPath = event.keySchemaPath ?? topic.keySchemaPath;
  const resolvedKeySchemaPath =
    keySchemaPath !== undefined ? resolvePath(sourceRoot, keySchemaPath) : undefined;

  return {
    eventName: event.name,
    resolvedSchemaPath: resolvePath(sourceRoot, event.schemaPath),
    schemaPath: event.schemaPath,
    subjectName: deriveSubjectName(topic.name, event.name, subjectStrategy, event.subject),
    topicName: topic.name,
    ...(keySchemaPath !== undefined ? { keySchemaPath } : {}),
    ...(resolvedKeySchemaPath !== undefined ? { resolvedKeySchemaPath } : {})
  };
}

export function validateConfig(config: unknown): KafkaTypegenConfig {
  const parsedConfig = kafkaTypegenConfigSchema.safeParse(config);

  if (!parsedConfig.success) {
    throw new ConfigValidationError(
      parsedConfig.error.issues.map<ConfigValidationIssue>((issue) =>
        buildValidationIssue(issue.path, issue.message)
      )
    );
  }

  const validatedConfig = parsedConfig.data as KafkaTypegenConfig;

  validateSemanticConfig(validatedConfig);

  return validatedConfig;
}

export function normalizeConfig(config: KafkaTypegenConfig): NormalizedKafkaTypegenConfig {
  const sourceRoot = resolvePath(config.sources?.rootDir ?? process.cwd());
  const defaultSubjectStrategy = config.schemaRegistry?.subjectStrategy ?? 'topic-event';

  const topics = [...config.topics]
    .sort((leftTopic, rightTopic) => leftTopic.name.localeCompare(rightTopic.name))
    .map((topic) => normalizeTopic(topic, sourceRoot, defaultSubjectStrategy));

  return {
    events: topics.flatMap((topic) => topic.events),
    generation: {
      clientName: config.generation?.clientName ?? 'KafkaTypegenClient',
      typesFileName: config.generation?.typesFileName ?? 'kafka-client.ts'
    },
    naming: {
      eventTypeSuffix: config.naming?.eventTypeSuffix ?? 'Payload',
      topicTypeSuffix: config.naming?.topicTypeSuffix ?? 'Topic'
    },
    outputDir: config.outputDir,
    resolvedOutputDir: resolvePath(config.outputDir),
    runtime: {
      module: config.runtime?.module ?? 'kafka-typegen/runtime',
      transport: config.runtime?.transport ?? 'kafkajs'
    },
    sources: {
      rootDir: sourceRoot
    },
    topics,
    ...(config.schemaRegistry !== undefined
      ? {
          schemaRegistry: {
            subjectStrategy: defaultSubjectStrategy,
            url: config.schemaRegistry.url
          }
        }
      : {})
  };
}
