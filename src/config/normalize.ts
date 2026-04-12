import { resolve as resolvePath } from 'node:path';

import type {
  KafkaTypegenConfig,
  KafkaTypegenEventConfig,
  KafkaTypegenTopicConfig,
  NormalizedEventConfig,
  NormalizedKafkaTypegenConfig,
  NormalizedTopicConfig,
  SubjectNameStrategy
} from './types.js';
import { getRuntimeModule } from './runtime-module.js';
import { deriveSubjectName } from './subject-name.js';
import { normalizeSyncConfig } from './sync-normalize.js';

function normalizeEvent(
  event: KafkaTypegenEventConfig,
  topic: KafkaTypegenTopicConfig,
  sourceRoot: string,
  subjectStrategy: SubjectNameStrategy
): NormalizedEventConfig {
  const keySchemaPath = event.keySchemaPath ?? topic.keySchemaPath;
  const resolvedKeySchemaPath = keySchemaPath !== undefined ? resolvePath(sourceRoot, keySchemaPath) : undefined;

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

function normalizeTopic(
  topic: KafkaTypegenTopicConfig,
  sourceRoot: string,
  defaultSubjectStrategy: SubjectNameStrategy
): NormalizedTopicConfig {
  const subjectStrategy = topic.subjectStrategy ?? defaultSubjectStrategy;
  const topicKeySchemaPath = topic.keySchemaPath;
  const resolvedTopicKeySchemaPath = topicKeySchemaPath !== undefined ? resolvePath(sourceRoot, topicKeySchemaPath) : undefined;
  const events = [...topic.events]
    .sort((leftEvent, rightEvent) => leftEvent.name.localeCompare(rightEvent.name))
    .map((event) => normalizeEvent(event, topic, sourceRoot, subjectStrategy));

  return {
    events,
    topicName: topic.name,
    subjectStrategy,
    ...(topicKeySchemaPath !== undefined ? { keySchemaPath: topicKeySchemaPath } : {}),
    ...(resolvedTopicKeySchemaPath !== undefined
      ? { resolvedKeySchemaPath: resolvedTopicKeySchemaPath }
      : {}),
    ...(topic.sync !== undefined ? { sync: topic.sync } : {})
  };
}

export function normalizeConfig(config: KafkaTypegenConfig): NormalizedKafkaTypegenConfig {
  const sourceRoot = resolvePath(config.sources?.rootDir ?? process.cwd());
  const defaultSubjectStrategy = config.schemaRegistry?.subjectStrategy ?? 'topic-event';
  const normalizedTransport = config.runtime?.transport ?? 'kafkajs';
  const normalizedSyncConfig = normalizeSyncConfig(config);
  const topics = [...config.topics]
    .sort((leftTopic, rightTopic) => leftTopic.name.localeCompare(rightTopic.name))
    .map((topic) => normalizeTopic(topic, sourceRoot, defaultSubjectStrategy));

  return {
    events: topics.flatMap((topic) => topic.events),
    generation: {
      avroExternalTypes: Object.fromEntries(
        Object.entries(config.generation?.avroExternalTypes ?? {}).sort(([left], [right]) =>
          left.localeCompare(right)
        )
      ),
      avroSemanticMode: config.generation?.avroSemanticMode ?? 'default',
      typesFileName: config.generation?.typesFileName ?? 'kafka-client.ts'
    },
    naming: {
      eventTypeSuffix: config.naming?.eventTypeSuffix ?? 'Payload',
      topicTypeSuffix: config.naming?.topicTypeSuffix ?? 'Topic'
    },
    outputDir: config.outputDir,
    resolvedOutputDir: resolvePath(config.outputDir),
    runtime: { module: getRuntimeModule(normalizedTransport, config.runtime?.module), transport: normalizedTransport },
    sources: { rootDir: sourceRoot },
    topics,
    ...(normalizedSyncConfig !== undefined && Object.keys(normalizedSyncConfig).length > 0
      ? { sync: normalizedSyncConfig }
      : {}),
    ...(config.schemaRegistry !== undefined
      ? {
          schemaRegistry: {
            subjectStrategy: defaultSubjectStrategy,
            url: config.schemaRegistry.url,
            ...(config.schemaRegistry.auth !== undefined
              ? { auth: config.schemaRegistry.auth }
              : {})
          }
        }
      : {})
  };
}
