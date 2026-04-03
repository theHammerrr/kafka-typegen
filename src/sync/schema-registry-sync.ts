import type { EventCatalog } from '../catalog/index.js';
import type { NormalizedKafkaTypegenConfig } from '../config/index.js';

import { buildSchemaRegistryPlan } from './schema-registry-plan.js';
import { normalizeSchemaText } from './schema-registry-schema-text.js';
import {
  applySubjectCompatibility,
  buildCompatibilityDetails,
  registerSubjectVersion
} from './schema-registry-sync-helpers.js';
import type { SchemaRegistryClient, SyncOperation } from './types.js';

export async function executeSchemaRegistrySync(
  catalog: EventCatalog,
  config: NormalizedKafkaTypegenConfig,
  schemaRegistry: SchemaRegistryClient,
  apply: boolean
): Promise<readonly SyncOperation[]> {
  const operations: SyncOperation[] = [];
  const schemaRegistryConfig = config.sync?.schemaRegistry;

  for (const subject of buildSchemaRegistryPlan(catalog)) {
    const existing = await schemaRegistry.getLatestSubject(subject.subjectName);

    if (existing === undefined) {
      if (apply) {
        await registerSubjectVersion(schemaRegistry, subject);
        await applySubjectCompatibility(schemaRegistry, config, subject.subjectName);
      }

      operations.push({
        action: 'create',
        details: [
          apply ? 'Subject created.' : 'Subject will be created.',
          ...buildCompatibilityDetails(config, apply)
        ].join(' '),
        resourceName: subject.subjectName,
        target: 'registry'
      });
      continue;
    }

    if (normalizeSchemaText(existing.schemaText) === normalizeSchemaText(subject.schemaText)) {
      operations.push({
        action: 'noop',
        details: 'Subject already exists with matching schema.',
        resourceName: subject.subjectName,
        target: 'registry'
      });
      continue;
    }

    if (schemaRegistryConfig?.onDrift === 'register') {
      if (apply) {
        await applySubjectCompatibility(schemaRegistry, config, subject.subjectName);
        await registerSubjectVersion(schemaRegistry, subject);
      }

      operations.push({
        action: 'update',
        details: [
          apply
            ? `Registered a new schema version for event '${subject.eventName}'.`
            : `A new schema version will be registered for event '${subject.eventName}'.`,
          ...buildCompatibilityDetails(config, apply)
        ].join(' '),
        resourceName: subject.subjectName,
        target: 'registry'
      });
      continue;
    }

    operations.push({
      action: 'drift',
      details:
        schemaRegistryConfig?.onDrift === 'ignore'
          ? `Existing subject schema differs for event '${subject.eventName}' and will be left unchanged.`
          : `Existing subject schema differs for event '${subject.eventName}'.`,
      resourceName: subject.subjectName,
      target: 'registry'
    });
  }

  if (
    schemaRegistryConfig?.onDrift === 'fail' &&
    operations.some((operation) => operation.action === 'drift')
  ) {
    throw new Error(
      'Schema Registry sync detected subject drift and sync.schemaRegistry.onDrift is set to fail.'
    );
  }

  return operations;
}
