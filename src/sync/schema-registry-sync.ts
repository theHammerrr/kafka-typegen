import type { EventCatalog } from '../catalog/index.js';
import type { NormalizedKafkaTypegenConfig } from '../config/index.js';

import { buildSchemaRegistryPlan } from './schema-registry-plan.js';
import {
  buildSchemaEvolutionDetails,
  buildSchemaEvolutionFailure
} from './schema-registry-evolution.js';
import { assertSchemaRegistryDriftAllowed } from './schema-registry-sync-errors.js';
import { buildRegistryDriftDetails } from './schema-registry-sync-details.js';
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
      const evolutionHints = buildSchemaEvolutionDetails(
        existing.schemaText,
        subject.schemaText
      );
      if (apply) {
        await applySubjectCompatibility(schemaRegistry, config, subject.subjectName);

        try {
          await registerSubjectVersion(schemaRegistry, subject);
        } catch (error) {
          throw buildSchemaEvolutionFailure(
            error,
            existing.schemaText,
            subject.schemaText
          );
        }
      }

      operations.push({
        action: 'update',
        details: [
          apply
            ? `Registered a new schema version for event '${subject.eventName}'.`
            : `A new schema version will be registered for event '${subject.eventName}'.`,
          ...buildCompatibilityDetails(config, apply),
          ...evolutionHints
        ].join(' '),
        resourceName: subject.subjectName,
        target: 'registry'
      });
      continue;
    }

    operations.push({
      action: 'drift',
      details: buildRegistryDriftDetails(
        subject.eventName,
        existing.schemaText,
        subject.schemaText,
        schemaRegistryConfig?.onDrift === 'ignore'
      ),
      resourceName: subject.subjectName,
      target: 'registry'
    });
  }

  assertSchemaRegistryDriftAllowed(operations, schemaRegistryConfig?.onDrift === 'fail');

  return operations;
}
