import avsc from 'avsc';
import type { Schema } from 'avsc';

import type { EventCatalog } from '../catalog/index.js';
import type { NormalizedKafkaTypegenConfig } from '../config/index.js';

import { buildSchemaRegistryPlan } from './schema-registry-plan.js';
import { stableStringify } from './stable-stringify.js';
import type { SchemaRegistryClient, SyncOperation } from './types.js';

const { Type } = avsc;

export async function executeSchemaRegistrySync(
  catalog: EventCatalog,
  config: NormalizedKafkaTypegenConfig,
  schemaRegistry: SchemaRegistryClient,
  apply: boolean
): Promise<readonly SyncOperation[]> {
  const operations: SyncOperation[] = [];

  for (const subject of buildSchemaRegistryPlan(catalog)) {
    const existing = await schemaRegistry.getLatestSubject(subject.subjectName);

    if (existing === undefined) {
      if (apply) {
        await schemaRegistry.registerSubject(subject);
      }

      operations.push({
        action: 'create',
        details: apply ? 'Subject created.' : 'Subject will be created.',
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

    operations.push({
      action: 'drift',
      details: `Existing subject schema differs for event '${subject.eventName}'.`,
      resourceName: subject.subjectName,
      target: 'registry'
    });
  }

  if (config.sync?.schemaRegistry?.failOnDrift === true && operations.some((operation) => operation.action === 'drift')) {
    throw new Error('Schema Registry sync detected subject drift and sync.schemaRegistry.failOnDrift is enabled.');
  }

  return operations;
}

function normalizeSchemaText(schemaText: string): string {
  try {
    const schema = JSON.parse(schemaText) as Schema;
    return stableStringify(Type.forSchema(schema).schema());
  } catch {
    return schemaText.trim();
  }
}
