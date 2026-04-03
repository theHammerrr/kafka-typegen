import type { NormalizedKafkaTypegenConfig } from '../config/index.js';

import type {
  DesiredSchemaRegistrySubject,
  SchemaRegistryClient
} from './types.js';

export async function registerSubjectVersion(
  schemaRegistry: SchemaRegistryClient,
  subject: DesiredSchemaRegistrySubject
): Promise<void> {
  try {
    await schemaRegistry.registerSubject(subject);
  } catch (error) {
    throw new Error(
      `Failed to register Schema Registry subject '${subject.subjectName}' for event '${subject.eventName}': ${String(error)}`
    );
  }
}

export async function applySubjectCompatibility(
  schemaRegistry: SchemaRegistryClient,
  config: NormalizedKafkaTypegenConfig,
  subjectName: string
): Promise<void> {
  if (config.sync?.schemaRegistry?.compatibility === undefined) {
    return;
  }

  try {
    await schemaRegistry.updateSubjectCompatibility(
      subjectName,
      config.sync.schemaRegistry.compatibility
    );
  } catch (error) {
    throw new Error(
      `Failed to update Schema Registry compatibility for subject '${subjectName}': ${String(error)}`
    );
  }
}

export function buildCompatibilityDetails(
  config: NormalizedKafkaTypegenConfig,
  apply: boolean
): readonly string[] {
  if (config.sync?.schemaRegistry?.compatibility === undefined) {
    return [];
  }

  return [
    `Compatibility ${config.sync.schemaRegistry.compatibility} ${
      apply ? 'was' : 'will be'
    } applied.`
  ];
}
