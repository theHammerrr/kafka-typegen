import { buildSchemaEvolutionDetails } from './schema-registry-evolution.js';

export function buildRegistryDriftDetails(
  eventName: string,
  previousSchemaText: string,
  nextSchemaText: string,
  ignored: boolean
): string {
  return [
    ignored
      ? `Existing subject schema differs for event '${eventName}' and will be left unchanged.`
      : `Existing subject schema differs for event '${eventName}'.`,
    ...buildSchemaEvolutionDetails(previousSchemaText, nextSchemaText)
  ].join(' ');
}
