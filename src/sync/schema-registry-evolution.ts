import {
  buildSchemaEvolutionHints,
  formatSchemaEvolutionHints
} from './schema-evolution-hints.js';

export function buildSchemaEvolutionDetails(
  previousSchemaText: string,
  nextSchemaText: string
): readonly string[] {
  return formatSchemaEvolutionHints(
    buildSchemaEvolutionHints(previousSchemaText, nextSchemaText)
  );
}

export function buildSchemaEvolutionFailure(
  error: unknown,
  previousSchemaText: string,
  nextSchemaText: string
): Error {
  return new Error(
    [String(error), ...buildSchemaEvolutionDetails(previousSchemaText, nextSchemaText)]
      .join(' ')
      .trim()
  );
}
