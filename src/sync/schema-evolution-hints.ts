import { analyzeSchemaEvolution } from './schema-evolution-analyzer.js';

function dedupeHints(hints: readonly string[]): readonly string[] {
  return [...new Set(hints)].sort((left, right) => left.localeCompare(right));
}

export function buildSchemaEvolutionHints(
  previousSchemaText: string,
  nextSchemaText: string
): readonly string[] {
  return dedupeHints(analyzeSchemaEvolution(previousSchemaText, nextSchemaText));
}

export function formatSchemaEvolutionHints(hints: readonly string[]): readonly string[] {
  if (hints.length === 0) {
    return [];
  }

  return [`Evolution hints: ${hints.join(' ')}`];
}
