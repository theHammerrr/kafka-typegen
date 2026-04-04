import { readFile } from 'node:fs/promises';

// Renders runnable TS app fixtures with per-test broker/group/topic values.
// The source files stay as .ts.template so placeholder tokens are not compiled by repo-level TypeScript.
export type IntegrationAppFixtureName =
  | 'conflicting-options.ts'
  | 'decode-error.ts'
  | 'handler-error.ts'
  | 'happy-path.ts'
  | 'invalid-produce.ts'
  | 'typecheck-app.ts';

export interface IntegrationAppTemplateValues {
  readonly kafkaBroker: string;
  readonly runId: string;
}

export async function renderIntegrationAppFixture(
  fixtureName: IntegrationAppFixtureName,
  values: IntegrationAppTemplateValues
): Promise<string> {
  const templateText = await readFile(
    new URL(`./app-fixtures/${fixtureName}.template`, import.meta.url),
    'utf8'
  );

  return templateText
    .replaceAll('{{KAFKA_BROKER}}', values.kafkaBroker)
    .replaceAll('{{RUN_ID}}', values.runId);
}
