import { readFile } from 'node:fs/promises';
import { buildTopicPropertyName } from '../../src/catalog/naming.js';

// Renders runnable TS app fixtures with per-test broker/group/topic values.
// The source files stay as .ts.template so placeholder tokens are not compiled by repo-level TypeScript.
export type IntegrationAppFixtureName =
  | 'conflicting-options.ts'
  | 'decode-error.ts'
  | 'handler-error.ts'
  | 'happy-path.ts'
  | 'invalid-produce.ts'
  | 'secure-happy-path.ts'
  | 'secure-typecheck-app.ts'
  | 'standalone-factories.ts'
  | 'typecheck-app.ts';

export interface IntegrationAppTemplateValues {
  readonly kafkaBroker: string;
  readonly kafkaPassword?: string;
  readonly kafkaUsername?: string;
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
    .replaceAll('{{KAFKA_PASSWORD}}', values.kafkaPassword ?? '')
    .replaceAll('{{KAFKA_USERNAME}}', values.kafkaUsername ?? '')
    .replaceAll('{{USER_EVENTS_PROPERTY}}', buildTopicPropertyName(`${values.runId}.user.events`))
    .replaceAll('{{USER_PROFILE_PROPERTY}}', buildTopicPropertyName(`${values.runId}.user.profile`))
    .replaceAll('{{RUN_ID}}', values.runId);
}
