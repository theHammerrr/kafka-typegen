import avsc from 'avsc';
import type { Schema } from 'avsc';

import { stableStringify } from './stable-stringify.js';

const { Type } = avsc;

export function normalizeSchemaText(schemaText: string): string {
  try {
    const schema = JSON.parse(schemaText) as Schema;
    return stableStringify(Type.forSchema(schema).schema());
  } catch {
    return schemaText.trim();
  }
}
