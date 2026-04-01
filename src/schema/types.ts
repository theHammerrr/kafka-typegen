export interface SchemaSource {
  readonly filePath: string;
}

export interface SchemaLoadResult {
  readonly source: SchemaSource;
  readonly rawSchema: string;
}

export interface SchemaLoader {
  load(source: SchemaSource): Promise<SchemaLoadResult>;
}

export interface ParsedSchemaField {
  readonly name: string;
  readonly type: string;
}

export interface ParsedSchema {
  readonly fields: readonly ParsedSchemaField[];
  readonly filePath: string;
  readonly name: string;
  readonly namespace?: string;
  readonly rawSchema: unknown;
}

export interface SchemaParser {
  parse(result: SchemaLoadResult): Promise<ParsedSchema>;
}
