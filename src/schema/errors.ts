export class SchemaLoadError extends Error {
  public readonly filePath: string;
  public override readonly cause?: unknown;

  public constructor(filePath: string, message: string, options?: { cause?: unknown }) {
    super(message);

    this.name = 'SchemaLoadError';
    this.filePath = filePath;
    this.cause = options?.cause;
  }
}

export class SchemaParseError extends Error {
  public readonly filePath: string;
  public override readonly cause?: unknown;

  public constructor(filePath: string, message: string, options?: { cause?: unknown }) {
    super(message);

    this.name = 'SchemaParseError';
    this.filePath = filePath;
    this.cause = options?.cause;
  }
}
