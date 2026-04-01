export class CatalogValidationError extends Error {
  public readonly issues: readonly string[];

  public constructor(issues: readonly string[]) {
    super(`Invalid event catalog:\n${issues.map((issue) => `- ${issue}`).join('\n')}`);

    this.name = 'CatalogValidationError';
    this.issues = issues;
  }
}
