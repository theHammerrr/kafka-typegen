interface AvroField {
  readonly type?: unknown;
}

interface AvroEnumSchema {
  readonly symbols?: unknown;
  readonly type?: unknown;
}

function isEnumSchema(schema: unknown): schema is AvroEnumSchema {
  return typeof schema === 'object' && schema !== null && (schema as { type?: unknown }).type === 'enum';
}

function collectEnumSymbolsByPath(
  avroType: unknown,
  path: string,
  symbolsByPath: Map<string, readonly string[]>
): void {
  if (Array.isArray(avroType)) {
    avroType.forEach((memberType, index) => {
      collectEnumSymbolsByPath(memberType, `${path}[${index}]`, symbolsByPath);
    });
    return;
  }

  if (typeof avroType !== 'object' || avroType === null) {
    return;
  }

  if (isEnumSchema(avroType) && Array.isArray(avroType.symbols)) {
    symbolsByPath.set(path, avroType.symbols.map((symbol) => String(symbol)));
  }

  const typeRecord = avroType as Record<string, unknown>;
  collectEnumSymbolsByPath(typeRecord.type, `${path}.type`, symbolsByPath);
  collectEnumSymbolsByPath(typeRecord.items, `${path}.items`, symbolsByPath);
  collectEnumSymbolsByPath(typeRecord.values, `${path}.values`, symbolsByPath);

  if (Array.isArray(typeRecord.fields)) {
    typeRecord.fields.forEach((field, index) => {
      const fieldRecord = field as AvroField;
      collectEnumSymbolsByPath(fieldRecord.type, `${path}.fields[${index}]`, symbolsByPath);
    });
  }
}

export function analyzeEnumChanges(
  previousSchema: unknown,
  nextSchema: unknown
): readonly string[] {
  const hints: string[] = [];
  const previousEnums = new Map<string, readonly string[]>();
  const nextEnums = new Map<string, readonly string[]>();

  collectEnumSymbolsByPath(previousSchema, 'schema', previousEnums);
  collectEnumSymbolsByPath(nextSchema, 'schema', nextEnums);

  for (const [path, previousSymbols] of previousEnums) {
    const nextSymbols = nextEnums.get(path);
    if (nextSymbols === undefined) {
      continue;
    }

    const removedSymbols = previousSymbols.filter((symbol) => !nextSymbols.includes(symbol));
    if (removedSymbols.length > 0) {
      hints.push(
        `Enum at '${path}' removed symbol(s): ${removedSymbols.join(', ')}. Removing enum symbols is often incompatible.`
      );
    }
  }

  return hints;
}
