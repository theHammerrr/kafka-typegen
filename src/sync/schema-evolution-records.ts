interface AvroField {
  readonly default?: unknown;
  readonly name?: unknown;
  readonly type?: unknown;
}

interface AvroRecordSchema {
  readonly fields?: unknown;
}

function isNullableType(avroType: unknown): boolean {
  return Array.isArray(avroType) && avroType.includes('null');
}

function isRequiredField(field: AvroField): boolean {
  return field.default === undefined && !isNullableType(field.type);
}

function typeKind(avroType: unknown): string {
  if (typeof avroType === 'string') {
    return avroType;
  }

  if (Array.isArray(avroType)) {
    return `union(${avroType.map((member) => typeKind(member)).join('|')})`;
  }

  if (typeof avroType === 'object' && avroType !== null) {
    const typeValue = (avroType as { type?: unknown }).type;
    return typeof typeValue === 'string' ? typeValue : JSON.stringify(typeValue);
  }

  return JSON.stringify(avroType);
}

function compareFieldTypeChanges(
  previousField: AvroField,
  nextField: AvroField,
  fieldName: string
): readonly string[] {
  const hints: string[] = [];

  if (!isRequiredField(previousField) && isRequiredField(nextField)) {
    hints.push(
      `Field '${fieldName}' changed from optional to required. Consider a staged rollout where producers populate it before tightening the schema.`
    );
  }

  if (typeKind(previousField.type) !== typeKind(nextField.type)) {
    hints.push(
      `Field '${fieldName}' changed type from '${typeKind(previousField.type)}' to '${typeKind(nextField.type)}'. This is often incompatible for existing data.`
    );
  }

  return hints;
}

export function analyzeRecordFields(
  previousRecord: AvroRecordSchema,
  nextRecord: AvroRecordSchema
): readonly string[] {
  const hints: string[] = [];
  const previousFields = new Map<string, AvroField>();
  const nextFields = new Map<string, AvroField>();

  for (const field of Array.isArray(previousRecord.fields) ? previousRecord.fields : []) {
    const fieldRecord = field as AvroField;
    if (typeof fieldRecord.name === 'string') {
      previousFields.set(fieldRecord.name, fieldRecord);
    }
  }

  for (const field of Array.isArray(nextRecord.fields) ? nextRecord.fields : []) {
    const fieldRecord = field as AvroField;
    if (typeof fieldRecord.name === 'string') {
      nextFields.set(fieldRecord.name, fieldRecord);
    }
  }

  for (const [fieldName, nextField] of nextFields) {
    const previousField = previousFields.get(fieldName);
    if (previousField === undefined) {
      if (isRequiredField(nextField)) {
        hints.push(
          `Field '${fieldName}' was added without a default. Consider making it nullable or adding a default value first.`
        );
      }
      continue;
    }

    hints.push(...compareFieldTypeChanges(previousField, nextField, fieldName));
  }

  for (const [fieldName, previousField] of previousFields) {
    if (!nextFields.has(fieldName) && isRequiredField(previousField)) {
      hints.push(
        `Required field '${fieldName}' was removed. Consider first making it optional/nullable before removing it.`
      );
    }
  }

  return hints;
}
