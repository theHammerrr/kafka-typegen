const CONFLUENT_MAGIC_BYTE = 0;
const CONFLUENT_WIRE_PREFIX_LENGTH = 5;

export interface DecodedSchemaRegistryPayload {
  readonly payload: Uint8Array;
  readonly schemaId: number;
}

export function encodeSchemaRegistryWireFormat(schemaId: number, payload: Uint8Array): Uint8Array {
  if (!Number.isInteger(schemaId) || schemaId < 0) {
    throw new Error(`Schema Registry schema ids must be non-negative integers. Received '${schemaId}'.`);
  }

  const encodedPayload = new Uint8Array(CONFLUENT_WIRE_PREFIX_LENGTH + payload.byteLength);
  const view = new DataView(encodedPayload.buffer);
  view.setUint8(0, CONFLUENT_MAGIC_BYTE);
  view.setUint32(1, schemaId, false);
  encodedPayload.set(payload, CONFLUENT_WIRE_PREFIX_LENGTH);
  return encodedPayload;
}

export function decodeSchemaRegistryWireFormat(payload: Uint8Array): DecodedSchemaRegistryPayload {
  if (payload.byteLength < CONFLUENT_WIRE_PREFIX_LENGTH) {
    throw new Error('Schema Registry payloads must include the Confluent wire-format prefix.');
  }

  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const magicByte = view.getUint8(0);
  if (magicByte !== CONFLUENT_MAGIC_BYTE) {
    throw new Error(`Unsupported Schema Registry magic byte '${magicByte}'.`);
  }

  return {
    payload: payload.subarray(CONFLUENT_WIRE_PREFIX_LENGTH),
    schemaId: view.getUint32(1, false)
  };
}
