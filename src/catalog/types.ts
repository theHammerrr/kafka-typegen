import type { NormalizedKafkaTypegenConfig } from '../config/index.js';
import type { ParsedSchema } from '../schema/index.js';

export interface CatalogEvent {
  readonly eventName: string;
  readonly schema: ParsedSchema | null;
  readonly topicName: string;
  readonly typeName: string;
}

export interface EventCatalog {
  readonly config: NormalizedKafkaTypegenConfig;
  readonly events: readonly CatalogEvent[];
}

export interface CatalogBuilder {
  build(config: NormalizedKafkaTypegenConfig): Promise<EventCatalog>;
}
