import type { SASLOptions } from 'kafkajs';

import type { KafkaTypegenSyncSaslConfig } from '../config/index.js';

export function toKafkaJsSasl(config: KafkaTypegenSyncSaslConfig): SASLOptions {
  switch (config.mechanism) {
    case 'plain':
      return { mechanism: 'plain', password: config.password, username: config.username };
    case 'scram-sha-256':
      return { mechanism: 'scram-sha-256', password: config.password, username: config.username };
    case 'scram-sha-512':
      return { mechanism: 'scram-sha-512', password: config.password, username: config.username };
  }
}
