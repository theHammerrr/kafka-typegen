import { indent } from './render-utils.js';

export function emitClientTypes(): string {
  return [
    'export interface GeneratedClient {',
    indent(['producer: GeneratedProducer;', 'consumer: GeneratedConsumer;'].join('\n')),
    '}'
  ].join('\n');
}

export function emitClientFactory(): string {
  return [
    'export function createClient(runtime: RuntimeClient): GeneratedClient {',
    indent(
      [
        'return {',
        indent(['producer: createProducer(runtime.producer),', 'consumer: createConsumer(runtime.consumer)'].join('\n')),
        '};'
      ].join('\n')
    ),
    '}'
  ].join('\n');
}
