import { indent } from './render-utils.js';

export function emitClientTypes(): string {
  return [
    'export type GeneratedClient<TRuntimeClient extends RuntimeClient = RuntimeClient> = Omit<TRuntimeClient, \'producer\' | \'consumer\'> & {',
    indent(
      [
        'producer: GeneratedProducer<TRuntimeClient[\'producer\']>;',
        'consumer: GeneratedConsumer<TRuntimeClient[\'consumer\']>;'
      ].join('\n')
    ),
    '};'
  ].join('\n');
}

export function emitClientFactory(): string {
  return [
    'export function createClient<TRuntimeClient extends RuntimeClient>(runtime: TRuntimeClient): GeneratedClient<TRuntimeClient> {',
    indent(
      [
        'return Object.assign(Object.create(runtime), {',
        indent(
          [
            'producer: createProducer(runtime.producer),',
            'consumer: createConsumer(runtime.consumer)'
          ].join('\n')
        ),
        '}) as GeneratedClient<TRuntimeClient>;'
      ].join('\n')
    ),
    '}'
  ].join('\n');
}
