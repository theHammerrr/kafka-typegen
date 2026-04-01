import type { SubjectNameStrategy } from './types.js';

export function deriveSubjectName(
  topicName: string,
  eventName: string,
  strategy: SubjectNameStrategy,
  explicitSubject?: string
): string {
  if (explicitSubject !== undefined) {
    return explicitSubject;
  }

  switch (strategy) {
    case 'event-name':
      return eventName;
    case 'topic-name':
      return topicName;
    case 'topic-event':
      return `${topicName}-${eventName}`;
  }
}
