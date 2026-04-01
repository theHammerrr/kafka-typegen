import type { ConfigValidationIssue } from './types.js';

export function formatIssuePath(path: readonly PropertyKey[]): string {
  if (path.length === 0) {
    return 'config';
  }

  return path.reduce<string>((currentPath, segment) => {
    if (typeof segment === 'number') {
      return `${currentPath}[${segment}]`;
    }

    const segmentText = String(segment);

    return currentPath.length === 0 ? segmentText : `${currentPath}.${segmentText}`;
  }, '');
}

export function buildValidationIssue(path: readonly PropertyKey[], message: string): ConfigValidationIssue {
  return {
    message,
    path: formatIssuePath(path)
  };
}
