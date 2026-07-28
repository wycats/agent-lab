export type AssistantTextPart = {
  kind: 'answer' | 'thinking';
  text: string;
  complete: boolean;
};

type MarkdownFence = {
  marker: '`' | '~';
  width: number;
};

const THINKING_OPEN = '<Thinking>';
const THINKING_CLOSE = '</Thinking>';

export function splitAssistantText(source: string): AssistantTextPart[] {
  const tags = thinkingTags(source);
  if (tags.length === 0) {
    return [{ kind: 'answer', text: source, complete: true }];
  }

  const parts: AssistantTextPart[] = [];
  let cursor = 0;
  let thinkingStart: number | undefined;
  for (const tag of tags) {
    if (thinkingStart === undefined && tag.kind === 'open') {
      if (tag.start > cursor) {
        parts.push({
          kind: 'answer',
          text: source.slice(cursor, tag.start),
          complete: true
        });
      }
      thinkingStart = tag.end;
      cursor = tag.end;
    } else if (thinkingStart !== undefined && tag.kind === 'close') {
      parts.push({
        kind: 'thinking',
        text: source.slice(thinkingStart, tag.start),
        complete: true
      });
      thinkingStart = undefined;
      cursor = tag.end;
    }
  }

  if (thinkingStart !== undefined) {
    parts.push({
      kind: 'thinking',
      text: source.slice(thinkingStart),
      complete: false
    });
  } else if (cursor < source.length) {
    parts.push({
      kind: 'answer',
      text: source.slice(cursor),
      complete: true
    });
  }
  return parts;
}

function thinkingTags(source: string) {
  const tags: Array<{
    start: number;
    end: number;
    kind: 'open' | 'close';
  }> = [];
  let fence: MarkdownFence | undefined;
  let inlineCodeWidth: number | undefined;
  let lineStart = 0;
  while (lineStart < source.length) {
    const newline = source.indexOf('\n', lineStart);
    const lineEnd = newline === -1 ? source.length : newline + 1;
    const line = source.slice(lineStart, lineEnd);
    if (fence) {
      if (closesFence(line, fence)) fence = undefined;
    } else if (inlineCodeWidth === undefined) {
      const opened = opensFence(line);
      if (opened) {
        fence = opened;
      } else {
        inlineCodeWidth = findTagsInLine(line, lineStart, inlineCodeWidth, tags);
      }
    } else {
      inlineCodeWidth = findTagsInLine(line, lineStart, inlineCodeWidth, tags);
    }
    lineStart = lineEnd;
  }
  return tags;
}

function findTagsInLine(
  line: string,
  lineStart: number,
  inlineCodeWidth: number | undefined,
  tags: Array<{ start: number; end: number; kind: 'open' | 'close' }>
): number | undefined {
  let cursor = 0;
  while (cursor < line.length) {
    if (line[cursor] === '`' && !isEscaped(line, cursor)) {
      const runStart = cursor;
      while (line[cursor] === '`') cursor += 1;
      const width = cursor - runStart;
      if (inlineCodeWidth === undefined) inlineCodeWidth = width;
      else if (inlineCodeWidth === width) inlineCodeWidth = undefined;
      continue;
    }
    if (inlineCodeWidth === undefined && !isEscaped(line, cursor)) {
      const isOpen = line.startsWith(THINKING_OPEN, cursor);
      const isClose = line.startsWith(THINKING_CLOSE, cursor);
      if (isOpen || isClose) {
        const width = isOpen ? THINKING_OPEN.length : THINKING_CLOSE.length;
        tags.push({
          start: lineStart + cursor,
          end: lineStart + cursor + width,
          kind: isOpen ? 'open' : 'close'
        });
        cursor += width;
        continue;
      }
    }
    cursor += 1;
  }
  return inlineCodeWidth;
}

function isEscaped(source: string, at: number): boolean {
  let backslashes = 0;
  for (let cursor = at; cursor > 0 && source[cursor - 1] === '\\'; cursor -= 1) {
    backslashes += 1;
  }
  return backslashes % 2 === 1;
}

function opensFence(line: string): MarkdownFence | undefined {
  const marker = fenceMarker(line);
  return marker ? { marker: marker.marker, width: marker.width } : undefined;
}

function closesFence(line: string, fence: MarkdownFence): boolean {
  const marker = fenceMarker(line);
  return Boolean(
    marker &&
      marker.marker === fence.marker &&
      marker.width >= fence.width &&
      marker.remainder.trim() === ''
  );
}

function fenceMarker(line: string):
  | { marker: '`' | '~'; width: number; remainder: string }
  | undefined {
  const content = line.replace(/[\r\n]+$/, '');
  const match = /^( {0,3})(`{3,}|~{3,})(.*)$/.exec(content);
  if (!match) return undefined;
  const run = match[2];
  return {
    marker: run[0] as '`' | '~',
    width: run.length,
    remainder: match[3]
  };
}
