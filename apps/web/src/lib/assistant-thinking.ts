import type { MarkedExtension, Token } from '@humanspeak/svelte-markdown';
import { Lexer } from 'marked';

const THINKING_OPEN = '<Thinking>';
const THINKING_CLOSE = '</Thinking>';
const MARKER_PREFIX = '\u{e000}agent-lab-thinking';
const MAX_THINKING_TAGS = 512;

type ThinkingToken = Token & {
  type: 'thinking';
  raw: string;
  text: string;
  tokens: Token[];
  complete: boolean;
};

type PlainToken = {
  type: string;
  raw?: string;
  tokens?: PlainToken[];
  items?: PlainToken[];
};

type ThinkingTag = {
  kind: 'open' | 'close';
  start: number;
  end: number;
  topLevel: boolean;
};

type ThinkingRange = {
  open: ThinkingTag;
  close?: ThinkingTag;
};

export type AssistantThinkingProjector = {
  extension: MarkedExtension;
  project(source: string): string;
};

export function createAssistantThinkingProjector(): AssistantThinkingProjector {
  let markers = {
    open: `${MARKER_PREFIX}-unused-open`,
    close: `${MARKER_PREFIX}-unused-close`
  };
  return {
    extension: thinkingExtension(() => markers),
    project(source) {
      markers = uniqueMarkers(source);
      const tagCount = thinkingTagCount(source);
      if (tagCount === 0) return source;
      if (tagCount > MAX_THINKING_TAGS) {
        return source
          .replaceAll(THINKING_OPEN, '\\<Thinking>')
          .replaceAll(THINKING_CLOSE, '\\</Thinking>');
      }
      const parsedTags = parsedThinkingTags(source);
      const ranges = thinkingRanges(source, parsedTags);
      const selected = new Set(
        ranges.flatMap((range) => [
          range.open.start,
          ...(range.close ? [range.close.start] : [])
        ])
      );
      const literalTags = parsedTags.filter((tag) => !selected.has(tag.start));
      if (ranges.length === 0 && literalTags.length === 0) return source;

      const replacements = [
        ...ranges.flatMap((range) => [
          {
            start: range.open.start,
            end: range.open.end,
            value: `\n\n${markers.open}\n\n`
          },
          ...(range.close
            ? [{
                start: range.close.start,
                end: range.close.end,
                value: `\n\n${markers.close}\n\n`
              }]
            : [])
        ]),
        ...literalTags.map((tag) => ({
          start: tag.start,
          end: tag.end,
          value: tag.kind === 'open' ? '\\<Thinking>' : '\\</Thinking>'
        }))
      ];
      const projected = [];
      let cursor = 0;
      for (const replacement of replacements.sort((left, right) => left.start - right.start)) {
        projected.push(source.slice(cursor, replacement.start), replacement.value);
        cursor = replacement.end;
      }
      projected.push(source.slice(cursor));
      return projected.join('');
    }
  };
}

function thinkingTagCount(source: string): number {
  let count = 0;
  const tags = /<\/?Thinking>/g;
  while (count <= MAX_THINKING_TAGS && tags.exec(source)) {
    count += 1;
  }
  return count;
}

function thinkingRanges(
  source: string,
  parsedTags: ThinkingTag[]
): ThinkingRange[] {
  const tags = parsedTags;
  const ranges: ThinkingRange[] = [];
  let tagIndex = 0;
  while (tagIndex < tags.length) {
    while (
      tagIndex < tags.length &&
      (tags[tagIndex].kind !== 'open' ||
        !tags[tagIndex].topLevel ||
        !isBlockTag(source, tags[tagIndex].start))
    ) {
      tagIndex += 1;
    }
    if (tagIndex === tags.length) break;
    const open = tags[tagIndex];
    tagIndex += 1;

    const openingLine = lineStart(source, open.start);
    const openingIndentation = lineIndentation(source, open.start);
    const nestedLiteralOpens: ThinkingTag[] = [];
    let close: ThinkingTag | undefined;
    while (tagIndex < tags.length) {
      const candidate = tags[tagIndex];
      tagIndex += 1;
      if (candidate.kind === 'open') {
        nestedLiteralOpens.push(candidate);
        continue;
      }
      const blockAlignedOuterClose =
        isBlockTag(source, candidate.start) &&
        lineIndentation(source, candidate.start) <= openingIndentation;
      const nestedLiteralOpen = nestedLiteralOpens.at(-1);
      const closesNestedLiteral =
        nestedLiteralOpen !== undefined &&
        (lineStart(source, nestedLiteralOpen.start) ===
          lineStart(source, candidate.start) ||
          lineRemainderIsWhitespace(source, candidate.end));
      if (!blockAlignedOuterClose && closesNestedLiteral) {
        nestedLiteralOpens.pop();
        continue;
      }
      if (
        blockAlignedOuterClose ||
        lineStart(source, candidate.start) === openingLine ||
        (!candidate.topLevel && lineRemainderIsWhitespace(source, candidate.end))
      ) {
        close = candidate;
        break;
      }
    }
    ranges.push({ open, close });
    if (!close) break;
  }
  return ranges;
}

function parsedThinkingTags(source: string): ThinkingTag[] {
  const tokens = Lexer.lex(source, { breaks: false, gfm: true }) as PlainToken[];
  const candidates: ThinkingTag[] = [];
  collectPlainTokens(source, 0, tokens, true, true, candidates);
  return candidates.sort((left, right) => left.start - right.start);
}

function collectPlainTokens(
  source: string,
  sourceOffset: number,
  tokens: PlainToken[],
  topLevel: boolean,
  expandThinkingBlock: boolean,
  candidates: ThinkingTag[]
): void {
  let cursor = 0;
  for (const token of tokens) {
    const raw = token.raw ?? '';
    const tokenStart = source.indexOf(raw, cursor);
    if (tokenStart === -1) continue;
    const absoluteStart = sourceOffset + tokenStart;

    if (token.type === 'html') {
      const exact = exactThinkingTag(raw, absoluteStart, topLevel);
      if (exact) {
        candidates.push(exact);
      } else if (topLevel && expandThinkingBlock) {
        collectThinkingHtmlBlock(raw, absoluteStart, candidates);
      } else {
        collectLeadingThinkingHtmlTag(
          raw,
          absoluteStart,
          topLevel,
          candidates
        );
      }
    }
    if (token.tokens) {
      collectPlainTokens(
        raw,
        absoluteStart,
        token.tokens,
        topLevel && (token.type === 'paragraph' || token.type === 'text'),
        expandThinkingBlock,
        candidates
      );
    }
    if (token.items) {
      collectPlainTokens(
        raw,
        absoluteStart,
        token.items,
        false,
        expandThinkingBlock,
        candidates
      );
    }
    cursor = tokenStart + raw.length;
  }
}

function collectThinkingHtmlBlock(
  source: string,
  sourceOffset: number,
  candidates: ThinkingTag[]
): void {
  const match = /^( {0,3})(<\/?Thinking>)[ \t]*\r?(?:\n|$)/.exec(source);
  if (!match) return;
  const leading = exactThinkingTag(
    match[2],
    sourceOffset + match[1].length,
    true
  );
  if (leading) candidates.push(leading);
  if (leading?.kind === 'close') return;

  const bodyStart = match[0].length;
  const body = source.slice(bodyStart);
  const bodyTokens = Lexer.lex(body, { breaks: false, gfm: true }) as PlainToken[];
  collectPlainTokens(
    body,
    sourceOffset + bodyStart,
    bodyTokens,
    true,
    false,
    candidates
  );
}

function collectLeadingThinkingHtmlTag(
  source: string,
  sourceOffset: number,
  topLevel: boolean,
  candidates: ThinkingTag[]
): void {
  const match = /^( {0,3})(<\/?Thinking>)[ \t]*\r?(?:\n|$)/.exec(source);
  if (!match) return;
  if (!topLevel && match[2] !== THINKING_CLOSE) return;
  const tag = exactThinkingTag(
    match[2],
    sourceOffset + match[1].length,
    topLevel
  );
  if (tag) candidates.push(tag);
}

function exactThinkingTag(
  source: string,
  start: number,
  topLevel: boolean
): ThinkingTag | undefined {
  if (source === THINKING_OPEN) {
    return { kind: 'open', start, end: start + THINKING_OPEN.length, topLevel };
  }
  if (source === THINKING_CLOSE) {
    return { kind: 'close', start, end: start + THINKING_CLOSE.length, topLevel };
  }
  return undefined;
}

function isBlockTag(source: string, at: number): boolean {
  const start = lineStart(source, at);
  const prefix = source.slice(start, at);
  return prefix.length <= 3 && [...prefix].every((character) => character === ' ');
}

function lineStart(source: string, at: number): number {
  return source.lastIndexOf('\n', Math.max(0, at - 1)) + 1;
}

function lineIndentation(source: string, at: number): number {
  const start = lineStart(source, at);
  let indentation = 0;
  while (source[start + indentation] === ' ') indentation += 1;
  return indentation;
}

function lineRemainderIsWhitespace(source: string, at: number): boolean {
  const end = source.indexOf('\n', at);
  return source.slice(at, end === -1 ? source.length : end).trim().length === 0;
}

function uniqueMarkers(source: string): { open: string; close: string } {
  const occupied = new Set<string>();
  const pattern = new RegExp(`${MARKER_PREFIX}(?:-(\\d+))?-(?:open|close)`, 'g');
  for (const match of source.matchAll(pattern)) {
    occupied.add(match[1] ?? '0');
  }
  let index = 0;
  while (occupied.has(String(index))) index += 1;
  const suffix = index === 0 ? '' : `-${index}`;
  return {
    open: `${MARKER_PREFIX}${suffix}-open`,
    close: `${MARKER_PREFIX}${suffix}-close`
  };
}

function thinkingExtension(
  activeMarkers: () => { open: string; close: string }
): MarkedExtension {
  return {
    extensions: [
      {
        name: 'thinking',
        level: 'block',
        childTokens: ['tokens'],
        tokenizer(source, tokens): ThinkingToken | undefined {
          const { open, close } = activeMarkers();
          if (tokens !== this.lexer.tokens || !source.startsWith(open)) {
            return undefined;
          }

          const contentStart = open.length;
          const closingStart = source.indexOf(close, contentStart);
          const contentEnd = closingStart === -1 ? source.length : closingStart;
          let rawEnd =
            closingStart === -1 ? source.length : closingStart + close.length;
          while (source[rawEnd] === '\r' || source[rawEnd] === '\n') rawEnd += 1;
          const text = source.slice(contentStart, contentEnd);

          return {
            type: 'thinking',
            raw: source.slice(0, rawEnd),
            text,
            tokens: this.lexer.blockTokens(text, []),
            complete: closingStart !== -1
          };
        }
      }
    ]
  };
}
