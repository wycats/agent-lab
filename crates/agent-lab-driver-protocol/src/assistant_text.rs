//! Presentation helpers for assistant text that contains harness-authored thinking blocks.
//!
//! The wire protocol continues to retain assistant text byte-for-byte. These helpers are only
//! used at presentation and structured-response boundaries.

use std::borrow::Cow;

const THINKING_OPEN: &str = "<Thinking>";
const THINKING_CLOSE: &str = "</Thinking>";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssistantTextPartKind {
    Answer,
    Thinking,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssistantTextPart<'a> {
    pub kind: AssistantTextPartKind,
    pub text: &'a str,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy)]
enum ThinkingTag {
    Open,
    Close,
}

#[derive(Debug, Clone, Copy)]
struct MarkdownFence {
    marker: u8,
    width: usize,
}

#[must_use]
pub fn split_assistant_text(source: &str) -> Vec<AssistantTextPart<'_>> {
    let tags = thinking_tags(source);
    if tags.is_empty() {
        return vec![AssistantTextPart {
            kind: AssistantTextPartKind::Answer,
            text: source,
            complete: true,
        }];
    }

    let mut parts = Vec::new();
    let mut cursor = 0;
    let mut thinking_start = None;
    for (start, end, tag) in tags {
        match (thinking_start, tag) {
            (None, ThinkingTag::Open) => {
                if start > cursor {
                    parts.push(AssistantTextPart {
                        kind: AssistantTextPartKind::Answer,
                        text: &source[cursor..start],
                        complete: true,
                    });
                }
                thinking_start = Some(end);
                cursor = end;
            }
            (Some(content_start), ThinkingTag::Close) => {
                parts.push(AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: &source[content_start..start],
                    complete: true,
                });
                thinking_start = None;
                cursor = end;
            }
            _ => {}
        }
    }

    if let Some(content_start) = thinking_start {
        parts.push(AssistantTextPart {
            kind: AssistantTextPartKind::Thinking,
            text: &source[content_start..],
            complete: false,
        });
    } else if cursor < source.len() {
        parts.push(AssistantTextPart {
            kind: AssistantTextPartKind::Answer,
            text: &source[cursor..],
            complete: true,
        });
    }
    parts
}

/// Returns answer text without leading harness-authored thinking blocks.
///
/// A response that begins with ordinary answer text is returned unchanged. That protects literal
/// `<Thinking>` strings inside structured output while allowing proposal JSON to follow the same
/// leading thinking convention as interactive answers.
#[must_use]
pub fn answer_after_leading_thinking(source: &str) -> Cow<'_, str> {
    let tags = thinking_tags(source);
    let mut tag_index = 0;
    let mut cursor = 0;
    let mut removed = false;

    loop {
        let Some(non_whitespace) = source[cursor..]
            .find(|character: char| !character.is_whitespace())
            .map(|offset| cursor + offset)
        else {
            return if removed {
                Cow::Borrowed(&source[cursor..])
            } else {
                Cow::Borrowed(source)
            };
        };
        while tag_index < tags.len() && tags[tag_index].0 < non_whitespace {
            tag_index += 1;
        }
        if !matches!(
            tags.get(tag_index),
            Some((start, _, ThinkingTag::Open)) if *start == non_whitespace
        ) {
            return if removed {
                Cow::Borrowed(&source[cursor..])
            } else {
                Cow::Borrowed(source)
            };
        }

        let Some((close_index, (_, close_end, _))) = tags
            .iter()
            .enumerate()
            .skip(tag_index + 1)
            .find(|(_, (_, _, tag))| matches!(tag, ThinkingTag::Close))
        else {
            return Cow::Borrowed(&source[source.len()..]);
        };
        removed = true;
        cursor = *close_end;
        tag_index = close_index + 1;
    }
}

fn thinking_tags(source: &str) -> Vec<(usize, usize, ThinkingTag)> {
    let mut tags = Vec::new();
    let mut fence = None;
    let mut inline_code_width = None;
    let mut line_start = 0;
    while line_start < source.len() {
        let line_end = source[line_start..]
            .find('\n')
            .map_or(source.len(), |offset| line_start + offset + 1);
        let line = &source[line_start..line_end];
        if let Some(active) = fence {
            if closes_fence(line, active) {
                fence = None;
            }
        } else if inline_code_width.is_none()
            && let Some(opened) = opens_fence(line)
        {
            fence = Some(opened);
        } else {
            find_tags_in_line(line, line_start, &mut inline_code_width, &mut tags);
        }
        line_start = line_end;
    }
    tags
}

fn find_tags_in_line(
    line: &str,
    line_start: usize,
    inline_code_width: &mut Option<usize>,
    tags: &mut Vec<(usize, usize, ThinkingTag)>,
) {
    let bytes = line.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] == b'`' && !is_escaped(bytes, cursor) {
            let run_start = cursor;
            while bytes.get(cursor) == Some(&b'`') {
                cursor += 1;
            }
            let width = cursor - run_start;
            match *inline_code_width {
                None => *inline_code_width = Some(width),
                Some(active) if active == width => *inline_code_width = None,
                Some(_) => {}
            }
            continue;
        }
        if inline_code_width.is_none() && !is_escaped(bytes, cursor) {
            let (tag, width) = if bytes[cursor..].starts_with(THINKING_OPEN.as_bytes()) {
                (Some(ThinkingTag::Open), THINKING_OPEN.len())
            } else if bytes[cursor..].starts_with(THINKING_CLOSE.as_bytes()) {
                (Some(ThinkingTag::Close), THINKING_CLOSE.len())
            } else {
                (None, 0)
            };
            if let Some(tag) = tag {
                tags.push((line_start + cursor, line_start + cursor + width, tag));
                cursor += width;
                continue;
            }
        }
        cursor += 1;
    }
}

fn is_escaped(bytes: &[u8], at: usize) -> bool {
    let mut backslashes = 0;
    let mut cursor = at;
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        backslashes += 1;
        cursor -= 1;
    }
    backslashes % 2 == 1
}

fn opens_fence(line: &str) -> Option<MarkdownFence> {
    let marker = fence_marker(line)?;
    Some(MarkdownFence {
        marker: marker.0,
        width: marker.1,
    })
}

fn closes_fence(line: &str, fence: MarkdownFence) -> bool {
    let Some((marker, width, remainder)) = fence_marker(line) else {
        return false;
    };
    marker == fence.marker && width >= fence.width && remainder.trim().is_empty()
}

fn fence_marker(line: &str) -> Option<(u8, usize, &str)> {
    let line = line.trim_end_matches(['\r', '\n']);
    let bytes = line.as_bytes();
    let mut offset = 0;
    while offset < bytes.len() && offset < 4 && bytes[offset] == b' ' {
        offset += 1;
    }
    if offset > 3 {
        return None;
    }
    let marker = *bytes.get(offset)?;
    if marker != b'`' && marker != b'~' {
        return None;
    }
    let mut end = offset;
    while bytes.get(end) == Some(&marker) {
        end += 1;
    }
    let width = end - offset;
    (width >= 3).then_some((marker, width, &line[end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separates_multiple_complete_thinking_blocks_from_the_answer() {
        let parts = split_assistant_text(
            "<Thinking>Inspecting **alpha**.</Thinking>\nFirst.\n\
             <Thinking>Checking gamma.</Thinking>\nSecond.",
        );

        assert_eq!(
            parts,
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "Inspecting **alpha**.",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nFirst.\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "Checking gamma.",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nSecond.",
                    complete: true,
                },
            ]
        );
    }

    #[test]
    fn leaves_fenced_and_unmatched_tags_in_answer_text() {
        let source = "```md\n<Thinking>literal</Thinking>\n```\n</Thinking>\nanswer";
        assert_eq!(
            split_assistant_text(source),
            vec![AssistantTextPart {
                kind: AssistantTextPartKind::Answer,
                text: source,
                complete: true,
            }]
        );
    }

    #[test]
    fn leaves_inline_code_tags_in_answer_text() {
        let source = "Use `<Thinking>literal</Thinking>` in output.\n\
                      Use ``<Thinking>`literal`</Thinking>`` too.\n\
                      Use \\<Thinking>escaped\\</Thinking> too.";
        assert_eq!(
            split_assistant_text(source),
            vec![AssistantTextPart {
                kind: AssistantTextPartKind::Answer,
                text: source,
                complete: true,
            }]
        );
    }

    #[test]
    fn exposes_an_unclosed_streaming_thinking_block() {
        assert_eq!(
            split_assistant_text("before\n<Thinking>still working"),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "before\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "still working",
                    complete: false,
                },
            ]
        );
    }

    #[test]
    fn strips_thinking_only_when_it_leads_the_response() {
        assert_eq!(
            answer_after_leading_thinking("<Thinking>working</Thinking>\n{\"task\":\"ok\"}"),
            "\n{\"task\":\"ok\"}"
        );
        assert_eq!(
            answer_after_leading_thinking(
                "<Thinking>working</Thinking>\n\
                 {\"task\":\"say <Thinking>x</Thinking> literally\"}"
            ),
            "\n{\"task\":\"say <Thinking>x</Thinking> literally\"}"
        );
        let structured = "{\"task\":\"say <Thinking> literally\"}";
        assert!(matches!(
            answer_after_leading_thinking(structured),
            Cow::Borrowed(value) if value == structured
        ));
    }
}
