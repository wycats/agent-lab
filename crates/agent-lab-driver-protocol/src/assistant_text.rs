//! Presentation helpers for assistant text that contains harness-authored thinking blocks.
//!
//! The wire protocol continues to retain assistant text byte-for-byte. These helpers are only
//! used at presentation and structured-response boundaries.

use std::borrow::Cow;

use pulldown_cmark::{Event, Options, Parser, Tag};

const THINKING_OPEN: &str = "<Thinking>";
const THINKING_CLOSE: &str = "</Thinking>";

fn markdown_options() -> Options {
    Options::ENABLE_TABLES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_GFM
}

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
struct ThinkingCandidate {
    start: usize,
    end: usize,
    tag: ThinkingTag,
    top_level: bool,
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
    let mut candidates = Vec::new();
    let mut blocks = Vec::new();
    for (event, range) in Parser::new_ext(source, markdown_options()).into_offset_iter() {
        match event {
            Event::Start(tag) => {
                if matches!(tag, Tag::HtmlBlock) && blocks.is_empty() {
                    collect_thinking_html_block(source, range.clone(), &mut candidates);
                }
                blocks.push(matches!(tag, Tag::Paragraph));
            }
            Event::End(_) => {
                blocks.pop();
            }
            Event::InlineHtml(html) => {
                let top_level = blocks.as_slice() == [true];
                if (html.as_ref() == THINKING_OPEN || html.as_ref() == THINKING_CLOSE)
                    && (top_level || !blocks.is_empty())
                {
                    candidates.push(ThinkingCandidate {
                        start: range.start,
                        end: range.end,
                        tag: if html.as_ref() == THINKING_OPEN {
                            ThinkingTag::Open
                        } else {
                            ThinkingTag::Close
                        },
                        top_level,
                    });
                }
            }
            _ => {}
        }
    }

    let mut tags = Vec::new();
    let mut opening = None;
    let mut nested_literal_opens = Vec::new();
    for ThinkingCandidate {
        start,
        end,
        tag,
        top_level,
    } in candidates
    {
        let current_line = line_start(source, start);
        match (opening, tag) {
            (None, ThinkingTag::Open) if top_level && is_thinking_block_start(source, start) => {
                tags.push((start, end, tag));
                opening = Some((current_line, line_indentation(source, start)));
            }
            (Some(_), ThinkingTag::Open) => {
                nested_literal_opens.push((start, end));
            }
            (Some((opened_line, opened_indentation)), ThinkingTag::Close) => {
                let block_aligned_outer_close = is_thinking_block_start(source, start)
                    && line_indentation(source, start) <= opened_indentation;
                let closes_nested_literal =
                    nested_literal_opens
                        .last()
                        .is_some_and(|(nested_start, _)| {
                            line_start(source, *nested_start) == current_line
                                || line_remainder_is_whitespace(source, end)
                        });
                if !block_aligned_outer_close && closes_nested_literal {
                    nested_literal_opens.pop();
                    continue;
                }
                if block_aligned_outer_close
                    || current_line == opened_line
                    || (!top_level && line_remainder_is_whitespace(source, end))
                {
                    tags.push((start, end, tag));
                    opening = None;
                    nested_literal_opens.clear();
                }
            }
            _ => {}
        }
    }
    tags
}

fn collect_thinking_html_block(
    source: &str,
    range: std::ops::Range<usize>,
    candidates: &mut Vec<ThinkingCandidate>,
) {
    let block = &source[range.clone()];
    let tag_offset = block.bytes().take_while(|byte| *byte == b' ').count();
    if tag_offset > 3 {
        return;
    }
    let tag_start = range.start + tag_offset;
    if block[tag_offset..].starts_with(THINKING_CLOSE)
        && line_remainder_is_whitespace(source, tag_start + THINKING_CLOSE.len())
    {
        candidates.push(ThinkingCandidate {
            start: tag_start,
            end: tag_start + THINKING_CLOSE.len(),
            tag: ThinkingTag::Close,
            top_level: true,
        });
        return;
    }
    if !block[tag_offset..].starts_with(THINKING_OPEN) {
        return;
    }
    let open_start = tag_start;
    if !is_thinking_block_start(source, open_start)
        || !line_remainder_is_whitespace(source, open_start + THINKING_OPEN.len())
    {
        return;
    }
    candidates.push(ThinkingCandidate {
        start: open_start,
        end: open_start + THINKING_OPEN.len(),
        tag: ThinkingTag::Open,
        top_level: true,
    });

    let body_start = source[open_start + THINKING_OPEN.len()..range.end]
        .find('\n')
        .map_or(range.end, |offset| {
            open_start + THINKING_OPEN.len() + offset + 1
        });
    if body_start < range.end {
        collect_thinking_body(&source[body_start..range.end], body_start, candidates);
    }
}

fn collect_thinking_body(
    body: &str,
    source_offset: usize,
    candidates: &mut Vec<ThinkingCandidate>,
) {
    let mut blocks = Vec::new();
    for (event, range) in Parser::new_ext(body, markdown_options()).into_offset_iter() {
        match event {
            Event::Start(tag) => {
                if matches!(tag, Tag::HtmlBlock) && blocks.is_empty() {
                    collect_leading_thinking_html_tag(
                        &body[range.clone()],
                        source_offset + range.start,
                        candidates,
                    );
                }
                blocks.push(matches!(tag, Tag::Paragraph));
            }
            Event::End(_) => {
                blocks.pop();
            }
            Event::InlineHtml(html) => {
                let top_level = blocks.as_slice() == [true];
                if (html.as_ref() == THINKING_OPEN || html.as_ref() == THINKING_CLOSE)
                    && (top_level || !blocks.is_empty())
                {
                    candidates.push(ThinkingCandidate {
                        start: source_offset + range.start,
                        end: source_offset + range.end,
                        tag: if html.as_ref() == THINKING_OPEN {
                            ThinkingTag::Open
                        } else {
                            ThinkingTag::Close
                        },
                        top_level,
                    });
                }
            }
            _ => {}
        }
    }
}

fn collect_leading_thinking_html_tag(
    block: &str,
    source_offset: usize,
    candidates: &mut Vec<ThinkingCandidate>,
) {
    let indentation = block.bytes().take_while(|byte| *byte == b' ').count();
    if indentation > 3 {
        return;
    }
    let (tag, width) = if block[indentation..].starts_with(THINKING_OPEN) {
        (ThinkingTag::Open, THINKING_OPEN.len())
    } else if block[indentation..].starts_with(THINKING_CLOSE) {
        (ThinkingTag::Close, THINKING_CLOSE.len())
    } else {
        return;
    };
    let line_end = block[indentation + width..]
        .find('\n')
        .map_or(block.len(), |offset| indentation + width + offset);
    if !block[indentation + width..line_end].trim().is_empty() {
        return;
    }
    let start = source_offset + indentation;
    candidates.push(ThinkingCandidate {
        start,
        end: start + width,
        tag,
        top_level: true,
    });
}

fn line_start(source: &str, at: usize) -> usize {
    source[..at].rfind('\n').map_or(0, |line| line + 1)
}

fn is_thinking_block_start(source: &str, at: usize) -> bool {
    let indentation = &source.as_bytes()[line_start(source, at)..at];
    indentation.len() <= 3 && indentation.iter().all(|byte| *byte == b' ')
}

fn line_indentation(source: &str, at: usize) -> usize {
    source.as_bytes()[line_start(source, at)..]
        .iter()
        .take_while(|byte| **byte == b' ')
        .count()
}

fn line_remainder_is_whitespace(source: &str, at: usize) -> bool {
    let end = source[at..]
        .find('\n')
        .map_or(source.len(), |offset| at + offset);
    source[at..end].trim_end_matches('\r').trim().is_empty()
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
    fn many_fenced_literal_tags_do_not_disable_a_later_thinking_block() {
        let literals = (1..=256)
            .map(|index| format!("<Thinking>literal-{index}</Thinking>"))
            .collect::<Vec<_>>()
            .join("\n");
        let source = format!("```md\n{literals}\n```\n<Thinking>real</Thinking>\nanswer");
        let fence_end = source.find("\n<Thinking>real").unwrap();

        assert_eq!(
            split_assistant_text(&source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: &source[..=fence_end],
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "real",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nanswer",
                    complete: true,
                },
            ]
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
    fn recognizes_thinking_open_tags_only_at_block_start() {
        let literal = "Explain <Thinking>literal</Thinking> syntax.";
        assert_eq!(
            split_assistant_text(literal),
            vec![AssistantTextPart {
                kind: AssistantTextPartKind::Answer,
                text: literal,
                complete: true,
            }]
        );

        assert_eq!(
            split_assistant_text("   <Thinking>working</Thinking>"),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "   ",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "working",
                    complete: true,
                },
            ]
        );

        let indented_code = "    <Thinking>literal</Thinking>";
        assert_eq!(
            split_assistant_text(indented_code),
            vec![AssistantTextPart {
                kind: AssistantTextPartKind::Answer,
                text: indented_code,
                complete: true,
            }]
        );
    }

    #[test]
    fn recognizes_thinking_close_tags_only_on_the_opening_or_a_block_line() {
        let source = "<Thinking>\n\
                      A literal </Thinking> marker remains in the thought.\n\
                      final evidence\n\
                      </Thinking>\n\
                      answer";
        assert_eq!(
            split_assistant_text(source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "\nA literal </Thinking> marker remains in the thought.\nfinal evidence\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nanswer",
                    complete: true,
                },
            ]
        );
    }

    #[test]
    fn keeps_multiline_thinking_and_markdown_container_tags_distinct() {
        let multiline = "<Thinking>\none line\n</Thinking>\nAnswer";
        assert_eq!(
            split_assistant_text(multiline),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "\none line\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nAnswer",
                    complete: true,
                },
            ]
        );

        let listed = "- item\n  <Thinking>literal</Thinking>\n\n<Thinking>visible</Thinking>";
        assert_eq!(
            split_assistant_text(listed),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "- item\n  <Thinking>literal</Thinking>\n\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "visible",
                    complete: true,
                },
            ]
        );
    }

    #[test]
    fn closes_multiline_thinking_after_a_markdown_list_without_a_blank_line() {
        for (source, thinking) in [
            (
                "<Thinking>Inspecting **alpha**.\n\n\
                 - checking alpha\n\
                 - checking score</Thinking>\n\
                 # Answer",
                "Inspecting **alpha**.\n\n- checking alpha\n- checking score",
            ),
            (
                "<Thinking>\n\
                 - checking alpha\n\
                 - checking score\n\
                 </Thinking>\n\
                 # Answer",
                "\n- checking alpha\n- checking score\n",
            ),
        ] {
            assert_eq!(
                split_assistant_text(source),
                vec![
                    AssistantTextPart {
                        kind: AssistantTextPartKind::Thinking,
                        text: thinking,
                        complete: true,
                    },
                    AssistantTextPart {
                        kind: AssistantTextPartKind::Answer,
                        text: "\n# Answer",
                        complete: true,
                    },
                ]
            );
        }
    }

    #[test]
    fn nested_literal_thinking_pair_does_not_close_the_outer_thought() {
        let source = "<Thinking>\n\
                      - literal <Thinking>nested</Thinking>\n\
                      - still thinking\n\
                      </Thinking>\n\
                      answer";
        assert_eq!(
            split_assistant_text(source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "\n- literal <Thinking>nested</Thinking>\n- still thinking\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nanswer",
                    complete: true,
                },
            ]
        );
        assert_eq!(answer_after_leading_thinking(source), "\nanswer");
    }

    #[test]
    fn matches_the_browser_gfm_dialect_around_thinking_blocks() {
        for source in [
            "[^1]: definition\n<Thinking>after footnote-like text</Thinking>\nAfter",
            "$$\nmath\n<Thinking>after dollar-delimited text</Thinking>\n$$\nAfter",
            "Term\n: definition\n<Thinking>after definition-like text</Thinking>\nAfter",
        ] {
            assert!(
                split_assistant_text(source)
                    .iter()
                    .any(|part| part.kind == AssistantTextPartKind::Thinking),
                "expected a thinking part in {source:?}"
            );
        }
    }

    #[test]
    fn comment_closes_stay_inert_across_consecutive_multiline_blocks() {
        let source = "<Thinking>\n\
                      Comment-safe thought remains open.\n\
                      <!--\n\
                      </Thinking>\n\
                      hidden close\n\
                      -->\n\
                      Still in the first thought.\n\
                      </Thinking>\n\
                      Between.\n\
                      <Thinking>\n\
                      Second thought.\n\
                      </Thinking>\n\
                      After.";
        let parts = split_assistant_text(source);
        assert_eq!(
            parts
                .iter()
                .filter(|part| part.kind == AssistantTextPartKind::Thinking)
                .map(|part| part.text)
                .collect::<Vec<_>>(),
            vec![
                "\nComment-safe thought remains open.\n<!--\n</Thinking>\nhidden close\n-->\nStill in the first thought.\n",
                "\nSecond thought.\n",
            ]
        );
        assert!(parts.iter().any(|part| {
            part.kind == AssistantTextPartKind::Answer && part.text.contains("Between.")
        }));
        assert!(parts.iter().any(|part| {
            part.kind == AssistantTextPartKind::Answer && part.text.contains("After.")
        }));
    }

    #[test]
    fn treats_unmatched_backtick_runs_as_literal_text() {
        let source = "Before ` literal.\n<Thinking>working carefully</Thinking>\nAfter.";
        assert_eq!(
            split_assistant_text(source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "Before ` literal.\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "working carefully",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nAfter.",
                    complete: true,
                },
            ]
        );

        let source = "<Thinking>working ` carefully</Thinking>\nDone.";
        assert_eq!(
            split_assistant_text(source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "working ` carefully",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nDone.",
                    complete: true,
                },
            ]
        );
    }

    #[test]
    fn unmatched_backticks_do_not_suppress_later_thinking() {
        let later_paragraph = "Before ` literal.\n\
                               <Thinking>real thought</Thinking>\n\
                               Later literal.";
        assert_eq!(
            split_assistant_text(later_paragraph),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "Before ` literal.\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "real thought",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nLater literal.",
                    complete: true,
                },
            ]
        );
    }

    #[test]
    fn unmatched_backticks_do_not_pair_into_fenced_blocks() {
        let later_fence = "Before ` literal.\n\
                           ```md\n\
                           Later ` literal.\n\
                           ```\n\
                           <Thinking>real thought</Thinking>";
        assert_eq!(
            split_assistant_text(later_fence),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "Before ` literal.\n```md\nLater ` literal.\n```\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "real thought",
                    complete: true,
                },
            ]
        );
    }

    #[test]
    fn keeps_balanced_multiline_code_spans_in_answer_text() {
        let source = "Use `code across\n<Thinking>literal</Thinking>\nlines`.\n\
                      <Thinking>real</Thinking>";
        assert_eq!(
            split_assistant_text(source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "Use `code across\n<Thinking>literal</Thinking>\nlines`.\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "real",
                    complete: true,
                },
            ]
        );
    }

    #[test]
    fn leaves_html_comment_tags_in_answer_text() {
        let inline = "before <!-- <Thinking>hidden</Thinking> --> after";
        assert_eq!(
            split_assistant_text(inline),
            vec![AssistantTextPart {
                kind: AssistantTextPartKind::Answer,
                text: inline,
                complete: true,
            }]
        );

        let source = "<!--\n<Thinking>hidden</Thinking>\n-->\n\
                      <Thinking>visible</Thinking>\nanswer";
        assert_eq!(
            split_assistant_text(source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "<!--\n<Thinking>hidden</Thinking>\n-->\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "visible",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "\nanswer",
                    complete: true,
                },
            ]
        );

        let unclosed = "<!-- <Thinking>hidden</Thinking>";
        assert_eq!(
            split_assistant_text(unclosed),
            vec![AssistantTextPart {
                kind: AssistantTextPartKind::Answer,
                text: unclosed,
                complete: true,
            }]
        );
    }

    #[test]
    fn leaves_raw_html_block_tags_outside_the_thinking_projection() {
        let source = "<script>\n\
                      <Thinking>hidden script text</Thinking>\n\
                      </script>\n\
                      \n\
                      <div>\n\
                      <Thinking>hidden div text</Thinking>\n\
                      </div>\n\
                      \n\
                      <Thinking>visible</Thinking>";
        assert_eq!(
            split_assistant_text(source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "<script>\n<Thinking>hidden script text</Thinking>\n</script>\n\n<div>\n<Thinking>hidden div text</Thinking>\n</div>\n\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "visible",
                    complete: true,
                },
            ]
        );

        let after_heading = "# heading\n<foo>\n<Thinking>hidden</Thinking>";
        assert_eq!(
            split_assistant_text(after_heading),
            vec![AssistantTextPart {
                kind: AssistantTextPartKind::Answer,
                text: after_heading,
                complete: true,
            }]
        );
    }

    #[test]
    fn rejects_backtick_fence_info_that_commonmark_treats_as_text() {
        let source = "```bad`\n\
                      \n\
                      <Thinking>visible</Thinking>";
        assert_eq!(
            split_assistant_text(source),
            vec![
                AssistantTextPart {
                    kind: AssistantTextPartKind::Answer,
                    text: "```bad`\n\n",
                    complete: true,
                },
                AssistantTextPart {
                    kind: AssistantTextPartKind::Thinking,
                    text: "visible",
                    complete: true,
                },
            ]
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
                "<Thinking>working ` carefully</Thinking>\n{\"task\":\"ok\"}"
            ),
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
