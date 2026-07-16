# RFC 0002: Steel-thread method

- Status: Provisional

## Summary

Agent Lab develops through small end-to-end experiments that produce
architectural evidence. Each steel thread crosses the real layers needed to
answer one question while keeping unrelated ambitions outside its boundary.

## Required shape

Every steel thread states:

1. The hypothesis being tested.
2. The real implementation or protocol path it exercises.
3. The timebox or explicit stopping condition.
4. Observable acceptance evidence.
5. Non-goals and prohibited scope expansion.
6. The architectural conclusion supported by the outcome.

A failed thread is useful when it preserves enough evidence to distinguish an
intrinsic constraint from an incomplete implementation.

## Evidence

Evidence should be inspectable and proportionate to the claim. Depending on the
thread, it may include:

- exact commands and versions;
- event or tool transcripts;
- structured result fixtures;
- final workspace state;
- execution limits and permission decisions;
- paired trial output;
- cache and context accounting;
- failure modes and recovery behavior.

Deterministic fixtures establish precise contracts. Real integrations establish
that those contracts survive contact with an actual system. Important claims
usually need both.

## RFC relationship

RFCs may frame a steel thread before implementation, but implementation
evidence owns the revision. After a thread concludes, review the affected RFCs
and either tighten their contract, record the remaining gap, or withdraw the
unsupported direction.

## Initial sequence

1. Embedded Nushell plus modern MCP behavior. Completed by the first public
   steel thread.
2. A browser feedback surface attached to the same real PTY boundary. Completed
   by the visual-shell and browser-workbench steel threads.
3. A neutral external agent-driver boundary with durable run and evidence
   contracts.
4. A first real harness bound to a controlled workspace and capability source
   through the workbench.
5. A second real harness exercised through the same workbench. The initial pair
   is v0 and Eve, without forcing them to share one tool loop or native event
   model.
6. Real capability sources and editor-semantic tools.
7. Paired end-to-end evaluation of one agent-facing change.

Each item should land through reviewable pull requests rather than one framework
construction pass.
