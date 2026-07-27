# Steel thread 0006: Assisted evaluation proposal

- Status: demonstrated locally
- Timebox: one implementation and acceptance slice

## Hypothesis

Agent Lab can use a separate, attributable agent operation to turn durable turn
evidence into editable evaluation advice without changing the source session,
granting workspace mutation authority, or bypassing the validation and
promotion lifecycle demonstrated by steel thread 0005.

## Boundary

A `ProposalSession` is an operation-scoped external-driver session. It receives
redacted projections of stable source turns, uses the source harness and model
profile by default, and returns one versioned
`agent-lab/evaluation-proposal@1` candidate. It has an empty scratch workspace,
no MCP capability sources, and no workspace control grant.

The candidate may select only the supplied contiguous turn span. Its assertions
refer to the reviewed `catalog-to-file@1` evaluator and supported structured
parameters; model-generated assertion code is not executable. A successful
candidate creates the same editable draft resource as the manual path. It
cannot validate, save, or run the resulting evaluation.

Nushell and the browser project the same proposal identity, progress, result,
and draft. Only one nonterminal proposal may operate on an Explore workspace at
a time. Cancellation, failure, restart recovery, and final evidence remain
controller-owned.

## Evidence

The public conformance walkthrough used the checked-in fixture driver and the
catalog scenario. A completed interactive turn was supplied to a distinct
read-only proposer. The proposer emitted lifecycle progress and a strict
candidate containing the selected turn, standalone task, reviewed evaluator
parameters, measurements, and rationale. Agent Lab retained source-session and
proposal-session harness, model, adapter, prompt-contract, source-revision, and
event-sequence provenance.

The browser opened the resulting draft without replacing the terminal. The
builder edited the proposed task, created a new immutable revision, replayed
that revision to a complete passing validation, explicitly saved it as a
definition, and ran the definition through the configured v0 and Eve fixture
profiles. Both arms produced the expected active items, count 2, and total
score 11 from the same stored starting state.

The walkthrough also exposed a cross-surface gap: a definition run started in
one browser projection did not appear in another, making a duplicate diagnostic
run possible. The acceptance slice now publishes every
`workbench.evaluation.started` transition to all attached browser projections
and gives the initiating action an immediate disabled
**Starting comparison…** state.

After a locald service restart, the draft, proposal provenance, definition, and
both paired evaluations reopened from stored evidence. Reopening the original
paired result performed only evidence reads; it did not start a proposal,
definition run, or harness.

Contract coverage exercises strict candidate validation, source-span
confinement, scratch immutability, secret redaction, single-proposal ownership,
cancellation, terminal-event repair, partial-application recovery, Nushell
streaming and detach behavior, browser focus preservation, cross-surface
synchronization, and offline replay. The fixture proves the lifecycle and trust
boundary. Proposal quality with a real model remains a separate observation to
collect before making quality claims.

## Conclusion

AI assistance can remain attributable advice over the same durable resources
the builder already controls. A separate operation-scoped session preserves
the source conversation, keeps mutation authority narrow, and makes proposer
failure recoverable without weakening the manual path. Strict structured output
and explicit human validation keep proposal convenience separate from
evaluation truth.

The next experiment can use the completed interaction-to-evaluation loop to ask
whether settled semantic diagnostics improve a harness's TypeScript repair
behavior. Real-model proposer observations may refine the proposal contract,
but they do not block that richer capability experiment.
