# RFC 0005: Teaching evaluation concepts through use

- Status: Provisional
- Substrate evidence: the mechanical lifecycle demonstrated by
  [steel threads 0005](../steel-threads/0005-manual-evaluation-promotion.md)
  and [0006](../steel-threads/0006-assisted-evaluation-proposal.md)
- Product hypothesis: those rigorous resources do not yet teach their
  causal relationships well enough through use
- Acceptance evidence: not yet collected; see
  [Validation boundary](#validation-boundary)
- Timebox: one implementation and acceptance slice, capped at five working
  days; the uncoached walkthrough is capped at 45 minutes

## Summary

Agent Lab should help a harness builder develop an accurate intuition for
evaluations while they work. A useful agent turn should lead into one
continuous, inspectable story:

```text
understand the turn
  -> capture what matters
  -> replay it
  -> inspect the mismatch
  -> decide whether the behavior or contract is wrong
  -> retain or revise
  -> reproduce it
  -> save the evaluation
  -> vary the harness and learn from the comparison
```

The workbench currently has the rigorous resources needed for this loop:
source turns, immutable revisions, validation attempts, saved definitions,
concrete evaluation attempts, and replayable evidence. This RFC defines how
the browser and Nushell should project those resources so the builder can
understand their causal relationships without first learning Agent Lab's
controller vocabulary.

The primary projection is an **evaluation story** attached to the interaction
that motivated it. An evaluation story is not another persisted resource. It
is a view over the resources defined by RFC 0004 that explains:

- what happened;
- what state and behavior the builder chose to preserve;
- what a replay declared and verified;
- what the replay established;
- what changed between revisions; and
- what a later comparison varied.

An evaluation story is a controller-derived read model over explicit lineage
edges:

```text
source turn span
  -> proposal session, if any
  -> draft
  -> revisions
  -> validation attempts
  -> promoted definition
  -> evaluation attempts and their arms
```

It has no independent identity or lifecycle. The same projection is reachable
from the source and every descendant. Several drafts may share a source span;
validation attempts may fan out from a revision, and evaluation attempts may
fan out from a definition. Within one draft, revisions form a retained linear
chain: each edit is based on the current revision, and stale edits are rejected.

A **comparison** is the human projection of a paired evaluation attempt and its
arms, not another resource identity.

Formal identities, exact contracts, and raw evidence remain directly
accessible. Agent Lab should offer one progressively precise interface rather
than separate beginner and expert modes.

## Motivation

An evaluation system can be mechanically complete while remaining difficult to
reason about. A builder who sees tabs for drafts, revisions, validations,
definitions, and attempts may still need a manual to answer basic questions:

- Why was this workspace state captured?
- What is the difference between editing a draft and creating a revision?
- Did validation fail because the behavior was wrong or because the replay
  could not run?
- Which passing version will be saved?
- What does the definition fix, and what did each attempt actually verify?
- What claim does one successful replay actually support?

These are not introductory concerns that disappear with expertise. They are the
working intuitions an expert uses to design and interpret an experiment.

[Terence Tao describes mature mathematical
practice](https://terrytao.wordpress.com/career-advice/theres-more-to-mathematics-than-rigour-and-proofs/)
as a return to intuition supported by rigorous foundations rather than replaced
by them. The same relationship should hold here. The workbench should let a
builder reason with compact causal concepts while making the exact resource,
invariant, and evidence available whenever precision is needed.

[RFC 0001](0001-product-thesis.md) already says that the interface should teach
its programming model through use and that evaluations are the durable
continuation of exploration. This RFC tests the product hypothesis that the
implemented promotion loop needs a more specific interaction contract: correct
resource forms and status badges may not by themselves explain the loop.
Acceptance evidence for that hypothesis has not yet been collected.

Current implementation demonstrates one useful transition-level pattern:
proposal work begins beside the source turn, detailed lifecycle feedback remains
available, and terminal failure survives reload. That foothold does not yet
connect the resulting draft, replay, revision, saved definition, and comparison
into the complete causal story defined here.

## Experienced outcome

A harness builder encounters an interesting agent turn and can immediately
begin making it repeatable. Without command-by-command coaching, they can:

1. identify the turn, starting state, capabilities, and outcome being captured;
2. express the behavior that should remain true;
3. replay that behavior from the captured state;
4. distinguish an assertion mismatch from a replay that was inconclusive;
5. revise the evaluation while retaining the earlier result;
6. understand which exact version becomes a saved evaluation;
7. run another harness or model from the same definition; and
8. explain what the definition fixed, what the attempt varied and verified,
   and what its evidence supports.

An experienced builder can perform the same loop quickly, operate it from
Nushell, inspect exact revision and attempt identities, and descend into raw
events without changing to a different product mode.

## Design principles

### Begin with the builder's question

The workbench should organize the loop around questions the builder is trying
to answer:

1. **What happened?**
2. **What should remain true?**
3. **Can it be reproduced?**
4. **Why did this replay differ?**
5. **What exactly am I saving?**
6. **What am I varying now?**
7. **What did I learn?**

Agent Lab's resource names should appear when they make an answer more precise.
They should not be prerequisites for beginning the work.

### Maintain orientation at every state

Every evaluation surface should answer five questions:

1. **What am I looking at?**
2. **Where did it come from?**
3. **What has been established so far?**
4. **What changed since that evidence was produced?**
5. **What useful action can I take next?**

Resource ontology is not navigation ontology. The controller may need separate
draft, revision, attempt, definition, and evidence resources to preserve a
rigorous lifecycle. The builder should still experience their common lineage
and current meaning.

### Teach at the transition

Concepts become useful when an action changes the state of the evaluation.
Explanations, progress, and outcomes should therefore appear beside the action
and resource they describe.

The current **Suggest evaluation** action demonstrates the first part of this
pattern: the initiating browser shows progress beside the source turn, broader
session activity remains visible, and terminal failure survives reload. The
source-local marker itself remains page-local, and completion replaces the
source context with the draft. This RFC preserves that relationship across
origins and reloads and extends it across the rest of the evaluation story:

- the resulting draft says that its starting state was captured before
  that turn;
- editing a validated draft should explain that it creates a new revision and
  makes the previous validation historical;
- starting validation should say which revision and starting state will run;
- saving should identify the exact passing revision that will become reusable;
  and
- starting a comparison should state which dimensions the definition fixes,
  which variation was selected, and which facts the attempts later verify.

A global activity surface may retain operational detail, but it should not be
the only place to understand the consequence of a local action.

### Keep intuition and rigor adjacent

Each important state should have three mutually consistent projections:

1. **Meaning.** A concise account of what happened and why it matters.
2. **Contract.** The controlled inputs, assertions, limits, variants, and
   lifecycle state.
3. **Evidence.** Exact identities, revisions, event sequences, raw values, and
   retained artifacts.

These are levels of precision over the same resource, not separate modes with
different truth. A builder may move between them at any time.

### Let failure teach the model

Failed and incomplete work is part of the evaluation story. The workbench
should distinguish at least:

- **Behavior did not satisfy the assertions.** The replay completed and
  produced interpretable evidence.
- **Replay was inconclusive.** Agent Lab could not restore, execute, evaluate,
  or finalize the declared experiment.
- **Replay was cancelled or intervened.** The attempt stopped or no longer
  represents controlled agent-only behavior.
- **Current revision is unvalidated.** Earlier attempts belong to an older
  immutable revision.

A completed assertion failure should present expected and observed values,
relevant workspace effects, and the revision it exercised. It should support
three explicit continuations: inspect the evidence, retain the failing revision
as a regression draft, or revise the task, assertion, or evaluator after the
builder determines that the contract was wrong. The interface must not imply
that matching the observed output is the default correction.

The primary projection should translate rigorous state into experienced
meaning:

| Rigorous state | Experienced meaning |
| --- | --- |
| Incomplete draft | Review the task and checks before replaying them. |
| Local edits pending | These edits are not yet part of a replayable version. |
| Ready, unvalidated revision | This exact version can be replayed from the captured state. |
| Validation queued or running | Agent Lab is replaying this exact version with the named harness and model. |
| Complete with failed assertions | The replay ran successfully; these expectations did not match. |
| Inconclusive and not evaluated | Agent Lab could not establish the behavioral result because execution or evidence was incomplete. |
| Cancelled or intervened | Partial evidence was retained, but this is not a clean validation. |
| Complete and passed | This exact version reproduced successfully once. |
| Saved without a passing replay | The work is retained as an editable draft, not a runnable evaluation. |
| Promoted definition | This exact passing version is saved and runnable. |
| Paired evaluation attempt | The projection distinguishes declared inputs, selected variation, and facts verified for these attempts. |

### Preserve expert composition

Guidance must not turn the workbench into a fixed wizard. A builder may revisit
an earlier stage, keep multiple drafts, inspect a failed attempt before editing,
or launch a comparison from Nushell.

The browser should make the causal path visible without requiring it to be
linear. Nushell should preserve ordinary structured values and pipelines.
Direct display may render an explanatory projection; piping or binding the
value must retain the full structured record.

## Evaluation story

### 1. An observed turn supplies the origin

The story begins on a completed agent turn. The turn already contains the
prompt, structured input, pre-turn workspace state, capability revisions,
harness and model provenance, response, activity, effects, and evidence.

The turn should offer one contextual continuation such as **Make this
repeatable**. Assisted and manual authoring remain available within that
continuation:

- **Suggest an evaluation** starts the attributable proposal session; and
- **Create manually** starts from the selected evidence without a proposer.

The interface should keep the source turn visible and explain that the future
evaluation begins from the state before the selected turn, not from the
workspace after the agent acted.

### 2. A draft expresses what should remain true

The first draft view should relate source evidence to a standalone future task.
It should answer:

- Which turn or span motivated this evaluation?
- What did the builder ask, what did the agent answer, and what changed?
- What starting workspace and capabilities were captured?
- What task will a fresh agent receive?
- Which outcomes are hard assertions?
- Which observations are measurements rather than pass conditions?
- Which fields came from source evidence, proposal advice, or builder edits?

The workbench may prefill fields, but it should show their origin and invite
review. Proposal output is advice, not an established expectation.

The immutable revision model should be explained through behavior. Draft
creation may first persist a source-derived seed revision and, for assisted
authoring, a second proposal-applied revision before the builder acts. The
story presents these as attributed draft initialization, not as builder
confirmation or a meaningful user edit; the precise projection retains both
revision identities and contents. Builder review chooses whether to validate
the current revision or edit it. Each later material edit creates another
revision and retains the earlier one with its attempts. The formal term
**revision** and its identity remain visible in the precise projection.

### 3. Validation replays one exact version

Before launch, validation should summarize:

- the revision being exercised;
- the captured starting state;
- the selected harness and model;
- driver and adapter identities available in the current assembly;
- any execution-host identity retained by controller-owned evidence, otherwise
  **not reported**;
- the capability recipe and limits; and
- the assertions that will determine the result.

The completed attempt should then distinguish requested configuration from the
concrete identities it exercised, any stack or capability drift, unavailable
adapter acknowledgement, and which fixed dimensions Agent Lab verified rather
than merely requested. Execution-host identity is shown only when retained by
the attempt's evidence; otherwise it remains **not reported**.

While it runs, progress belongs in the draft story as well as global activity.
The UI should connect restoration, harness startup, agent activity, workspace
effects, evaluator execution, and evidence finalization to one attempt.

At completion, the primary result should explain the behavioral outcome.
Execution integrity and assertion outcome remain separately inspectable.

### 4. Revision turns a mismatch into learning

When a replay produces a mismatch, the builder should see the observed evidence
beside the relevant task or assertion and decide whether it exposes a behavior
regression or a mistake in the evaluation contract. A behavior regression can
remain as a failing draft. If the contract was wrong, the builder can revise
the relevant field. The UI should make clear that editing creates a new
revision and that the failed attempt remains valid evidence about the old
revision.

The history should read as a causal sequence rather than a list of opaque
identifiers:

```text
Version 1
  Complete replay: total score expected 10, observed 11
  Inspection: the fixture and evaluator contract establish that 11 is expected

Version 2
  Misconfigured total-score expectation corrected from 10 to 11
  Not replayed yet
```

Exact revision identifiers and full diffs remain available.
Validation attempts should be grouped by revision. A failed attempt against an
older revision remains visible as historical evidence without appearing to
contradict a passing current revision.

### 5. Saving names the reusable contract

Saving should answer **what will be reusable?** A passing current revision may
be saved as an evaluation definition. The interface should identify that exact
revision, its controlled starting state, task, capabilities, assertions,
limits, and provenance.

For a passing current revision, the primary action should use language such as
**Save as evaluation**. A revision that is not promotable may instead offer
**Keep draft**. If one controller operation supports both outcomes, the
interface must preview which one will occur before the builder invokes it.

The formal definition identity and local-library location belong in the
precise projection. If the builder later edits the draft, the previously saved
definition remains visibly bound to its passing revision; new draft work does
not silently change it.

### 6. Comparison explains the experiment

Before running a saved evaluation through another harness or model, the
workbench should show:

- **Fixed by the definition:** revision, snapshot digest, task, declared
  capability recipe and source revisions, assertions, evaluator version, and
  limits.
- **Selected variation:** harness, model, capability projection, or another
  named dimension that the controller will retain in the attempt.
- **Verified for this attempt:** the concrete harness, model, driver, adapter,
  and capability identities each arm actually exercised; execution-host
  identity when retained, otherwise **not reported**; any drift or unavailable
  acknowledgement; and which fixed dimensions Agent Lab verified rather than
  merely requested.
- **Measured:** correctness gates, activity, effects, latency, usage, context,
  and any scenario-specific observations.

The result should lead with the observed difference between these attempts,
then offer aligned activity and raw evidence. Call the comparison controlled
only for dimensions Agent Lab verified; one paired attempt does not establish
that the selected variant caused the difference. It should not declare a
universal winner or imply a repeated quality claim.

## Workbench projection

### Browser

The browser should keep a compact causal rail visible while the builder works:

```text
Source -> Draft -> Replay -> Saved evaluation -> Comparison
```

The rail is a projection over existing identities. It shows the current
position, meaningful prior outcomes, and available continuations. It also
allows non-linear navigation to source turns, earlier revisions, attempts,
definitions, and comparisons.

Resource-specific tabs may remain useful for detailed inspection, but tab
placement should not be the only explanation of how the resources relate.
The source-turn proposal marker is the first local anchor; the complete
evaluation story should keep progress, errors, and outcomes near their
initiating controls. Selecting an item should preserve the relationship to its
source rather than replacing the whole context with an unrelated page.

Drafts, saved definitions, comparisons, and their arm runs should also be
grouped by lineage in history. Storage identity remains inspectable, but one
conceptual evaluation should not appear as several unrelated history items.

### Nushell

RFC 0004's command surface remains the structured control model. This RFC
changes its human projection, not its resource identities.

Direct display of proposal, draft, validation, definition, and comparison
results should answer the same causal questions as the browser. Help and
completion should include short examples that demonstrate the next meaningful
operation. Piped and bound values remain ordinary structured records suitable
for inspection and transformation.

For example, a direct validation result may explain that the replay completed
but one assertion differed. The same value in a pipeline still exposes the
attempt status, assertion results, revision identity, evidence references, and
workspace effects as fields.

The existing proposal path demonstrates controller-driven completion
synchronization between browser tabs and from Nushell into the browser. The
evaluation story should extend that behavior across revisions, validations,
definitions, and attempts. Neither surface should require a reload, steal
terminal focus, or maintain an independent interpretation of the current
revision.

## Evidence integrity

The intuitive projection must be derived from controller-owned resources. It
must not invent a causal explanation that the retained evidence cannot support.
Every meaningful statement should resolve to the source turn, revision,
attempt, assertion result, workspace effect, or comparison evidence behind it.

Missing or incomplete observations remain visible as **not reported**,
**incomplete**, or another precise state rather than being filled in by
presentation logic. Model-generated proposal rationale remains attributed
advice and is not presented as the reason an evaluation actually passed or
failed.

A completed evaluation story must be reproducible from stored evidence after
restart without live harnesses or capabilities. Offline reopening should
produce the same meaning and contract projections and retain access to the same
exact identities and raw evidence.

## Implementation direction

The first implementation should be a presentation slice over the existing
RFC 0004 resources:

1. group the existing **Suggest evaluation** and **Create manually** actions
   under a contextual **Make this repeatable** continuation;
2. generalize the demonstrated source-turn-local proposal feedback across
   origins and reloads, and preserve a visible causal link to the source turn
   when the draft opens;
3. add the causal rail and question-oriented draft summary;
4. project validation as replay progress followed by expected-versus-observed
   results;
5. describe revision history as meaningful changes and retained outcomes;
6. rename or contextualize save and comparison actions around their
   consequences; and
7. give direct Nushell display the same account while preserving structured
   pipeline values.

This slice should avoid changing the controller's identity model unless direct
use reveals information that the current resources cannot supply.

The catalog scenario remains the first acceptance case because its passing and
seeded-mismatch validations already exercise the complete promotion lifecycle.
The code-server diagnostics experiment should then become the first richer
workspace problem taught through this interaction model.

## Validation boundary

The first evidence boundary is an uncoached catalog walkthrough. The
participant understands agent harnesses but receives only this goal:

> Turn something the agent did into a repeatable evaluation, then use it to
> compare two harnesses.

The recorded acceptance setup gives either authoring path a reviewed catalog
evaluator with one stale total-score expectation. The interface identifies the
resulting expected-versus-observed mismatch after replay, but neither the
participant instruction nor a facilitator identifies which field is stale or
how to resolve it.

The walkthrough ends when the participant completes the final explanation,
explicitly stops, requests or receives task-specific coaching, or reaches 45
minutes. In the latter three cases, record the terminal state, intervention if
any, and observed gap as a non-accepting observation rather than extending the
attempt. If the presentation slice cannot reach this boundary within five
working days, preserve the blockers and revise the proposal instead of
broadening the slice.

The participant should be able to:

1. choose a consequential completed turn and explain what state the evaluation
   will begin from;
2. use assisted or manual authoring and identify which expectations require
   their judgment;
3. validate the starting revision, producing one complete assertion failure,
   and distinguish it from an inconclusive replay;
4. diagnose the seeded stale evaluator parameter, correct it, and explain why
   the earlier validation remains evidence about the previous revision;
5. obtain one passing replay and save the exact current revision as an
   evaluation;
6. run the saved evaluation through two configured harness profiles;
7. identify what the definition fixed, what the comparison varied and
   verified, and one evidence-supported difference between the attempts;
8. reach exact identities, evaluator versions, limits, raw events, and
   structured Nushell records through a deliberate disclosure or command
   without losing the causal account; and
9. reopen the same story after restart without live harnesses.

Acceptance is not only task completion. Without help from the implementer, the
participant should be able to explain in their own words:

- the difference between a draft, a revision, a validation attempt, a saved
  evaluation, and a paired evaluation attempt;
- why a completed failed replay is useful evidence;
- why one passing replay is reproduction rather than a general quality claim;
  and
- which facts make a cross-harness comparison controlled.

Deterministic browser and controller tests should establish projection,
synchronization, failure taxonomy, and offline reopening.

One recorded uncoached walkthrough supplies initial qualitative evidence that
this interaction can support the intended mental model for that participant; it
does not establish broad learnability. Preserve the participant instruction,
starting UI state, errors or hesitations, any interventions, completion
outcome, and final explanation as the steel-thread evidence.

The public walkthrough uses only checked-in neutral conformance fixtures,
public-safe synthetic drivers, and non-sensitive workspace and model inputs.
Its steel-thread artifact contains only a participant-approved, sanitized
summary of the instruction, fixture and source revision, starting UI state,
elapsed time and terminal state, observed decision points, hesitations, errors,
interventions, outcome, and final explanation. Paraphrase participant language
by default; include quotations or screenshots only with explicit consent.

If a walkthrough uses a private workspace, model, driver, adapter, or product
system, the entire observation—including any derived summary—remains in its
owning repository and does not support claims in this public repository. Raw
recordings or transcripts, participant identity, machine-local paths, and
credentials also remain outside this repository. Retain or delete raw study
material according to the participant agreement. If sanitization removes
evidence needed for a public claim, mark that claim unsupported.

## Drawbacks

Maintaining an explanatory projection creates another coherence obligation.
The meaning, contract, and evidence views must derive from the same
controller-owned resources and be tested against lifecycle changes.

Contextual explanation can become visual clutter. The design should show the
explanation needed for the current transition, retain concise state after it is
understood, and keep detailed evidence available on demand.

Question-oriented language may obscure useful formal vocabulary if it replaces
the terms entirely. This RFC instead introduces formal names beside concrete
consequences and preserves them in structured output.

An uncoached walkthrough is slower and less mechanically reproducible than an
automated acceptance test. It is nevertheless necessary evidence for a product
whose claim is that builders can develop a working model through interaction.

## Alternatives considered

### Add documentation and tooltips

Documentation remains useful for reference, and concise contextual help belongs
in the interface. A separate manual cannot establish the relationship between
an action and the live state it changes. Tooltips also make the explanation
easy to miss and difficult to relate across the full loop.

### Build a linear evaluation wizard

A wizard could explain every step in order, but it would make exploration and
revision feel exceptional and would constrain experts who already know where
they want to go. The causal rail provides orientation without imposing one
path.

### Add simple and advanced modes

Separate modes tend to create separate vocabularies and make the simplified
surface a dead end. Progressive precision lets every builder move between
meaning, contract, and evidence over the same resource.

### Let an AI assistant explain the interface

Proposal assistance can help formulate an evaluation, but the product's state
and invariants should remain understandable without another stochastic agent.
An assistant may build on a coherent interaction model; it should not be the
only source of that coherence.

### Continue presenting controller resources directly

Direct resource views are valuable for inspection. Making them the primary
navigation asks the builder to reconstruct causality from backend ontology,
which is the gap this RFC addresses.

## Boundaries

This RFC defines the presentation and interaction model for the existing
promotion lifecycle. It does not:

- change the controller-owned resource identities in RFC 0004;
- add a general evaluator or assertion language;
- define repeated trials or statistical quality claims;
- export evaluations to repositories;
- add code-server, a browser host, or other workspace capabilities;
- make proposal agents authoritative; or
- replace harness-native evaluation systems.

## Open questions

- Which formal terms should remain persistently visible after their
  consequences are familiar, and which can move into the precise projection?
- Which surfaces should show the full lineage, and which should show a compact
  backlink into the same projection?
- How should Nushell direct display suggest meaningful continuations without
  adding presentation-only fields to composable records?
- What compact representation best explains capability and workspace state
  when a promoted evaluation spans richer scenarios?
- Which observations demonstrate that a builder has formed a useful mental
  model rather than merely completed the expected clicks?

## Relationship to other RFCs

[RFC 0001](0001-product-thesis.md) defines the interactive learning loop and
requires the interface to teach its programming model through use. This RFC
specifies that requirement for evaluation promotion and comparison.

[RFC 0002](0002-steel-thread-method.md) governs the evidence boundary and the
revision of this proposal after direct use.

[RFC 0003](0003-first-evidence-backed-architecture.md) records the controller,
driver, capability, surface, and evidence boundaries from which this projection
is derived.

[RFC 0004](0004-interactive-agent-sessions-and-evaluation-promotion.md) defines
the session, turn, proposal, draft, revision, validation, definition, and
attempt lifecycle. This RFC defines how one workbench teaches and operates that
lifecycle without creating a parallel resource model.
