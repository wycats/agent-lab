# RFC 0004: Interactive agent sessions and evaluation promotion

- Status: Provisional
- Evidence: [steel thread 0005](../steel-threads/0005-manual-evaluation-promotion.md)

## Summary

Agent Lab should let a harness builder ask a real agent to work in the active
environment, understand its answer, actions, and effects, and preserve an
interesting interaction as a durable evaluation. The product loop should feel
continuous:

```text
explore -> ask agent -> understand behavior -> follow up
        -> compare this turn -> make evaluation -> validate -> save -> rerun
```

The opening interaction is intentionally direct:

```nu
agent "Find the active catalog items and explain what matters"
agent "Why did you prioritize gamma?"
```

The first command lazily opens the selected harness's native session and lets
the harness discover the capabilities available in the workspace. The shell
presents the assistant's useful response as the primary result. The browser
opens the same Session and explains the response, capability calls, native
actions, and workspace effects without requiring the builder to understand the
controller's identity model first. The second command continues the same
harness-native conversation.

Structured pipeline input remains a distinguishing precision affordance. A
builder who has already inspected a value may supply that exact value as
additional turn context:

```nu
catalog list | where active | agent "Use exactly these items and compare them"
```

It is not a prerequisite for the agent to discover or invoke capabilities.
Agent Lab records each turn against its workspace revision and the capability
projection the controller supplied.

When an interaction reveals behavior worth preserving, the builder can now
create an editable evaluation draft directly from one or more stable turns.
The draft relates a standalone task, controlled starting state, capabilities,
assertions, and measurements back to the source turns and evidence. The
builder reviews it, replays it from its captured starting state, and explicitly
saves a passing revision. A later evaluation may exercise the same definition
with different harnesses or models.

An attributable `ProposalSession` remains the next assistance layer. It should
help choose a meaningful turn span and draft the editable resource while
preserving the manual path and the identities demonstrated here.

## Implementation status

This milestone implements the interactive-session and manual-promotion
substrate:

- workspace-scoped, multi-turn harness-native sessions and explicit session
  lifecycle commands;
- structured pipeline input and durable pre-turn workspace evidence;
- attributable assistant, capability, native-action, usage, and workspace
  projections;
- answer-first `agent-answer` records, terminal Markdown, streaming and raw
  modes, and durable turn reopening; and
- a synchronized browser Session view with live progress and
  **Rendered**/**Source** presentation;
- editable draft revisions derived from stable turn spans and revision-owned
  pre-turn snapshots;
- reviewed `catalog-to-file@1` validation with distinct execution and
  assertion outcomes;
- retained failed validations, explicit promotion of an exactly validated
  revision, and a local definition library; and
- definition reruns through the existing paired v0/Eve evaluation machinery,
  with durable browser and Nushell reopening after restart.

AI-assisted proposal drafting through a separate `ProposalSession` remains
proposed. General assertion languages, repository export, and repeated
statistical claims also remain outside the demonstrated boundary.

## Motivation

The local two-harness workbench demonstrates two valuable activities. A builder
can explore structured capabilities through Nushell, and can run a checked-in
scenario through two real harnesses and compare their evidence. These
activities share a workspace and controller, but the question being evaluated
still originates outside the exploration that motivated it.

The two-harness baseline left two gaps. The interactive-session milestone closes
the first by letting the shell ask the selected agent to work in the environment
being explored. The second remains: a useful interaction cannot yet become an
editable evaluation without manually reconstructing its task, starting state,
and expected outcome.

Harness builders need a shorter path between developing a working feel for an
agent and preserving what they learn. They should be able to inspect a
capability, give the real agent a task, follow up in the same session, observe
what changed, and turn the consequential part of that history into a
repeatable question. The workbench should retain enough evidence to help with
that transition instead of asking the builder to remember and transcribe it.

This interaction also makes the structured shell more central to Agent Lab.
Nushell is not merely a terminal beside an evaluation UI. It becomes a
language for inspecting the active assembly, supplying structured context to
an agent, observing a turn, and operating the evaluation lifecycle. The
browser presents the same controller-owned resources visually rather than
maintaining a parallel workflow.

## Product contract

### The opening experience is answer-first

The primary user is a person who builds or improves agent harnesses. They may
already understand models, tools, workspaces, and evaluations, but they should
not need to learn Agent Lab's controller vocabulary before receiving value. In
the first few minutes they should be able to:

1. ask the selected real harness to investigate the active workspace;
2. read the assistant's answer in the shell;
3. understand the same turn's capability calls, native actions, and effects in
   the browser;
4. follow up in the same native session;
5. compare the consequential turn with another harness; and
6. begin preserving the discovery as an evaluation.

The shell is where the builder explores and acts. The browser Session is where
the builder develops a legible account of what the harness answered, invoked,
and changed. Assembly identities, normalized events, and raw evidence remain
available for inspection, but they support this experience rather than leading
it.

### Agent sessions belong to the workspace

An `AgentSession` is a controller-owned resource attached to one Explore
workspace. It binds a real harness session to a harness, model profile,
workspace, capability-source identities and authorization, and evidence stream.
The selected harness and model are resolved when the session is created and
remain fixed for the life of that session.

Each Explore workspace has an active agent-session pointer. Multiple sessions
may coexist, and every browser or PTY attached to the workspace observes the
same pointer. Switching it does not close or recreate either session. This
allows a builder to try another harness or line of inquiry and then return to
the earlier conversation with its identity and history intact.

`agent "prompt"` continues the active session. If no session exists, Agent Lab
creates one from the shared workbench harness and model selection, reports its
startup phases, waits for it to become ready, and then starts the turn.
`agent new` provides the explicit form and may override the shared selection
for that session without changing the workbench default.

Creating a session does not replace the active pointer until the driver has
opened its native session successfully. A failed startup leaves the previous
session active and retains the failed startup record for inspection.

One Explore workspace accepts at most one active interactive agent turn against
its mutable workspace at a time. This prevents two sessions from racing on the
same files while the workbench is trying to preserve a coherent pre-turn state
and resulting effects. Snapshot-based validation and evaluation attempts retain
their own concurrency policy. Other sessions remain inspectable while the turn
runs, and the human workspace attachment remains usable with its interventions
recorded. Switching sessions or closing the session with the active turn
requires the builder to finish or cancel that turn first.

The first slice conservatively marks browser-observed keyboard, paste, and input
composition during an active turn as human intervention, so a resulting
workspace diff is never presented as clean agent-only evidence. Terminal
protocol replies share the PTY byte stream but are not human intervention. The
slice cannot yet observe edits made outside an attached Agent Lab terminal.
Evaluation promotion therefore requires a clean controlled replay until a
workspace observer can attribute editor and external-process mutations
directly.

Each implemented turn captures the current controller-owned capability
revisions and passes that capability projection to the driver with the turn.
Adapter acknowledgement remains a stabilization contract: the driver should
acknowledge the exact source identities and revisions it will observe. If the
harness cannot refresh its native session, a future stale-session transition
should refuse further turns until the builder creates another `AgentSession`
with `agent new`. Agent Lab must not replace a native harness session behind an
existing `AgentSession` identity. Until acknowledgement exists, retained
capability revisions describe the controller-supplied projection rather than a
separately verified adapter observation.

The controller resource outlives a PTY attachment. Closing or reconnecting the
browser terminal revokes that terminal's control grant without discarding the
agent session. Explicit close, workspace disposal, or server shutdown closes
the driver and its children together. After an unexpected server exit, stored
evidence remains inspectable and the live session is marked interrupted; this
RFC does not promise cross-process harness resumption.

The implemented slice enforces bounded turn input and duration and closes the
driver process tree through explicit session and controller lifecycle paths.
Idle eviction and reported effective session bounds remain stabilization
contracts. When added, eviction should record an observable reason and preserve
the session's turns and effects for inspection. Unless an adapter has an
explicit resumable-session contract, an evicted session should require the
builder to create a new `AgentSession`.

Model access is refreshed before each turn through the server's model-access
provider. An adapter may update an existing native session only through an
explicit credential-refresh contract. If an expired credential cannot be
refreshed in place, Agent Lab marks the `AgentSession` authentication-stale and
requires `agent new`; it does not replace the native session under the old
identity.

The first interactive-session slice intentionally stops short of that target:
it resolves model access once, immediately before creating the native session.
Per-turn refresh and an observable authentication-stale transition remain a
stabilization gate. The implementation must not claim freshness it has not
verified, and a builder can always use `agent new` to create a new identity with
freshly resolved access.

### Turns preserve structured context and effects

Every turn records:

- the session and turn identity;
- the human prompt and any structured pipeline input;
- an immutable pre-turn snapshot identity and content digest;
- capability-source identities and revisions;
- the bound harness, model profile, and resolved model identity in protected
  evidence;
- native model, tool, capability, workspace, usage, and terminal events;
- cancellation, failure, completion, and human-intervention outcomes.

The prompt is one explicit Nushell string. Any non-empty pipeline input is
additional structured turn context; it never silently replaces the prompt.
For example:

```nu
catalog list | agent "Analyze these items"
```

passes the table as explicit turn context rather than rendering it into an
unattributed string. Each harness adapter decides how to present that context
to its native loop. The implemented evidence preserves the structured value
after credential redaction; raw sensitive values are not durable evidence. A
dedicated canonical-input digest and redaction marker remain requirements for
portable evaluation promotion.

The default interaction is answer-first. After terminal evidence is durable,
`agent "prompt"` returns a tagged `agent-answer` record containing the response
and messages, session and turn identities, summarized activity, usage, outcome,
error, and evidence references. At the top-level prompt, Nushell recognizes
this value and renders its response as Markdown. When the value is bound or
piped, the record remains structured and unchanged. `agent turn` reopens the
same canonical answer shape.

`--stream` changes the output projection to a text stream containing assistant
Markdown as it arrives. `--raw` changes it to a stream of attributable session
events. The two modes are mutually exclusive; neither changes durable turn
retention. Lifecycle records are not part of the default human result.

Agent Lab also retains the normalized cross-harness presentation and native
event stream used to reconstruct and inspect the turn.

Ctrl-C cancels the active turn. The Session view and controller history retain
its partial response, actions, effects, and raw evidence.

### Exploration produces an evaluation draft

Evaluation promotion begins from a contiguous span of stable turn identities in
one agent session. The source span is evidence, not automatically the script
for the future evaluation. A useful evaluation normally needs a standalone
task and a controlled state from which another harness can attempt that task
without the original conversation.

An evaluation draft contains:

- a question and standalone task;
- the workspace state immediately before the first selected turn;
- a reproducible capability assembly, its source revisions, and any restorable
  source state;
- execution limits;
- hard assertions, evaluator identity and version, and separately identified
  tracked measurements;
- source references for the turns, observations, and effects from which each
  proposed field was derived;
- source-session and proposal-session provenance;
- validation attempts and their evidence.

The draft is designed to be independent of compatible harness and model
variants. Its definition does not require the harness or model that produced
the source interaction. Their identities remain in provenance and provide the
default configuration for the first validation replay. The first steel thread
tests this design with one explicit alternative rather than claiming general
portability.

A selected span may cross workspace or capability revisions. The proposer must
either normalize every required transition into explicit fixture setup or mark
the draft incomplete. It cannot silently combine multiple source assemblies
into one reproducible starting state. Human interventions and conversational
dependencies receive the same treatment.

This separation gives five kinds of identity to resources that the current
implementation partly combines:

1. **Evaluation draft.** An editable proposal with a stable identity, source
   evidence, revision history, and validation history.
2. **Evaluation definition.** A stable saved case created from an explicitly
   promoted evaluation revision.
3. **Evaluation revision.** An immutable schema-versioned task,
   snapshot and capability recipe, limits, assertions, evaluator version, and
   measurements. A draft points to its current revision; promotion attaches that
   exact revision to a definition without rewriting it. Each material edit
   creates a new revision rather than changing the target of an existing
   validation.
4. **Validation attempt.** One replay of an exact evaluation revision from its
   captured source state.
5. **Evaluation attempt.** One later execution of a definition with concrete
   harnesses, models, limits, and resulting evidence.

### AI assistance remains attributable advice

Agent Lab should use a separate `ProposalSession` to help select a meaningful
turn span and draft the standalone task, assertions, and measurements. A
`ProposalSession` is an operation-scoped external-driver session, not an
interactive `AgentSession`: it never appears in `agent sessions`, never becomes
the workspace's active session, and closes after its proposal operation. Its
durable identity and evidence remain available after the driver exits.

By default, the `ProposalSession` uses the source session's harness and model
profile but has no mutation authority over the Explore workspace. The workbench
should record the proposal-session identity, harness, model, adapter revision,
prompt contract, source evidence references, and returned structured value.

The proposal session should receive the redacted evidence needed for its task.
Its model-provider credential is resolved directly into the isolated child
process through the server's model-access boundary. It does not receive source
capability credentials or workspace control grants. Its output must conform to
a versioned draft schema before the workbench presents it as a candidate.
Proposed assertions reference reviewed, versioned evaluators and structured
parameters; Agent Lab must not execute model-generated assertion code.

The proposer may suggest the source span when the builder does not specify
one. The builder can instead select turns directly, edit every proposed field,
or construct a draft without proposal assistance. Agent Lab must never silently
turn model output into an evaluation definition.

### Validation checks reproducibility before promotion

Validation replays the current evaluation revision from its captured pre-turn
state. Before validation, Agent Lab should finalize the exact sanitized
snapshot that the local library would retain. Validation executes that
persisted snapshot, not a more privileged source copy. If redaction changes
task-relevant content or cannot produce a safe restorable state, the attempt is
inconclusive.

The capability assembly must likewise restore its declared revisions and state
or report that the attempt is inconclusive. Agent Lab must not substitute the
latest available state. The first replay requests the source session's harness,
model profile, concrete model, driver, and adapter revisions when they remain
available. Any drift is displayed and changes the claim to current-stack
validation rather than exact source reproduction. The replay is an ordinary
evidenced attempt with the same limits, cancellation, cleanup, and redaction
guarantees as other Agent Lab runs.

A validation attempt distinguishes execution integrity from behavioral outcome.
It is complete only when Agent Lab restored the declared state, executed the
task, evaluated the assertions, and finalized replayable evidence. Its behavior
may then pass or fail the assertions. Declared execution-limit exhaustion or a
harness-reported task failure is a complete behavioral failure when assertions
and evidence can still be finalized. Harness startup, protocol, controller,
evaluator, or evidence-finalization failure is inconclusive. Missing model
access, failed state restoration, corrupted evidence, cancellation, or human
intervention remain explicit inconclusive, cancelled, or intervened outcomes
rather than task failures.

A complete passing replay means that this evaluation revision satisfied its
assertions once. It does not establish a general model-quality claim.
Repetition, variants, and statistical interpretation belong to later
evaluation attempts. Every material edit creates a new evaluation revision;
validation attached to an earlier revision never transfers to the new one.

A complete failed replay remains useful. Agent Lab should retain the candidate
and its failure evidence as an editable draft so the builder can distinguish a poor
proposal, an incomplete assertion, nondeterministic behavior, and a genuinely
difficult case. The UI should present those outcomes directly rather than
manufacturing a successful evaluation.

Saving is always explicit. `save` persists exactly one selected draft revision
in the local library. When that revision has completed a passing replay, the
same explicit action promotes that exact revision into a runnable evaluation
definition. A failed, inconclusive, cancelled, intervened, stale, or untried revision is
saved only as a clearly marked draft. The command should return a typed draft
or definition record with its status; passing validation never promotes or
publishes a definition by itself.

### Definitions are local before they are repository artifacts

Agent Lab should maintain a local, versioned evaluation library. Saving a draft
atomically retains an immutable evaluation revision and its restorable source
state while preserving editable history, provenance, and validation attempts.
The saved case cannot depend indefinitely on the originating mutable workspace
or a separately garbage-collected run bundle. The definition and completed
attempts can be reopened without a live driver, capability source, or model
credential.

A saved definition should later run with selected harness and model variants.
Those choices belong to the attempt and do not rewrite the definition. The
first thread tests whether an interaction observed through one harness can
become one controlled two-harness catalog comparison.

Exporting a definition into a repository-owned scenario is a later, explicit
operation. Export must materialize its seed, capability recipe, evaluator
references, and safe provenance without copying credentials, private evidence,
or machine-specific paths. The first steel thread should prove the local
library and leave the repository export format open to evidence.

## Workbench surfaces

### Nushell

The implemented interactive-session command surface is:

```nu
agent
agent "prompt" [--stream | --raw]
agent turn [turn-id]
agent new [--harness harness-id] [--model model-profile]
agent sessions
agent switch session-id
agent cancel
agent close [session-id]
```

Bare `agent` returns the active session as a structured record. `agent
sessions` returns a table with an explicit active marker. Session, harness,
model, and turn arguments participate in completion, and every implemented
command has native help.

`agent "prompt"` and `agent turn` return the canonical `agent-answer` record.
Direct REPL display renders its response as Markdown; pipeline consumers receive
the record itself. Builders can select `$answer.response` as text or pass it
through `from md` when they want Nushell's structured Markdown parser.

`agent close` selects the active session when its identity is omitted and fails
if there is no active session. `agent cancel` targets the workspace's active
interactive turn and fails if none is running. For `agent new`, omitted harness
and model arguments use the shared workbench selection and fail before creating
a session when no compatible selection is available.

The proposed evaluation-promotion command surface is:

```nu
lab evaluation propose [--session session-id] [--from turn-id --through turn-id]
lab evaluation new --from turn-id --through turn-id [--session session-id]
lab evaluation validate [draft-id] [--raw]
lab evaluation save [draft-id] [--name name]
lab evaluation run definition-id [harness-a harness-b] [--model model-profile]
```

Omitting the session from `propose` or `new` selects the active
`AgentSession`; the proposed command should fail clearly if none exists.

When `--from` and `--through` are supplied, both are required. The controller
verifies that the stable turn identities belong to the selected session and
bound one contiguous span. When they are omitted, the proposal agent may
suggest the span. `new` always requires both bounds and constructs an incomplete,
editable draft directly from that evidence span without starting a
`ProposalSession`. It fills the captured state, capability, and provenance
fields. The selected turn prompt and scenario may seed explicitly labelled
task, assertion, and measurement suggestions, but the draft remains incomplete
until the builder confirms or edits those fields by creating a revision.
That confirmation, rather than the presence of suggested values, is what makes
the manual draft eligible for validation.

`propose` returns the full draft record as structured data. `validate` and
`save` require exactly one draft source: either the positional draft identity
or a non-empty draft record from pipeline input. Supplying both or neither is
an error. This permits a Nushell-native editing flow without requiring
generated code:

```nu
let draft = (lab evaluation propose)
$draft | update task "Summarize the active catalog items" | lab evaluation validate
```

The record carries its base revision identity. A pipeline or browser edit
creates a new immutable revision, and the controller rejects a stale edit
instead of overwriting a newer revision.

A positional draft identity resolves the current immutable revision atomically
when `validate` or `save` begins. A positional definition identity resolves the
exact promoted revision attached to that definition. Every response and attempt
records the resolved revision identity; a later browser edit cannot change the
target of an operation already in progress.

The existing `lab evaluation [evaluation-id]` inspection behavior remains the
inspector for current paired evaluation attempts. Drafts and saved definitions
use distinct identities and controller resources; the longer `propose`,
`validate`, `save`, and `run` command names do not make bare `lab evaluation`
choose among unlike resource kinds. `run` uses the shared comparison pair when
no harnesses are supplied and requires exactly two when they are explicit. It
uses the shared model profile when `--model` is omitted and fails before launch
when either harness lacks a compatible mapping. It creates a concrete paired
attempt that appears in the existing evaluation history and can be inspected
with `lab evaluation`. The browser should expose the same New Draft, Save, and
Run comparison operations and reopen both saved drafts and definitions from the
local library.

### Browser

The browser and Nushell project the same implemented session resources. A
shell-created agent session opens a conversation-first Session view without
unmounting the terminal or stealing its focus. Each turn shows, in order:

- the builder's prompt;
- optional **Provided context**, collapsed and omitted entirely when no
  pipeline value was supplied;
- the assistant response as primary content, with **Rendered** selected by
  default and **Source** exposing the exact retained Markdown for each message;
- a compact chronological trail of capability calls, native actions,
  approvals or failures, and workspace effects;
- duration, usage, and projection completeness when reported; and
- a disclosure containing source revisions, evidence identities, and raw
  native events.

The session-level **Rendered**/**Source** control changes presentation only.
**Rendered** uses constrained Markdown rendering: raw HTML is not executed,
remote images are not fetched, and unsafe links are disabled. **Source**
preserves message boundaries and exposes the retained Markdown without mutating
evidence.

The Session view does not place an unrelated Explore-run review beneath the
conversation. Assembly and raw lifecycle state remain available as secondary
disclosures.

The proposed promotion projection extends that view with **Compare this turn**
and **Make evaluation** as natural continuations. **Make evaluation** should
open an editable draft rather than silently saving model output as an
evaluation.

A shell-created evaluation proposal should open the corresponding draft review
without unmounting the terminal or stealing its focus.

The draft view makes the following relationships visible:

- selected source turns and their pre-turn state;
- proposed task, assertions, and measurements;
- source and proposer provenance;
- edits that make prior validation stale;
- validation attempts, assertion mismatches, and workspace effects;
- whether the current draft revision has been promoted into a runnable
  definition.

The browser's New Draft operation uses the same manual `new` operation as
Nushell: the builder selects an evidence span and receives an incomplete,
editable draft without starting a `ProposalSession`. Propose remains the
separate AI-assisted operation.

Browser edits and shell operations should observe the same revision. Updates
travel through controller events rather than independent browser state or idle
polling.

## Identity, authorization, and evidence

Agent sessions, turns, proposal sessions, drafts, definitions, evaluation
revisions, validation attempts, and evaluation attempts have distinct stable
identities. Evidence references use these identities plus source-local sequence
numbers; display order is not treated as provenance. A validation attempt also
records the concrete model, driver and adapter revision, controller revision
when available, limits, and evidence bundle it actually exercised.

The workspace-scoped shell grant authorizes operations only for the attached
Explore workspace and its sessions, proposal sessions, drafts, definitions,
evaluation revisions, and attempts. In the first boundary, every saved library
entry retains an owning Explore-workspace identity. A shell grant may read or
run only entries owned by its workspace; arbitrary definition identities do not
widen its authority. Cross-workspace attachment or import is a later explicit
operation. The server-authenticated browser may reopen the local library after
the source workspace is no longer live.

Interactive agent drivers and validation runs receive only the provider
credentials and capability endpoints required for their operation, resolved
directly into the selected child process. Proposal sessions receive only their
model-provider credential and the bounded redacted evidence projection
described above.

Model-provider credentials and workspace shell grants must not enter Nushell
environment values, browser-readable response bodies, logs, events, drafts,
definitions, or evidence bundles. Browser authentication tokens necessarily
travel through the browser's authentication transport; they remain scoped to
that mechanism and are never persisted as product state or evidence.

Persisted source evidence remains immutable after redaction. Sensitive
pre-redaction input is transient operation data, not durable evidence. A
proposal, edited draft, assertion result, or comparison is a derived projection
with links back to its inputs. The workbench should therefore be able to explain
what the builder selected, what the proposer inferred, what the builder changed,
and what the validation replay actually established.

## Relationship to the existing architecture

The external driver protocol already models a long-lived process and session
with multiple turns. Paired scenario runs continue to use one turn before
closing their session. The interactive-session milestone exercises the same
protocol shape through a workspace-owned session actor rather than introducing
a universal agent loop inside Agent Lab.

Harnesses continue to own their prompts, tools, model loop, context policy,
checkpoints, approvals, and native events. Agent Lab owns session binding,
controlled workspace state, operation coordination, and evidence. Evaluation
promotion will remain controller-owned, and proposal assistance should run
through an identified external driver instead of adding a model SDK or hidden
prompt loop to the public core.

The current paired evaluation controller accepts a checked-in scenario and
creates two concrete attempts. Evaluation promotion adds the missing portable
definition between exploration and those attempts. Existing scenarios and
evidence bundles remain valid; definitions created through the workbench should
become another source of the same controlled run machinery.

## Drawbacks

Capturing a reproducible pre-turn workspace state increases storage and
lifecycle cost, particularly during long sessions. The implementation will
need bounded retention and may eventually use content-addressed snapshots or
incremental storage, but the evidence contract requires the state to remain
reconstructable.

AI-assisted proposal can introduce plausible but unsupported tasks or
assertions. Attribution, editable drafts, schema validation, explicit save, and
replay reduce that risk without treating the proposer as an authority.

Proposal and validation add model calls, startup latency, credential use, and
stochastic failure before an evaluation becomes runnable. Defaulting the
proposal session to the source harness and model may also reproduce that
system's assumptions or blind spots. Visible proposer identity and a fully
manual draft path keep this cost and bias inspectable.

Persistent agent sessions add process ownership, readiness, interruption, and
cleanup states to the controller. Making those states explicit is more work
than launching one process per run, but it is necessary to distinguish harness
startup from turn latency and to support real exploratory conversation.

Separating a local evaluation library from repository export creates one more
artifact lifecycle. It also preserves a low-friction exploratory workflow while
keeping source-control changes intentional and reviewable.

The first boundary keeps a complete behavioral failure in draft state until a
current revision passes. This conservative gate makes runnable definitions easy
to interpret, but it may be awkward when the desired artifact is a known
failing regression case. Repeated draft validation retains that case while
direct use tests whether later RFC revisions should relax the promotion rule.

## Alternatives considered

### Continue authoring scenario manifests manually

Checked-in scenario manifests make review and exact inputs straightforward, and
they remain a useful export target. They require the builder to reconstruct the
task, starting state, and expected behavior after exploration, which is the gap
this proposal addresses. Manual authoring remains available when AI assistance
or captured state is inappropriate.

### Keep direct agent interaction in the browser

A browser-only chat would provide familiar interaction, but it would leave the
structured shell adjacent to the agent rather than making exploration,
pipeline input, and harness operation one programming model. The browser
remains an important synchronized projection, not the exclusive control
surface.

### Ask the active agent to propose its own evaluation

Continuing the source session is mechanically smaller, but it mixes proposal
activity into the history being evaluated and may mutate the shared workspace.
A separate, read-only proposal session keeps source behavior and derived advice
distinct while still allowing the same harness and model to supply the default
proposal capability.

### Save the successful run as the evaluation

A preserved run is valuable evidence, but it contains a concrete harness,
model, conversation, and resulting state. Treating it as the definition would
make exact replay easy at the cost of a portable question. Promotion instead
derives a standalone task and controlled starting state while retaining the
run as provenance.

### Pin the source harness and model in the definition

Pinning would create a precise regression case for one stack, but would prevent
the same discovery from naturally testing another harness or model. Concrete
attempts retain exact identities; the definition remains variant-independent.

### Begin with code-server diagnostics

Editor diagnostics remain the next richer capability experiment. Completing
the promotion loop first lets that experiment begin as a question discovered,
authored, validated, and rerun through the product rather than as another
checked-in scenario.

## Validation boundary

The first steel thread should prove one complete catalog-based path as a
five-minute harness-builder walkthrough. The participant understands agent
harnesses but receives only this orientation:

> Use the shell to work with the agent and the Session view to understand what
> the harness did.

The walkthrough is:

Steps 1-5 exercise the interactive-session substrate. Steps 6-10 describe the
manual promotion boundary demonstrated by steel thread 0005.

1. Run `agent "Find the active catalog items and explain what matters"` without
   pipeline input. A real harness discovers the catalog capability, and the
   shell returns a readable answer.
2. In Session, identify the same prompt and answer, the capability calls that
   produced it, any native actions, and whether the workspace changed without
   opening raw trace.
3. Run `agent "Why did you prioritize gamma?"` and demonstrate continuity in
   the same native session.
4. Pipe a structured catalog value into another turn as explicit **Provided
   context**, preserve its redacted structured form, and show no placeholder on
   turns that have no pipeline input.
5. Create a second session, switch between sessions, and reopen the first with
   its conversation and evidence intact.
6. Choose **Make evaluation** on a stable turn and create an editable draft
   from its durable pre-turn state and capability recipe.
7. Review and edit the same draft through Nushell and the browser.
8. Replay it from the captured state, retain one complete failed validation,
   revise the evaluator parameters, and produce a passing current revision.
9. Explicitly save and reopen the portable definition, then run it through v0
   and Eve without rewriting the task.
10. Verify provenance, redaction, cancellation, process-tree cleanup, bounded
    retention, terminal reconnection, and offline replay.

Acceptance is persona-level as well as mechanical. Without command-by-command
coaching, the participant should be able to explain what the agent answered,
what the harness invoked or changed, one cross-harness difference, and what the
saved evaluation preserved.

Deterministic external drivers establish session, projection, validation,
failure, and recovery contracts. One real-model catalog demonstration,
including a retained assertion failure, corrected passing revision, explicit
promotion, and paired variant run, establishes that the interaction survives
contact with actual harnesses. The visible walkthrough completed locally; its
public-safe evidence is summarized in steel thread 0005.

## Boundaries

This RFC's demonstrated manual boundary extends interactive sessions with
evaluation drafts, bounded validation attempts sufficient to retain a failed
revision and pass a corrected revision, local versioned saving, and one paired
variant run.

The following work remains available for subsequent evidence:

- attributable AI-assisted proposal drafting;
- a general assertion or evaluator language;
- repeated trials and statistical quality claims;
- cross-harness checkpoint or conversation portability;
- repository export and review workflow;
- server-independent live-session resumption;
- code-server diagnostics and broader editor capabilities;
- a universal operation registry or agent SDK.

## Remaining implementation questions

The manual steel thread answered the initial snapshot, evaluator, revision,
validation, and local-library questions. The next slices should answer:

- Which normalized assistant, capability, native-action, workspace-effect, and
  usage observations must an adapter supply, and how should incomplete
  projections remain visible?
- How should proposal progress remain readable and composable without replacing
  its useful draft with lifecycle records?
- Which local definition layout supports later repository export without
  freezing that export format prematurely?

## Relationship to other RFCs

[RFC 0001](0001-product-thesis.md) defines the interactive learning loop and
the principle that evaluations preserve exploration. This RFC supplies the
provisional interaction contract for the promotion step.

[RFC 0002](0002-steel-thread-method.md) governs the evidence boundary and the
revision of this proposal after implementation.

[RFC 0003](0003-first-evidence-backed-architecture.md) records the demonstrated
controller, driver, capability, surface, and evidence boundaries. Manual
promotion now adds revision-owned snapshots, distinct validation attempts, and
portable definitions without changing RFC 0003's Candidate status.
