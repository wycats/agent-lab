# RFC 0001: Product thesis

- Status: Provisional

## Summary

Agent Lab is an open workbench for understanding and improving agent
harnesses. It lets builders inspect and operate the same workspaces and
capabilities their agents use, exposes harness configuration and runtime state
through structured interfaces, runs the real agent under controlled
conditions, and turns its behavior into repeatable evaluations backed by
replayable evidence.

The primary audience is people who build agents and want to understand or
improve the harness around them. Agent Lab may make that work accessible to
more people over time, but it begins as a power tool for builders who need to
reason about prompts, tools, context policy, execution environments, effects,
and outcomes together.

The product is organized around an interactive learning loop. A builder
assembles a harness and environment, explores them directly, forms a question,
runs the real agent, inspects what happened, and preserves useful discoveries
as evaluations. Those evaluations create a durable basis for changing the
harness and beginning the loop again.

The workbench is where builders spend time. Evaluations are how exploration
becomes durable improvement.

## Motivation

Agent behavior is shaped by more than a model and prompt. The harness chooses
which tools exist, how their definitions are presented, what context is
retained or compacted, how permissions work, where effects occur, how sessions
resume, and what evidence survives a run. These choices are difficult to
understand from source code, chat transcripts, or aggregate scores alone.

Harness builders need to move between two modes that are often separated:

- direct exploration of the environment and harness machinery;
- repeated evaluation of agent behavior under controlled conditions.

Exploration without preservation produces anecdotes. Evaluation without
exploration encourages assertions about a system the builder has not developed
a working feel for. Agent Lab connects them: direct interaction develops the
builder's model of the system, and the workbench helps promote consequential
discoveries into repeatable evaluations.

This relationship matters especially for agent systems because:

- behavior is stochastic and a single successful run may not establish an
  improvement;
- context, compaction, tool projection, permissions, and recovery are part of
  the implementation under test;
- the execution environment may be local, virtual, remote, or supplied by the
  harness itself;
- a human needs to inspect both what the agent could observe and the harness
  decisions that shaped its behavior;
- correctness, cost, latency, selection behavior, and retained context may all
  matter to one experiment.

## Product thesis

### The workbench supports a learning loop

Agent Lab should make the following cycle coherent:

1. **Assemble.** Choose a harness, model, workspace, capability sources,
   permissions, limits, and initial state.
2. **Explore.** Inspect and operate the environment and the harness directly.
3. **Form a question.** State the behavior, difference, risk, or improvement
   worth investigating.
4. **Run.** Exercise the real harness under controlled conditions.
5. **Inspect.** Relate model activity, tool use, context changes, workspace
   effects, usage, and outcomes.
6. **Preserve.** Turn useful discoveries into seeds, setup, assertions,
   metrics, scenarios, baselines, or evaluation cases.
7. **Change and repeat.** Modify the harness or environment and use the
   preserved evaluation to understand the result.

These stages are not a wizard or a required linear workflow. Each produces
structured material that the builder can inspect, compose, save, revise, and
reuse. A builder may move between exploration and execution many times before
an observation is ready to become an evaluation.

The workbench should actively help with this loop. It should make the current
assembly legible, provide enough orientation to begin a meaningful
investigation, retain the history and origin of observations, and offer a
natural path from an exploratory action to a durable artifact.

### Exploration has a programming model

Agent Lab should not present an empty terminal and expect the builder to invent
both the investigation and the interface. An exploratory session should make
five things clear:

1. **Question.** What behavior or uncertainty is the builder investigating?
   This may be a lightweight charter rather than a scripted test.
2. **Assembly.** Which harness, model, workspace, capabilities, policies, and
   limits are active?
3. **Operations.** What can the builder inspect or operate, and what effects do
   those operations have?
4. **History.** What did the human, harness, agent, and environment each do?
5. **Promotion.** Which parts of the exploration should become durable inputs,
   assertions, metrics, or scenarios?

The interface should teach this model through use. Completion, help, examples,
structured values, visible state, and origin-labelled events are product
behavior, not documentation added after the fact.

A useful exploration can leave behind several kinds of durable material:

- workspace state can become a seed or fixture;
- a sequence of operations can become reusable setup;
- an observation can become an assertion or tracked metric;
- a run can become an evaluation case or comparison baseline;
- an unexpected result can become the question for the next exploration.

### Humans and agents meet in the same environment

The builder must be able to reason about the workspace and capabilities the
agent is actually using. Agent Lab should always make it clear which workspace
a run is bound to, what state and revision it has, and whether a human action
observes or changes that same state.

Human and agent surfaces do not need identical ergonomics. A human may use
structured pipelines while an agent uses native tool calls. They must retain
enough shared identity and evidence to establish whether they operated on the
same capability source and workspace state.

Human intervention during a run remains possible and is recorded explicitly.
An intervention changes how a result may be compared, but it does not make the
workbench unusable while an agent is active.

### The harness remains real

An agent harness owns its prompts, tool definitions, model loop, context
policy, checkpoints, approval behavior, native event taxonomy, and other
agent-specific semantics. Agent Lab supplies a controlled environment,
inspection surfaces, run coordination, and evidence without replacing those
choices with a universal agent implementation.

Where a harness permits it, Agent Lab should expose its configuration and
runtime state through structured projections. Prompts, tools, context budgets,
checkpoints, permissions, sessions, and usage may therefore be inspected and
composed through the workbench while remaining owned by the harness.

Native facts must survive projection. A shared vocabulary helps builders
compare harnesses, but it should not erase meaningful differences.

### Evaluations preserve learning

An evaluation begins with a question and enough controlled state to revisit
it. It may include a seeded workspace, capability revisions, a task, one or
more harness or model variants, limits, assertions, tracked measurements, and
the evidence required to interpret the result.

Agent Lab should support both deterministic checks and repeated real-model
trials. It should help builders distinguish:

- hard correctness gates from tracked measurements;
- one observed run from a repeated behavioral claim;
- raw evidence from normalized comparison views;
- task success from secondary characteristics such as cost, latency, tool
  selection, recovery, or context retention.

The workbench is the primary environment. Evaluations are not a separate mode
for specialists; they are the durable continuation of ordinary exploration.

## Product model

The following concepts describe the current product model. Their precise
interfaces remain provisional.

### Harness

The agent implementation under study. A harness can be inspected, started,
observed, cancelled, and evaluated to the extent its adapter supports those
operations. Harness-specific concepts and native events remain available.

### Workspace

The files, revisions, processes, limits, and effects available to an agent run.
A workspace has explicit identity and lifecycle. It may be physical, virtual,
remote, or supplied by the harness's execution host.

### Workspace attachment

A human interface bound to a workspace. An attachment may expose structured
filesystem operations, an editor, a terminal, semantic services, or other
host-supported operations. A workspace attachment is distinct from a promise
that every execution host can provide a normal local shell.

### Capability source

An authoritative source of operations or information presented to a human or
agent. MCP is the first capability protocol, not the generic capability model.
Discovery, identity, revisions, invocation, progress, authorization, and
effects remain attributable to their source.

### Run

One execution of a harness bound to a particular assembly. A run correlates the
harness session, workspace, capability sources, model configuration, limits,
events, effects, and terminal outcome.

### Evidence

The immutable observations required to understand or replay a run. Evidence
may include native harness events, capability observations, workspace changes,
usage, logs, and named comparison projections. Evidence makes evaluation
results inspectable rather than merely scored.

### Evaluation

A versioned question exercised through one or more controlled runs. An
evaluation specifies how to assemble the relevant state, what must hold, what
should be measured, and how repeated or variant runs should be compared.

## Product surfaces

The product model currently suggests four modes. These are user jobs, not a
commitment to four tabs.

### Workbench

Assemble a harness and environment, inspect their active state, and explore
them directly through structured commands, editors, capability browsers, and
other workspace attachments.

### Live run

Start or attach to a real harness session. Observe model steps, tool choices,
context changes, workspace effects, usage, approvals, failures, and human
interventions as they happen.

### Evaluations

Create repeatable scenarios from exploratory work, run them across harness or
environment variants, and compare correctness and tracked behavior over time.

### Run review

Reopen a completed run, replay its timeline, inspect its workspace and context
effects, and understand why it passed, failed, or remains inconclusive.

Evidence underlies all four modes. It need not become a separate administrative
destination.

## Structured shell interaction

Nushell is the first structured control and inspection language for Agent Lab.
Its records, tables, pipelines, nested values, completion, and help make it a
strong environment for exploring capabilities, workspaces, harness state, run
events, and evaluation results without flattening them into strings.

Nushell is therefore more than a terminal pane. Its structured language can
cut across the workbench, live runs, reviews, and evaluations.

The first product boundary is an Agent Lab-aware Nushell attachment with
structured operations against the active workspace and harness. Whether a
workspace should also provide a normal Nushell session with the usual portable
commands remains an open design question. Execution hosts may differ in their
ability to support that faithfully, so ordinary shell access is an optional
host capability rather than part of the workspace definition.

## Multiple real harnesses

Agent Lab should develop its shared vocabulary against multiple demanding
harnesses rather than generalizing from one implementation. v0 and
[Eve](https://github.com/vercel/eve) are a strong initial pair because they make
different choices about tools, sessions, execution, durability, context, and
evaluation.

Eve can integrate through public code in Agent Lab. The product-specific v0
adapter remains in v0's owning repository and connects through Agent Lab's
public protocol and synthetic conformance fixtures. No v0 prompts, tools,
private evidence, or product-specific adapter code move into this repository.

A useful early demonstration is intentionally simple: select Eve or v0 through
their respective adapters, inspect what the harness reports about itself,
attach it to a resettable seeded workspace and capability source, run it,
observe its native activity, and evaluate the outcome. The two harnesses do not
need to share a tool loop for the workbench to make their common shape and
important differences legible.

Eve is especially useful pressure on the design because its public inspection
and session APIs expose instructions, tools, skills, model information,
sandbox state, durable session identity, cancellation, and streaming events.
It also has a native evaluation system. Agent Lab should preserve those native
semantics and determine how harness-owned evaluations compose with
cross-harness experiments rather than reimplementing Eve inside Agent Lab.

This work can establish an aspirational common vocabulary around harnesses,
workspaces, capabilities, sessions, runs, actions, observations,
interventions, evidence, and evaluations. The vocabulary exists to make
differences discussable, not to force every harness into one framework.

## Development feedback loop

Agent Lab should be built through the same kind of interaction it promises to
harness builders. A meaningful product slice should remain available as a
resettable, visible workbench in which both a person and an implementation
agent can exercise the feature directly.

For each slice:

- the workbench explains the active assembly and offers a meaningful place to
  begin exploring;
- a person can operate the feature directly and deviate from the expected
  path;
- implementation agents validate through the same visible product surface in
  addition to lower-level automated tests;
- confusing, surprising, or inexpressible interactions become design input;
- useful discoveries become the next scenario or evaluation.

This is both a development method and a product requirement. Harness builders
need the same movement between direct manipulation, observed agent behavior,
and durable evaluation.

## Design values

### Structured exploration is product behavior

Completion, help, tables, pipelines, nested values, errors, artifacts,
progress, and visible state are part of the interaction model. Adapters should
preserve structured values and native identity.

### Agents are implementations under test

Agent Lab controls the experiment around a harness without absorbing the
harness's prompt model, tool taxonomy, context policy, or loop.

### Evidence supports generalization

The project extracts contracts from working end-to-end slices. Raw evidence is
retained even when named projections make runs comparable. A plausible
abstraction without a demanding consumer remains a hypothesis.

### Effects and identity remain explicit

Workspace identity, capability revisions, authorization, side effects,
progress, recovery, intervention, and session ownership survive presentation
through shells, protocols, agent drivers, and evaluation reports.

### Public and private integrations remain separable

The public repository defines neutral protocols, synthetic fixtures, and
public integrations. Product-specific adapters can implement those protocols
in their owning repositories without publishing proprietary code, prompts,
tools, data, or evidence.

### Interaction drives the design

Product ergonomics and shared contracts are revised against direct use. The
workbench should make its programming model learnable and should preserve the
path from an exploratory question to a durable evaluation.

## Relationship to prior art

Exploratory testing treats learning, test design, execution, and result
interpretation as mutually supporting activities. Session-based testing adds a
lightweight charter that focuses an investigation without prescribing its
steps. These ideas inform the workbench's movement between a question, direct
interaction, observation, and a durable evaluation.

- [Exploratory Testing](https://martinfowler.com/bliki/ExploratoryTesting.html)
- [Session-Based Test Management](https://www.satisfice.com/download/session-based-test-management)

REPL-aided development contributes the movement between improvisation and
automation: perform small tasks directly, develop a working understanding, and
gradually encode what should persist. It also warns that an ephemeral REPL is
not a substitute for preserving learning in code, tests, documentation, and
data.

- [Programming at the REPL: Introduction](https://clojure.org/guides/repl/introduction)
- [Guidelines for REPL-Aided Development](https://clojure.org/guides/repl/guidelines_for_repl_aided_development)

Agent Lab applies these ideas to a system whose behavior depends on stochastic
models, stateful context, tool projection, execution hosts, and multiple forms
of evidence. The precedents are design input rather than a finished interface.

## Product focus

This RFC concentrates Agent Lab on the feedback loop for people who understand
or improve agent harnesses. The project invests first in:

- direct, structured exploration of real harnesses and environments;
- controlled execution of the real agent loop;
- visible and attributable effects;
- promotion of discoveries into repeatable evaluations;
- comparison across meaningful harness and environment variants;
- replayable evidence that supports interpretation.

The project does not currently define a universal agent API, reproduce a
production virtual machine, promise an ordinary local shell for every
workspace, or replace harness-native evaluation systems. Those boundaries let
the workbench learn from real integrations while its product model is still
evolving.

## Current evidence

Existing steel threads have demonstrated:

- an embedded Nushell session with structured MCP discovery and invocation;
- dynamic command refresh, help, pipelines, and native structured values;
- an external agent-driver process boundary;
- distinct capability, harness, workspace, and session identities;
- browser-visible live activity;
- a real-model run against an authenticated capability source and physical
  workspace;
- durable evidence that can reopen after the live driver and capability source
  are gone;
- controller-owned harness and model selection shared by Nushell and the
  browser;
- shell- and browser-initiated paired evaluations from one immutable Explore
  snapshot;
- real v0 and Eve runs using the same model profile, prompt, capability
  revisions, workspace seed, and limits;
- behavioral comparison that preserves native timelines, unavailable metrics,
  workspace effects, and independently replayable arm evidence;
- manual evaluation drafts created from durable interactive turns;
- immutable draft revisions with retained failed and passing validations;
- explicit promotion of an exactly validated revision into the local
  evaluation library; and
- a promoted definition rerun through v0 and Eve and reopened after restart.

The manual catalog promotion is the first coherent end-to-end improvement
loop. A builder can ask a real harness to act, create an evaluation from the
resulting turn, deliberately observe and retain a failed replay, correct the
draft, promote the passing revision, compare v0 and Eve without rewriting the
task, and reopen every resource from stored evidence.

The demonstrated result remains one real-model observation. AI assistance has
not yet proposed a draft, and no harness implementation change has yet been
followed by a controlled rerun of the preserved evaluation.

## Next validation boundary

Add attributable AI assistance to the demonstrated manual path in
[RFC 0004](0004-interactive-agent-sessions-and-evaluation-promotion.md). A
separate read-only `ProposalSession` should recommend a meaningful source span,
standalone task, and reviewed evaluator parameters while producing the same
editable draft resource the builder can already create manually. Its advice,
model, prompt contract, source references, and lifecycle must remain explicit.

After that assistance layer, live code-server diagnostics remain the first
richer capability experiment. They should use the completed promotion loop to
test whether settled semantic diagnostics improve repair behavior while
preserving correctness.

## Open questions

- Which harness facts deserve shared structured projections, and which should
  remain native-only?
- Which parts of the provisional promotion contract in
  [RFC 0004](0004-interactive-agent-sessions-and-evaluation-promotion.md) will
  survive direct use and deserve to become stable product vocabulary?
- When can a workspace attachment faithfully provide a normal Nushell session,
  and how should the host advertise that support?
- How should harness-native evaluations compose with Agent Lab's cross-harness
  scenarios and evidence?
- What amount of repetition and statistical treatment should the workbench
  recommend before a real-model observation is presented as an improvement?
- How should human intervention appear in exploratory runs and comparison
  results without discouraging direct interaction?
