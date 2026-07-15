# RFC 0001: Product thesis

- Status: Provisional

## Summary

Agent Lab is a Rust laboratory for interactive capability exploration and
repeatable agent evaluation. Human exploration and automated trials share one
session, capability, event, and evidence model while retaining frontend- and
agent-specific ergonomics.

The first interactive frontend is an embedded Nushell experiment. The first
capability ecosystem is MCP. Neither is assumed to be the permanent core
contract: the project uses them because structured pipelines, dynamic
discovery, progress, session state, and protocol evolution put useful pressure
on the design.

## Values

### Structured exploration is product behavior

Completion, help, tables, pipelines, nested values, errors, artifacts, and
progress are part of the interaction model. Adapters should not flatten them
into strings to reduce implementation work.

### Agents are implementations under test

An agent owns its prompts, tool definitions, selection policy, and loop. Agent
Lab supplies controlled capabilities, workspace hosts, execution limits, and
evidence collection without imposing one agent framework's tool taxonomy.

### Evidence precedes generalization

The project extracts contracts from working end-to-end slices. A plausible
abstraction without a demanding consumer remains a hypothesis.

### Effects and identity remain explicit

Capability identity, authorization, side effects, progress, recovery, and
session ownership must survive presentation through shells, protocols, and
agent drivers.

### Public and private integrations stay separable

The public repository defines neutral protocols and synthetic fixtures. A
private integration can implement those protocols in its owning repository
without publishing proprietary code, prompts, tools, data, or evidence.

## Initial component model

The current hypothesis distinguishes:

- a session and trial model;
- capability sources such as MCP;
- interactive and noninteractive surfaces;
- workspace and execution hosts;
- agent drivers;
- typed events, traces, and evidence bundles.

These names are provisional. The first steel threads should change them when
the observed boundaries disagree.

## Non-goals

The bootstrap does not attempt to:

- define a universal agent API;
- reproduce a production virtual machine;
- standardize every shell or filesystem feature;
- publish private product integrations;
- claim MCP conformance before a conformance boundary exists.

## First evidence boundary

Embed the current Nushell release, connect a deterministic MCP fixture and one
real open-source server, preserve structured results through native Nushell
pipelines, exercise native command parsing, help, and discovery refresh, and
record typed lifecycle events without constructing a runtime per tool call.
Interactive completion remains important product behavior, but is not evidence
claimed by the first headless embedding thread.
