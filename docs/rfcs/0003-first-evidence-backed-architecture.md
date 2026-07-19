# RFC 0003: First evidence-backed architecture

- Status: Candidate
- Evidence: [steel thread 0001](../steel-threads/0001-nushell-mcp-session.md), [steel thread 0002](../steel-threads/0002-external-agent-driver.md), [steel thread 0003](../steel-threads/0003-catalog-feedback-loop.md)

## Summary

Agent Lab should be a Rust-first experiment controller and evidence system with
independent capability sources, execution hosts, agent drivers, and human
surfaces. It should not be a shell runtime, MCP framework, v0 extraction, or
universal virtual machine.

The first three steel threads support a hybrid product shape. Embedded Nushell
is the first rich human surface because it preserves structured exploration,
state, pipelines, and dynamically discovered commands. A just-bash-backed v0
host is the first bounded agent execution host because it is small, in-process,
and easy to instrument. The catalog slice demonstrates that a person and an
external agent can operate against the same workspace and capability-source
revisions while retaining distinct sessions and native interfaces.

The public Rust controller runs agents as external drivers, observes capability
sources separately, and retains exact evidence plus named canonical
projections. Private or product-specific drivers remain in their owning
repositories. Agent Lab compares their behavior without standardizing their
prompts, native tool registries, checkpoints, filesystem interfaces, or model
SDKs.

## Decision

### Build the broader successor in Agent Lab

Agent Lab is the successor repository. The old `mcp-repl` remains an important
ergonomic reference and may later receive a compatibility-oriented frontend or
migration path, but its MCP-specific repository identity should not define the
new architecture. The evidence now includes an agent driver, a bounded
execution host, an application capability, and paired agent projections; that
scope is materially broader than an MCP REPL.

Do not revive `mcp-repl` in place. Do not split a generic core back into that
repository before a real compatibility consumer exists. Preserve the valuable
behavior—completion, help, pipelines, structured values, persistent sessions,
and human discovery—as product requirements for Agent Lab's interactive
surface.

### Keep five boundaries explicit

1. **Experiment controller.** Owns trial/session identity, driver processes,
   declared limits, observation correlation, evidence bundles, and comparison.
2. **Capability source.** Owns authoritative discovery, invocation, source
   sessions, resources, progress, errors, effect metadata, and source-local
   observation order. MCP is the first adapter, not the generic model.
3. **Execution host.** Owns workspace state and effects such as filesystem,
   shell, terminals, processes, network, and permission enforcement. It may be
   driver-owned or externally supplied, but its limits must be observable.
4. **Agent driver.** Owns one agent implementation's prompts, model, loop,
   native tools, permission UX, context policy, checkpoints, and native event
   taxonomy. The public process protocol owns lifecycle, not those internals.
5. **Surface.** Presents capabilities to a human or agent. Nushell commands,
   agent MCP tools, browser reviews, and future harness-native tools are
   projections, not capability sources.

An evidence plane crosses these boundaries without merging their native event
taxonomies. Every observation retains source identity and source-local order;
the controller may add a global observation order for one trial. Agent-native
events cannot prove capability effects, and capability events cannot explain
the agent's selection policy.

## Why the hybrid is the current default

| Concern | Embedded Nushell | just-bash-style host | Current decision |
| --- | --- | --- | --- |
| Human exploration | Native records, tables, pipelines, variables, help, and future completion | Familiar command strings but no comparable structured-data language | Nushell is the first rich human surface |
| Agent execution | Embedding is possible but carries a large engine and synchronous/async bridge | Small in-process filesystem and shell with explicit limits | just-bash is the first v0 execution-host adapter |
| Capability discovery | Dynamic commands can be added, revised, and hidden in one session | Custom commands are straightforward, but rich live discovery is host work | Capability registry and source events stay outside both |
| Structured values | Native end to end | Usually JSON or command-specific objects around shell text | The source/evidence model stays structured; surfaces choose rendering |
| Sessions | Persistent engine and stack proved | Persistent virtual filesystem and per-run cwd/env are natural | Controller, source, driver, and host sessions remain distinct identities |
| Streaming | Requires an explicit async bridge for synchronous commands | Bounded execution streams are natural to instrument | Source and driver streams are recorded separately |
| Permissions | Shell itself is not the security boundary | Limits and denied operations can be enforced by the host | Source annotations, agent approval, and host enforcement remain separate evidence |
| Embeddability | Rust crates work, but the tested dependency graph contains 319 normal packages | TypeScript library integrates easily with v0 | Rust core; optional Rust Nushell surface; adapters may use their owner's language |
| Reproducibility | Persistent state must be captured explicitly | Virtual state is easy to seed and snapshot | Raw evidence plus named canonicalization, independent of host |

This is a hybrid architecture, not one process that embeds every shell. The
same capability source can be explored through Nushell and exercised by an
agent whose driver happens to use just-bash. Agent Lab correlates the evidence.
It does not port Nushell's data language into just-bash or make Nushell emulate
a virtual machine.

## Supported contracts

### Persistent capability sessions

One capability session may serve multiple discoveries and invocations. Live
descriptor changes, progress, logs, tool errors, and protocol failures retain
distinct typed observations. A surface may cache a catalog, but staleness and
refresh are explicit. The source session is not silently replaced under one
identity.

MCP remains a protocol adapter. Agent Lab should learn from Twill's
authoritative catalog, response profiles, effect lanes, steering, and resource
lifecycle model rather than building a competing MCP authoring framework.
Modern MCP resources, prompts, elicitation, sampling, progress, notifications,
and caching experiments can pressure the same source boundary as tools are
added.

### External agent drivers

The experimental JSON Lines driver protocol supports readiness, one long-lived
process and session, turns, incremental native events, cancellation, completion,
failure, and orderly close. Opaque driver configuration, task input, native
events, and checkpoints remain driver-owned. Exact stdout records and stderr
are retained separately.

The protocol must not absorb v0's `AgentPromptInput`, tool registry,
`AgentFileSystem`, checkpoint schema, prompt variants, or model SDK. A driver
may declare features and expose opaque evidence, but declarations are not proof
of host enforcement.

### Raw and comparable evidence

Raw evidence is immutable input to comparison. A named canonicalization policy
may remove or rewrite explicitly listed unstable fields, producing a separate
projection. Equality of canonical projections never deletes clocks, ids,
provider metadata, process output, or other diagnostic facts from the raw
bundle.

The next protocol revision should make capability observations first-class:
source id, source-local sequence, trial correlation, event kind, and structured
payload. Prefixing an agent-driver event name is sufficient for a spike but not
a durable source contract.

### Shared capability identity across surfaces

The catalog source is projected into Nushell for direct human exploration and
into each external agent session over authenticated MCP. Both projections
retain the source identity and revision while using separate protocol sessions.
The run controller records capability-owned observations independently from
the driver's native events, so agent activity cannot stand in for proof that a
source operation occurred.

Future harness-native projections may present the same source facts with
different selection, compression, or recovery behavior. Those differences are
evaluation input rather than abstraction failure. Agent Lab should add such a
projection only with evidence that the source identity and observations remain
attributable across the boundary.

## State, permissions, and lifecycle

Agent Lab should model bindings among distinct lifecycles rather than inventing
one universal session:

```text
trial
  controller session
  agent-driver process and session
  capability-source session(s)
  execution-host workspace/session
  surface session(s)
```

A binding records which identities participated in one trial and when. Closing
one lifecycle does not imply that every other lifecycle was recreated or
destroyed. Reconnect, replacement, and restore must be visible transitions.

Permissions also have three non-interchangeable layers:

- source effect declarations and preview/recovery contracts;
- agent or host approval policy;
- execution-host enforcement and observed effect.

MCP annotations and command-string hooks are useful metadata, not the security
boundary. A denied background process in the just-bash host is stronger
evidence than a description saying background work is unsupported.

## v0 integration boundary

v0 plugs in as an external driver and remains free to test its own assumptions.
The first adapter proves that a small optional root-agent variant seam is
enough to run the real loop without impersonating a subagent. A root-confined
physical workspace adapter lets the real v0 file and just-bash tools operate on
the controller-owned run workspace without moving the full `IVirtualMachine`
surface into Agent Lab.

No v0 prompt model, checkpoint contract, tool definition, or VM abstraction is
part of this RFC. No public Agent Lab package depends on v0.

## Evaluation model

An evaluation consists of a versioned scenario, seeded host and capability
state, driver identity, model configuration, declared limits, scoring policy,
raw evidence, and named comparison projection. A scripted model proves
plumbing and determinism. Claims about agent quality require repeated real-model
trials.

Useful scores are task-specific. The browser precedent measures task success,
first useful selection, capability calls, native actions, workspace effects,
duration, and any usage or cache data the harness reports. Later scenarios may
add domain-specific measurements, but Agent Lab should provide evaluation
mechanics without prematurely publishing a universal scalar score.

## What remains provisional

- The JSON Lines message names and schemas are experimental.
- Interactive completion has not yet been implemented in the Nushell surface.
- Capability events do not yet have a first-class public envelope.
- Only one real harness has exercised the driver boundary.
- Cross-harness comparison and model-profile matching remain unproven.
- Detach/reconnect, durable checkpoints, caching, and multi-agent scheduling
  remain unproven.

These gaps are boundaries for later steel threads, not reasons to generalize
the current adapters into a framework now.

## Next validation boundary

Run the same catalog scenario through v0 and Eve from one immutable Explore
workspace snapshot. Each harness must use its native loop and execution host,
stream its native events, operate against the same capability revisions, and
produce independently replayable evidence. The workbench should compare task
correctness and observable behavior without forcing the harnesses to share a
tool loop or claiming a universal winner.

This boundary must also make Nushell and the browser two controls for the same
workbench state: shared harness and model selection, shell- or browser-initiated
evaluation, origin-labelled events, aligned visual review, and durable paired
replay. Editor diagnostics, a universal operation registry, and publication of
product-specific adapters remain outside that change.

Repository pushes, pull requests, and public design communication remain
separate approval boundaries.
