# RFC 0003: First evidence-backed architecture

- Status: Candidate
- Evidence: [steel thread 0001](../steel-threads/0001-nushell-mcp-session.md), [steel thread 0002](../steel-threads/0002-external-agent-driver.md), [steel thread 0003](../steel-threads/0003-editor-diagnostics.md)

## Summary

Agent Lab should be a Rust-first experiment controller and evidence system with
independent capability sources, execution hosts, agent drivers, and human
surfaces. It should not be a shell runtime, MCP framework, v0 extraction, or
universal virtual machine.

The first three steel threads support a hybrid product shape. Embedded Nushell
is the first rich human surface because it preserves structured exploration,
state, pipelines, and dynamically discovered commands. A just-bash-backed v0
host is the first bounded agent execution host because it is small, in-process,
and easy to instrument. Both consume the same capability facts, but neither is
the core contract.

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
   MCP dynamic tools, v0's native LSP tool, and a future eval runner are
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

### Multiple projections of one capability

One capability may have different agent-native projections. Steel thread 0003
presented one diagnostic source as an MCP dynamic tool and as v0's built-in LSP
tool. The source snapshot was identical; v0's retained context was not. The
dynamic projection kept the full structured snapshot, while the native path
compressed it to a count after the immediate turn.

That difference is evaluation input, not abstraction failure. Agent Lab should
measure selection, task success, recovery, retained bytes, and token/cache use
across projections. It must preserve capability-owned observations so a native
tool does not disappear from attribution merely because it bypassed the
driver's third-party-tool set.

A current multi-client capability ([v0 PR 26616](https://github.com/vercel/v0/pull/26616))
provides a second portability shape: web and CLI surfaces can execute the full
local handler while a native surface intentionally renders a structured
continuation to the web surface. Agent Lab should not classify that as a
missing capability or force every client to implement identical behavior.
Capability identity and intent remain shared; renderer choice, local support,
and cross-surface continuation belong to the surface or driver projection.

The evidence model therefore needs an explicit handoff outcome with the target
surface, reason, resumable state, and source capability identity. A handoff is
neither tool success with hidden follow-up nor tool failure. This gives the
driver harness a concrete portability scenario: compare full execution and
intentional continuation without changing the capability-source contract.

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
enough to run the real loop without impersonating a subagent. It also proves
that semantic services can be injected into the existing filesystem boundary
without moving the full `IVirtualMachine` surface into Agent Lab.

The editor spike found a real internal gap: v0 defines semantic LSP requests,
the remote filesystem implements them, and the Git VM/code-server filesystem
currently returns unsupported. A narrow v0 cleanup can improve that boundary,
but Agent Lab should not choose whether semantic services permanently belong
under `AgentFileSystem`. The paired harness is specifically where an extracted
`AgentSemanticServices` interface can be compared against the current design
without blocking on local-sandbox or ongoing VM simplification work.

No v0 prompt model, checkpoint contract, tool definition, or VM abstraction is
part of this RFC. No public Agent Lab package depends on v0.

## Evaluation model

An evaluation consists of a versioned scenario, seeded host and capability
state, driver identity, model configuration, declared limits, scoring policy,
raw evidence, and named comparison projection. A scripted model proves
plumbing and determinism. Claims about agent quality require repeated real-model
trials.

Useful scores are task-specific. The browser precedent measures task success,
first useful selection, semantic-fallback violations, and forbidden backend
effects. The editor slice adds post-edit diagnostic absence, unnecessary
file/shell operations, retained diagnostic bytes, turns, and token/cache use.
Multi-client scenarios add correct renderer selection, preservation of
resumable state, and successful continuation on the target surface. Agent Lab
should provide evaluation mechanics without prematurely publishing a universal
scalar score.

## What remains provisional

- The JSON Lines message names and schemas are experimental.
- Interactive completion has not yet been implemented in the Nushell surface.
- Capability events do not yet have a first-class public envelope.
- The deterministic editor source is not a real VS Code Problems adapter.
- The paired diagnostic run proves plumbing, not a model-quality improvement.
- Workspace/execution-host extraction has not been attempted.
- Detach/reconnect, durable checkpoints, caching, and multi-agent scheduling
  remain unproven.

These gaps are boundaries for later steel threads, not reasons to generalize
the current adapters into a framework now.

## Product sequencing update

RFC 0001 now identifies a two-harness workbench using v0 and Eve as the next
product-validation boundary. That slice should establish the interactive
learning loop and demonstrate that the harness boundary works for two real
implementations before Agent Lab deepens one semantic-capability comparison.

The diagnostic adapter below remains the next architecture-validation boundary
for semantic editor capabilities. This sequencing update does not change the
architecture or evidence requirements described by this RFC; it places them
inside the product loop established by RFC 0001.

## Exact next implementation boundary

Build one real VS Code/code-server diagnostic adapter behind the public
diagnostic snapshot contract. The extension uses
`vscode.languages.getDiagnostics()` and a bounded local transport. It must
prove workspace binding, pending and settled revisions, cancellation, timeout,
extension restart, no-listener behavior, and a repair followed by a settled
empty snapshot.

Run the same seeded TypeScript task through both v0 projections with identical
source observations. First use the scripted model to pin the evidence, then run
repeated real-model trials. Do not add rename-symbol, generic VS Code command
execution, a universal workspace host, or a stable public driver SDK in that
change.

After that evidence, decide two questions before proposing the v0 work or
promoting public contracts:

1. Should semantic services remain a facet of `AgentFileSystem`, or become a
   separate injected interface?
2. What diagnostic facts must v0 retain after its native LSP compression step
   to preserve recovery without keeping the full snapshot in agent context?

Repository pushes, pull requests, and public design communication remain
separate approval boundaries.
