# Steel thread 0002: Neutral external agent driver and evidence

- Status: Validated (public foundation)
- Depends on: [Steel thread 0001](0001-nushell-mcp-session.md)

## Question

Can Agent Lab start and observe an agent harness through a small process
boundary while leaving the harness's prompts, tools, model loop, context
policy, workspace host, checkpoints, and native event vocabulary under the
harness's control?

The boundary must preserve enough raw and structured evidence to replay and
compare a run. It must not become a universal agent SDK before two real
harnesses have tested the vocabulary.

## Hypotheses

1. A versioned JSON Lines protocol can preserve driver identity, session and
   turn identity, incremental native events, cancellation, completion, and
   failure without importing one harness's internal types.
2. Agent Lab can retain the exact controller, driver, and stderr streams while
   deriving a separate named projection for deterministic comparison.
3. An explicit executable, argument, working-directory, and environment
   launch description is enough for an owning repository to attach a real
   harness without Agent Lab choosing its runtime or tool adapter.
4. A synthetic fixture can establish process and evidence invariants. Real
   v0 and Eve integrations must separately establish that the boundary
   survives contact with their native loops.

The names and shapes in this thread remain experimental. The evidence supports
a seam worth testing, not a stable public driver API.

## Public boundary

The public repository contains:

- candidate protocol envelopes;
- a child-process controller;
- a deterministic driver fixture;
- raw transcript capture;
- a named canonical comparison projection;
- an atomic, reopenable evidence directory;
- synthetic tests for success and failure behavior.

Public harnesses such as Eve may implement the process boundary here or in
their own repository. The v0 adapter, prompts, tool definitions, and evidence
remain in v0's owning repository. Agent Lab does not require either harness to
adopt the other's tool loop, native event taxonomy, or workspace abstraction.

## Candidate process semantics

The controller and driver exchange newline-delimited JSON over stdio. Protocol
output owns stdout; diagnostics use stderr. Every message declares a protocol
version and type. Controller commands also carry a message identity, and
driver messages carry a contiguous source-local sequence and optional causal
message identity.

Controller commands express only:

- opening a session with opaque driver configuration and declared limits;
- starting a turn with an opaque task and capability-source configuration;
- aborting an active turn;
- closing the session.

Driver output expresses only:

- readiness and a versioned driver descriptor;
- session acceptance;
- an incremental native event with an opaque structured payload;
- turn completion with an outcome and opaque evidence;
- driver, protocol, session, or turn failure;
- session closure.

The controller validates framing, protocol version, and driver-local sequence
while retaining the exact bytes it observed. It does not reinterpret native
events as Agent Lab tool events.

## Durable evidence

A successful protocol transcript can be finalized as an evidence directory:

```text
<run>/
  manifest.json
  controller.jsonl
  driver.jsonl
  driver.stderr.log
  canonical.json
```

The manifest records the evidence schema, controller revision, driver
descriptor, process identity, canonicalization policy, record counts, and
stderr byte count. The JSON Lines files contain the exact protocol records,
including their delimiters. Driver stderr remains separate.

`canonical.json` is derived from the retained driver records using a named
policy. The first policy can remove explicitly named object keys recursively;
it does not mutate or replace the raw transcript. Finalization writes a new
staging directory and renames it into place. Reopening validates the manifest,
parses every typed record, and reproduces the canonical projection. An existing
target or a tampered projection is rejected.

This directory is the driver-level part of run evidence. Workspace snapshots,
capability observations, model usage, scores, and scenario identity belong to
the run controller that will compose it.

## Timebox and stopping conditions

The public-foundation pass stops after one synthetic driver proves the process
lifecycle and durable evidence invariants. It does not add a scheduler, remote
transport, workspace controller, MCP adapter, browser UI, or model service.

Record a negative result and revisit the seam if a real harness requires:

- serializing its prompt, tool, checkpoint, or agent-input types into the
  public protocol;
- importing the harness runtime into Agent Lab;
- adopting MCP, AI SDK Harness, or another agent framework as the generic
  driver wire format;
- flattening native events to display strings;
- retaining only normalized evidence;
- silently widening permissions or execution limits;
- hiding a process replacement behind one session identity.

## Acceptance evidence

The public foundation is successful when synthetic evidence demonstrates:

1. One fixture process opens one session, streams multiple events before turn
   completion, runs a second turn, handles explicit cancellation, closes, and
   exits cleanly.
2. The controller distinguishes malformed JSON, a driver-reported failure, an
   unsupported protocol version, a repeated driver sequence, timeout, and
   unexpected process exit.
3. Controller records, driver records, and stderr are retained separately and
   exactly.
4. Two equivalent fixture runs keep distinct raw process identities while a
   named projection compares equal after removing only the declared field.
5. A finalized evidence directory reopens without a live driver and reproduces
   the same bundle.
6. Re-finalization over an existing target and reopening a tampered projection
   both fail rather than silently changing evidence.
7. The crate passes formatting, tests, and strict Clippy checks as part of the
   full Agent Lab workspace.

## Non-goals

This thread does not provide:

- a stable driver SDK or universal harness schema;
- a production v0 or Eve integration;
- a universal prompt, model, tool, capability, or checkpoint model;
- a run controller, workspace host, or scenario manifest;
- authorization or credential redaction policy;
- detach and reconnect across controller processes;
- code-server, LSP, browser, or editor integration;
- statistical evaluation or benchmark scoring.

## Architectural conclusions

### Driver events and capability events remain distinct

The driver stream explains what the harness emitted. Capability and execution
host streams explain what effects were requested, allowed, performed, or
rejected. A later run controller should correlate those sources without
pretending they share one native taxonomy.

### Determinism is a projection

Process identities and future model/provider metadata vary even when behavior
is equivalent. Comparison therefore needs a reviewable transform, but raw
evidence remains authoritative. A score or canonical stream cannot replace the
observations that produced it.

### The harness retains its execution host

The protocol carries opaque configuration rather than a shared filesystem or
shell interface. A real driver may use a physical workspace, a virtual
filesystem, a remote sandbox, or its own VM abstraction. Workspace identity
and effects become run-level evidence without making the driver protocol own
the host.

### Capability configuration is not a universal tool schema

The controller can pass structured capability-source configuration while the
driver decides how to project it into its native loop. MCP is the first real
capability protocol, not the generic driver contract.

## Evidence log

The experimental `agent-lab-driver-protocol` crate exercises the complete
public-foundation boundary:

```console
$ cargo test -p agent-lab-driver-protocol --all-features
running 16 tests
test clean_exit_drains_queued_stdout_before_transcript_capture ... ok
test clean_exit_drains_trailing_stderr_before_transcript_capture ... ok
test clean_exit_terminates_descendants_that_hold_reader_pipes ... ok
test dropping_a_driver_terminates_its_process_group ... ok
test durable_evidence_reopens_and_rejects_tampering ... ok
test eof_from_a_running_driver_respects_the_receive_timeout ... ok
test evidence_rejects_protocol_and_manifest_identity_mismatches ... ok
test malformed_output_reported_failure_and_process_exit_are_distinct ... ok
test one_process_streams_two_turns_and_cancels_the_second ... ok
test oversized_driver_records_are_bounded_before_buffering ... ok
test probe_can_finalize_fixture_evidence_for_direct_inspection ... ok
test probe_rejects_a_nonzero_driver_exit_before_finalizing_evidence ... ok
test probe_rejects_completion_for_an_unexpected_turn ... ok
test protocol_version_and_sequence_violations_are_distinct ... ok
test raw_runs_remain_distinct_while_named_canonical_evidence_matches ... ok
test unterminated_driver_records_are_rejected_before_parsing ... ok

test result: ok. 16 passed; 0 failed
```

The focused strict-Clippy gate also passes:

```console
$ cargo clippy -p agent-lab-driver-protocol --all-targets --all-features -- -D warnings
Finished `dev` profile
```

The tests confirm that the fixture-reported PID matches the child process,
incremental events precede completion, cancellation stays within the same
process and session, protocol failures remain distinct, and raw records retain
their JSON Lines delimiters. A clean process exit joins both reader threads so
trailing stderr and queued stdout are present before transcript capture;
descendants that inherit those pipes are terminated within the same bounded
lifecycle. Oversized and unterminated frames fail before unbounded retention.
Two fixture processes produce different raw records because their PIDs differ, while the
`fixture-v1` projection matches after removing the explicitly named
`processId` field. The finalized directory
can be reopened byte-for-byte, and canonical tampering is detected.

The probe provides the same boundary for direct inspection. Setting
`AGENT_LAB_EVIDENCE_DIR` finalizes the run there; an optional
`AGENT_LAB_CANONICAL_POLICY_JSON` names the comparison transform. The probe
retains its inputs exactly, so this experimental command is intended for
synthetic or otherwise publishable inputs until the run controller owns a
credential-redaction policy.

## Exact next boundary

Bind one real harness to this experimental seam while a run controller owns a
controlled workspace, capability-source identity, lifecycle events, and the
larger evidence bundle. Use the current catalog-to-file scenario as the first
vertical slice.

The public path should use Eve or another open harness. The v0 path should use
the same protocol from v0's owning repository. The pair should test whether
the candidate messages preserve meaningful native differences before any type
moves into `agent-lab-core` or is described as stable.
