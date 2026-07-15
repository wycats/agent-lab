# Steel thread 0002: External agent driver with a real v0 loop

- Status: Active
- Depends on: [Steel thread 0001](0001-nushell-mcp-session.md)

## Question

Can Agent Lab run a real agent implementation as a long-lived external driver,
stream its native behavior into reproducible evidence, and give it access to
the same capability source used for human exploration—without making either
side adopt the other's prompts, tool taxonomy, runtime, or internal types?

## Hypotheses

1. A small versioned process protocol can preserve driver identity, session and
   turn identity, incremental events, cancellation, completion, and failure
   without encoding one agent framework's loop or message types.
2. The real v0 loop can implement that protocol in its owning repository while
   all v0 prompt construction, model adapters, tool selection, and checkpoints
   remain private to the driver.
3. The deterministic MCP fixture from steel thread 0001 can be explored through
   Nushell and invoked through the v0 driver while retaining the same capability
   identity and structured arguments, results, and lifecycle evidence.
4. Agent Lab can retain raw driver output and derive a separate canonical view
   for comparison, so repeatability does not require discarding provenance or
   unstable-but-diagnostic fields.

The thread should discover the smallest boundary supported by both the Rust
controller and the real driver. The candidate messages below are inputs to the
experiment, not a stable public API.

## Real path

The public repository will contain a Rust protocol experiment, child-process
controller, and deterministic driver fixture. The real adapter will live with
v0 and use the actual v0 agent loop rather than a second agent implementation.
Cross-repository acceptance will pin both revisions and record only evidence
safe for the public repository.

The real path will reuse:

- the MCP fixture and typed lifecycle evidence from steel thread 0001;
- v0's existing deterministic agent-loop harness and event observation seam;
- v0's existing MCP-to-dynamic-tool behavior;
- the bounded in-process shell host used by the v0 harness as one execution-host
  implementation, not as the Agent Lab driver contract.

This pairing is deliberate. The public fixture proves the protocol precisely;
the v0 adapter proves that the protocol survives a demanding agent with its own
tool, prompt, filesystem, and evidence assumptions.

## Candidate process semantics

The first candidate is newline-delimited JSON over stdio. Protocol output owns
stdout; driver diagnostics use stderr. Every parsed message carries a protocol
version and type. Session-, turn-, message-, and source-local sequence identity
are present where applicable.

Controller commands need only express:

- opening a session with an opaque driver configuration and declared limits;
- starting a turn with a task and capability-source configuration;
- aborting an active turn;
- closing the session.

Driver output needs only express:

- readiness and a versioned driver descriptor;
- session acceptance;
- an incremental native event with an opaque structured payload;
- turn completion with an outcome and evidence references;
- driver or protocol failure.

Agent Lab records the exact input and output bytes alongside the parsed
envelopes. Canonicalization is a named evidence transform, never a mutation of
the raw record. A driver may expose an opaque checkpoint, but Agent Lab does not
define its schema.

## Timebox and stopping conditions

Stop the first pass when the public fixture and pinned v0 adapter have either
met every acceptance check or produced a reproducible failure at a named
boundary. Do not expand this pass into a general RPC framework, production
scheduler, remote model service, or editor integration.

Pause and record a negative result if the real path requires any of:

- serializing v0's internal agent input, prompt, tool, or checkpoint types into
  the public protocol;
- importing v0 or TypeScript runtime code into Agent Lab;
- importing Agent Lab, Nushell, or the Rust MCP bridge into v0;
- adopting AI SDK Harness, MCP, or any agent SDK as the generic driver wire
  protocol;
- reducing native events to display strings or retaining only normalized
  evidence;
- letting the driver silently widen execution permissions or limits;
- hiding a reconnect, runtime replacement, or new process behind one session
  identity.

## Acceptance evidence

The thread is successful only when repository and cross-repository evidence
demonstrates all of the following:

1. A public fixture driver completes protocol negotiation, preserves one
   process and session across at least two turns, streams events before turn
   completion, and shuts down cleanly.
2. The controller distinguishes malformed protocol output, driver-reported
   failure, turn cancellation, unexpected process exit, and successful
   completion.
3. A pinned v0 adapter runs the real v0 loop through the same protocol. The
   public wire contains no v0 prompt, tool-registry, filesystem, or agent-input
   types.
4. One scripted v0 trial exercises multiple native v0 tools and produces a raw
   event stream, final workspace evidence, finish/usage evidence, and an opaque
   checkpoint or explicit statement that checkpointing is unsupported.
5. The v0 driver discovers and invokes at least one structured tool from the
   steel-thread-0001 MCP fixture. The evidence correlates driver invocation with
   the fixture's capability identity, arguments, result, and lifecycle events.
6. At least one denied or unsupported operation proves that the declared
   execution-host permissions and limits are enforced rather than descriptive.
7. Two equivalent runs preserve their raw records and produce equal canonical
   evidence. The transform and every removed or rewritten field are named.
8. The evidence records exact repository revisions, driver and dependency
   versions, process and runtime counts, protocol transcripts, permission
   decisions, observed failures, and the architectural conclusion.

## Non-goals

This thread does not yet provide:

- a stable public driver SDK;
- a production v0 integration or published private adapter;
- a universal task, prompt, model, tool, or checkpoint schema;
- a shared virtual filesystem or shell API;
- code-server, LSP, browser, or other editor/application capabilities;
- statistical eval scoring or a benchmark suite;
- detach and reconnect across controller processes.

## Architectural pressure

### Driver events are not capability events

An agent's native event stream explains what the loop believed and emitted.
Capability and execution-host events explain what effects were requested,
allowed, performed, or rejected. Agent Lab must correlate these streams without
pretending they share one native taxonomy. Evidence envelopes therefore need
source identity and source-local order before a unified observation order can
be derived.

### Determinism is a projection

Real agent events contain identifiers, clocks, provider metadata, and other
values that can vary while behavior remains equivalent. Removing those fields
can support comparison, but cannot be the only retained evidence. The thread
must make the canonicalization policy reviewable and preserve the raw input
that produced it.

### The first execution host belongs to the driver

The initial v0 path may use its bounded in-process shell and filesystem because
that is the shortest real route through the agent loop. The driver must declare
that host and its unsupported features. Moving the workspace host behind an
Agent Lab boundary is a later steel thread; the process protocol must not make
the initial host permanent.

### Capability configuration is not a universal tool schema

The first real trial uses MCP because both current experiments can reach it.
The generic driver envelope carries structured, driver-interpreted capability
configuration and stable source identity; it does not redefine MCP tools or AI
SDK tools. A future driver may reach the same capability through another
adapter without changing the controller lifecycle.

## Exact implementation boundary

The public implementation begins with one experimental Rust crate containing:

1. serde types for the candidate envelopes;
2. an incremental JSON Lines codec that retains raw records;
3. a child-process controller with explicit lifecycle and cancellation;
4. a deterministic fixture driver and integration tests for success, streaming,
   malformed output, failure, cancellation, and process exit;
5. an evidence bundle containing pinned identities, raw transcripts, parsed
   envelopes, and a named canonical projection.

In parallel, the v0 repository should add only the adapter needed to place its
existing deterministic harness behind this process boundary and inject the
steel-thread-0001 MCP fixture through v0's own dynamic-tool path. Any cleanup
needed to avoid treating the harness as a subagent belongs in v0 and should be
reviewed there.

The cross-repository acceptance run is the gate for extracting a reusable Agent
Lab driver contract. Until that run passes, the Rust types and message names
remain experimental and must not move into `agent-lab-core`.

## Evidence log

### Public process fixture

The first Rust pass implements the candidate envelopes, an incremental JSON
Lines child-process controller, and a deterministic fixture driver in the
experimental `agent-lab-driver-protocol` crate.

```console
$ cargo test -p agent-lab-driver-protocol --test process_protocol
running 3 tests
test one_process_streams_two_turns_and_cancels_the_second ... ok
test malformed_output_reported_failure_and_process_exit_are_distinct ... ok
test protocol_version_and_sequence_violations_are_distinct ... ok

test result: ok. 3 passed; 0 failed
```

Observed behavior:

- the fixture's protocol-reported process ID matches the controller's child
  process ID;
- one process and session completes one turn, begins a second, receives an
  explicit abort, reports the aborted outcome, and exits successfully after
  `session.close`;
- turn events arrive as individual records before `turn.finished`;
- every sent and received record retains its exact bytes, including the JSON
  Lines delimiter, alongside the parsed envelope;
- driver-reported turn failure remains a parsed `driver.failed` message;
- malformed JSON, protocol version 2, a repeated source sequence, and process
  exit code 17 produce four distinct controller errors;
- stderr is captured separately and poisoned diagnostic locks recover without
  panicking the trial controller.

The focused strict-Clippy gate also passes:

```console
cargo clippy -p agent-lab-driver-protocol --all-targets --all-features -- -D warnings
```

This is partial evidence only. The crate does not yet produce a complete
evidence bundle or canonical projection, and no real agent driver or MCP
capability has crossed the protocol. The message names therefore remain
candidate vocabulary rather than an architectural conclusion.
