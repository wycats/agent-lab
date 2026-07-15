# Steel thread 0001: Embedded Nushell with a persistent MCP session

- Status: Validated (first pass)

## Question

Can a long-lived embedded Nushell host make tools from a modern MCP session
feel native to structured human exploration without forcing Agent Lab's core
model to become either a Nushell engine or an MCP client?

## Hypotheses

1. One Nushell engine and stack can evaluate multiple commands while preserving
   shell state and accepting newly discovered commands between evaluations.
2. One MCP client session can serve synchronous Nushell commands without
   creating a Tokio runtime, process, or protocol connection for each call.
3. MCP structured tool results can remain typed Nushell records, lists, and
   scalars through native pipelines rather than becoming display strings.
4. Discovery changes, progress, logs, and failures can be recorded as typed
   lifecycle events independently of how the shell renders them.

The thread should falsify a hypothesis rather than hide a failed boundary
behind a broader framework.

## Real path

The experiment uses the current published Nushell crates and the official Rust
MCP SDK. It embeds Nushell through `EngineState`, `Stack`, `StateWorkingSet`, the
parser, and the evaluation engine. It connects over MCP stdio to both:

- a deterministic public fixture that can expose structured results, persistent
  state, progress, logs, failures, and a changing tool list;
- one real open-source MCP server selected for structured, non-trivial output.

The fixture is contract evidence. The real server checks that the same path is
not overfit to the fixture.

## Timebox and stopping conditions

Stop the first implementation pass when the deterministic fixture has either
met every acceptance check below or produced a reproducible failure at one of
the named boundaries. Do not expand the pass into REPL presentation, a general
agent API, virtual filesystem design, or exhaustive JSON Schema mapping.

Pause and record a negative result if satisfying the checks requires any of:

- a new Tokio runtime or MCP connection per tool call;
- rebuilding the Nushell engine for each evaluation;
- flattening structured results to JSON or display strings;
- hiding protocol lifecycle events inside frontend-specific state;
- unsafe code or a private integration.

## Acceptance evidence

The thread is successful only when its repository evidence demonstrates all of
the following:

1. Two evaluations reuse the same Nushell engine and preserve a shell variable.
2. At least two tool calls reuse one fixture process, MCP connection, and async
   runtime; a stateful fixture tool proves session continuity.
3. Tool discovery produces structured records and registers names that Nushell
   can parse as commands in the same session.
4. A structured tool result survives a native pipeline that filters or selects
   nested fields.
5. A tool-list change marks discovery stale and a refresh makes the changed
   command set available without recreating the shell or MCP session.
6. Progress, structured logs, tool-level errors, and protocol failures remain
   distinguishable in the captured event stream.
7. The same client and shell path lists and invokes one real open-source MCP
   server.
8. Evidence records exact dependency versions, commands, transcripts, process
   and runtime counts, observed failures, and the final architectural
   conclusion.

## Non-goals

This thread does not yet provide:

- a polished interactive REPL or final completion UI;
- a complete JSON Schema-to-Nushell signature mapping;
- virtual filesystem, network policy, or permission enforcement;
- a stable capability-source or agent-driver API;
- v0 integration or a just-bash compatibility layer;
- MCP conformance beyond the behavior directly exercised here.

## Architectural pressure

Nushell commands are synchronous while MCP clients and lifecycle notifications
are asynchronous. The experiment therefore needs an explicit bridge. The
current candidate is one background MCP runtime that owns its sessions and
accepts typed requests from synchronous shell commands. That bridge is an
implementation hypothesis, not yet an Agent Lab core contract.

Dynamic discovery creates a second pressure: commands are declarations in a
Nushell engine, while MCP tool lists can change during a session. The experiment
must show whether merging new declarations is sufficient and what semantics are
possible when tools disappear or change schema.

## Evidence log

### Environment and dependency surface

The validated run used:

- `rustc 1.97.0 (2d8144b78 2026-07-07)` and Cargo 1.97.0;
- Nushell crates 0.114.1 (`nu-cmd-lang`, `nu-command`, `nu-engine`,
  `nu-parser`, and `nu-protocol`);
- the official Rust MCP SDK crate `rmcp` 2.2.0;
- Node 22.22.2 and npm 11.11.1 for the real-server probe;
- `@modelcontextprotocol/server-everything@2026.7.4` over stdio.

`cargo tree -p agent-lab-nushell-mcp --edges normal --prefix none` contains 319
unique normal-dependency package names. The first complete test build took
27.68 seconds on the development machine. That cost is acceptable for an
optional rich frontend experiment, but is evidence against making the Nushell
engine Agent Lab's core runtime.

### Deterministic fixture

The fixture and tests run through the public `rmcp` client and server paths over
child-process stdio. The fixture exposes structured values, process-local
state, progress, a structured log, tool and protocol failures, and live tool
addition, descriptor revision, and removal.

```console
$ cargo test -p agent-lab-nushell-mcp --test steel_thread
running 3 tests
test persistent_nushell_and_mcp_session_preserve_structured_behavior ... ok
test lifecycle_and_discovery_changes_remain_observable ... ok
test synchronous_bridge_calls_are_safe_inside_a_tokio_context ... ok

test result: ok. 3 passed; 0 failed
```

The tests directly establish:

- two evaluations retain a mutable Nushell variable;
- multiple tool calls report the same fixture PID and bridge runtime ID while a
  session-local counter advances from 1 to 2;
- `tool fixture catalog | get items | where active | get name` returns the
  native string list `alpha`, `gamma`;
- `mcp fixture tools` yields structured discovery records;
- one lifecycle call produces two `mcp.progress` events and one `mcp.log`
  event;
- tool-level failure returns `isError: true`, while protocol failure produces a
  failed request and a distinct `mcp.tool.protocol_failed` event;
- a tool-list notification marks discovery stale, after which refresh can add
  a declaration, replace it when its descriptor changes, and hide it when the
  tool disappears—all without replacing the engine, fixture process, bridge
  runtime, or MCP connection.

After removal, Nushell reports `RunExternalNotFound` as a compile error for the
formerly registered multiword command. The host preserves that native failure
classification rather than translating it into an MCP error.

### Real server

The same `McpBridge` and `NushellHost` path connects to the official
[Everything Server](https://github.com/modelcontextprotocol/servers/tree/main/src/everything),
discovers 13 tools, and invokes its structured-content tool through a native
Nushell pipeline:

```console
$ cargo run -p agent-lab-nushell-mcp --example everything
{
  "runtimeId": 1,
  "toolCount": 13,
  "resultType": "record<temperature: int, conditions: string, humidity: int>",
  "result": "Record({\"temperature\": Int(33), \"conditions\": String(\"Cloudy\"), \"humidity\": Int(82)})",
  "eventKinds": [
    "mcp.tools.changed",
    "bridge.ready",
    "mcp.tools.listed",
    "mcp.tools.listed",
    "mcp.tool.started",
    "mcp.tool.completed"
  ]
}
```

The first real-server attempt used `Portland`; the server rejected it against
its published location enum. Changing the argument to `New York` succeeded.
That failure confirms the probe crosses the real server's schema-validation
path rather than returning fixture-shaped data locally.

The server emitted `tools/list_changed` during startup before the bridge
recorded readiness. Consumers therefore cannot infer that lifecycle events
begin only after a frontend considers a session ready; ordering must come from
the event sequence itself.

### Integration pressure observed

- Nushell commands are synchronous. One background thread with one persistent
  multi-thread Tokio runtime owns the MCP session and accepts typed requests
  from commands. No runtime, child process, or connection is created per call.
- `rmcp` moves incoming request metadata into `RequestContext.meta` before
  invoking a server handler. Progress tokens must be read from that context,
  not from the request parameter object.
- MCP logging types are deprecated in `rmcp` 2.2.0 as the protocol evolves.
  This experiment retains narrowly scoped deprecation allowances so the
  compatibility pressure is visible instead of globally suppressing it.
- Nushell's public evaluation and command APIs return the large `ShellError`
  type by value. The experimental crate has one documented, crate-local
  `clippy::result_large_err` allowance rather than weakening workspace lints.
- Cargo reports that transitive `proc-macro-error2` 2.0.1 contains code a
  future Rust version will reject. It is upstream dependency pressure, not a
  current test or lint failure.

### Architectural conclusion

The final workspace gate passed with:

```console
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
git diff --check origin/main...HEAD
```

All four hypotheses are supported by this pass. Embedded Nushell is viable as
a high-value structured exploration host: it preserves session state, native
values, pipelines, dynamic commands, and help while MCP continues to own
capability discovery and invocation. The explicit synchronous-to-asynchronous
actor boundary is tractable and reusable.

The evidence does **not** support making Nushell, MCP, or this coupled
experimental crate the core harness abstraction. Nushell's dependency weight
and frontend-specific declaration lifecycle belong behind an optional host
adapter. The ordered event log and structured discovery/call values are better
candidates for a shared boundary, but they should remain provisional until an
agent driver exercises them from the other side.

### Exact next boundary

Do not add REPL polish or extract a stable general framework from this thread.
The next steel thread should drive one live capability session through a
neutral external agent-driver port with only the operations this evidence
requires: list structured descriptors, invoke with structured arguments,
receive structured results, observe ordered lifecycle events, retain session
identity, and react to stale discovery.

That thread should pair a public deterministic driver fixture with an early v0
adapter in v0's owning environment. Agent Lab must not adopt v0's prompt model,
tool taxonomy, or virtual-machine interface, and v0 must not depend on Nushell
or this MCP bridge. The paired implementation should reveal the smallest real
contract before either repository treats it as stable.
