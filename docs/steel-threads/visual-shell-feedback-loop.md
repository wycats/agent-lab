# Visual shell feedback loop

This is supporting acceptance infrastructure for steel thread 0001, not a new
shell architecture. It gives contributors and agents a real terminal session
in which they can submit Nushell lines, inspect the rendered screen, and check
human-facing behavior before completion, history, editing, or other REPL polish
obscures the underlying contract.

## Boundary

The loop owns only three things:

1. a stable visible prompt on terminal stdin and stdout;
2. one submitted line at a time;
3. MCP declaration refresh between lines.

Nushell still owns parsing, compilation, evaluation, structured values, help,
tables, and error rendering. `NushellHost` still owns the persistent engine and
stack. `McpBridge` still owns the source process, connection, asynchronous
runtime, lifecycle events, and discovery generation.

This separation is important for visual testing. An agent can drive the same
PTY boundary as a human without Agent Lab implementing another data language
or replacing Nushell rendering with test strings. At the same time, Agent Lab
retains one between-line boundary where a changed capability catalog can safely
merge new declarations into the engine that will compile the next line.

## Acceptance evidence

The feedback loop is useful when one PTY session demonstrates all of the
following:

1. The banner, attached namespaces, and `agent-lab>` prompt are visible.
2. A mutable Nushell variable survives across submitted lines.
3. A structured MCP result renders as a native table after a Nushell pipeline.
4. `help` renders the description and signature of a dynamically registered
   MCP command.
5. A tool-list notification produces a visible refresh marker, and the new
   command compiles and runs at the next prompt without recreating the shell or
   MCP session.
6. A tool-level error uses Nushell's native diagnostic rendering and returns to
   a usable prompt.
7. Both an automated PTY driver and a direct interactive run observe the same
   behavior.

The automated gate is:

```console
$ cargo test -p agent-lab-nushell-mcp --test visual_shell
running 1 test
test pty_session_preserves_visible_nushell_and_mcp_behavior ... ok

test result: ok. 1 passed; 0 failed
```

The shell itself is built and started with:

```console
$ cargo build -p agent-lab-nushell-mcp --bins
$ target/debug/agent-lab-nushell-mcp-shell --fixture
Agent Lab visual shell
MCP namespaces: fixture
Nushell evaluates each submitted line; `exit` leaves.
agent-lab>
```

It can instead attach one arbitrary stdio server:

```console
$ target/debug/agent-lab-nushell-mcp-shell \
    --mcp everything npx -y @modelcontextprotocol/server-everything@2026.7.4 stdio
```

## Direct observation

A direct PTY run rendered the fixture catalog as a box-drawn Nushell table. A
pipeline over the structured catalog rendered two rows, `alpha` with score `3`
and `gamma` with score `8`. After `tool fixture enable_extra {}`, the screen
showed:

```text
[capabilities refreshed: fixture]
agent-lab>
```

At that prompt, `help tool fixture extra` rendered the live description, usage,
parameter, and input/output sections, and `tool fixture extra {}` rendered an
`available: true` record. `tool fixture fail {}` rendered a source-anchored
Nushell diagnostic with the stable title `MCP tool failed` and the fixture's
structured error detail, then returned to the prompt. A mutable `answer`
variable advanced from `41` to `42` across separate submissions. `exit` ended
the child with status 0.

## Dependency pressure

The first rendering attempt used `nu-cli::eval_source`. That reached the right
visual behavior but added 152 lockfile packages and enabled the full OS-facing
Nushell CLI dependency surface. The final implementation instead uses
`PipelineData::print_table` and Nushell's diagnostic reporters, which were
already available through the embedded crates. The normal dependency graph
therefore remains at the 319 package names recorded by steel thread 0001.

The PTY regression test adds `expectrl` only as a development dependency. Its
15 lockfile packages are test infrastructure and do not enter the normal shell
runtime.

## Deliberate omissions

This loop does not yet provide line editing, tab completion, history,
multiline submission, interrupt handling, resize testing, a browser terminal,
or pixel snapshots. Those are product and presentation questions, while this
loop establishes that the underlying visible semantics are directly
inspectable.

When cursor movement, responsive layout, or browser embedding becomes the
active question, place an xterm-compatible surface over this same PTY and add
screen snapshots there. Do not build a second evaluator or bypass the terminal
boundary to make those tests easier.
