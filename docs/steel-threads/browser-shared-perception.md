# Browser shared perception

## Hypothesis

Agent Lab can provide a browser surface that a human and an agent can inspect
together without adding another evaluator or weakening the real terminal
boundary established by the visual-shell steel thread. A SvelteKit frontend
should be able to drive the existing Nushell/MCP shell through a child PTY,
render its terminal state through Ghostty, and expose enough semantic screen
state for deterministic browser acceptance.

The implementation timebox is one evidence-producing pull request. The result
is useful only if it reproduces the existing visual-shell acceptance path rather
than stopping at a disconnected terminal mockup.

## Real path exercised

The browser and child process are separated into explicit layers:

```text
SvelteKit lab bench
  -> renderer-neutral TerminalSurface
  -> ghostty-web terminal state and canvas
  -> authenticated same-origin WebSocket
  -> Rust FixtureSessionProvider
  -> operating-system PTY
  -> agent-lab-nushell-mcp-shell --fixture
```

PTY output uses binary WebSocket frames. Browser input is UTF-8 encoded into
binary frames so text frames remain reserved for typed control messages and
session evidence. Initial dimensions travel with the authenticated WebSocket
upgrade and size the PTY before the child starts. Later control messages resize
the live PTY. Evidence events report session start, accepted resizes, exit, and
session failures. This leaves a deliberate extension point for future
capability or agent events without making the terminal renderer the session
model.

`ghostty-web` is behind `TerminalSurface`. Agent Lab depends on its terminal
behavior, not its concrete API. The surface also reads Ghostty's public active
buffer into a visually hidden semantic mirror. Browser tests and assistive tools
therefore observe the same terminal state as the canvas instead of relying on
fixed delays or a second transcript parser.

## Security boundary

The server binds only to `127.0.0.1`. It generates a new random token for every
process, serves that token only from a no-store same-origin endpoint, and
requires both the token and the exact browser `Origin` on WebSocket upgrades.
Host validation prevents a page served under another hostname from acquiring
or using the session. The server adds framing and content-type protections, and
the static Svelte build supplies a hash-based content security policy.

The only session provider in this thread launches the repository's fixture
shell with `--fixture`. That flag fixes the MCP server to the synthetic fixture;
it does not sandbox Nushell. The PTY retains Nushell's filesystem and external
command capabilities, so possession of the browser session is equivalent to
local shell access. The executable cannot be selected through HTTP or
WebSocket input, and this gateway must remain local and trusted.

## Acceptance evidence

The Playwright acceptance drives Ghostty's input element like a human keyboard
and proves all of the following in one live browser session:

1. The banner and prompt arrive from the real child PTY.
2. A mutable Nushell value survives across submissions.
3. A browser viewport change resizes Ghostty and is accepted by the child PTY.
4. A structured MCP catalog pipeline renders `alpha` and `gamma`.
5. Dynamic command help remains available.
6. A capability notification arrives while the prompt is open, and the newly
   added command is refreshed before immediate evaluation.
7. A tool-level error retains Nushell's native diagnostic and returns to a
   usable prompt.
8. The session remains connected after error recovery.

The test asserts semantic terminal state and emits a full-page screenshot. CI
retains the screenshot, trace on failure, and HTML report as the
`browser-steel-thread` artifact. The screenshot is evidence for inspection, not
a cross-platform pixel oracle whose font rendering would vary by runner.

Run the acceptance with:

```console
$ pnpm web:test
```

## Deliberate omissions

This thread does not add arbitrary MCP configuration, selectable shell
executables, sandboxing, remote binding, public hosting, persistent or
reconnectable sessions, a v0 driver, an agent loop, capability inspection UI,
or a general observability schema. Public deployment will require a separate
isolated session provider; it must not be achieved by exposing this loopback
gateway.

The evidence rail intentionally shows only the session lifecycle that the live
path already proves. Richer evidence should be added alongside a real consumer
such as an eval or external agent driver rather than speculated into this PR.
