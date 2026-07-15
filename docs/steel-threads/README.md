# Steel threads

Steel threads are end-to-end experiments that turn Agent Lab's provisional
architecture into inspectable evidence. Their documents state the test before
implementation and record the result afterward.

- [0001: Embedded Nushell with a persistent MCP session](0001-nushell-mcp-session.md)
- [0002: External agent driver with a real v0 loop](0002-external-agent-driver.md)

## Feedback loops

- [Visual shell feedback loop](visual-shell-feedback-loop.md): a PTY-driven
  surface for validating the visible behavior of steel thread 0001 before REPL
  polish.
- [Browser shared perception](browser-shared-perception.md): a loopback-only
  SvelteKit and Ghostty surface over the same real PTY, with semantic and pixel
  browser evidence.
