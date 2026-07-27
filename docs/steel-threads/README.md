# Steel threads

Steel threads are end-to-end experiments that turn Agent Lab's provisional
architecture into inspectable evidence. Their documents state the test before
implementation and record the result afterward.

- [0001: Embedded Nushell with a persistent MCP session](0001-nushell-mcp-session.md)
- [0002: Neutral external agent driver and evidence](0002-external-agent-driver.md)
- [0003: Catalog feedback loop](0003-catalog-feedback-loop.md)
- [0004: Two-harness workbench](0004-two-harness-workbench.md)
- [0005: Manual evaluation promotion](0005-manual-evaluation-promotion.md)
- [0006: Assisted evaluation proposal](0006-assisted-evaluation-proposal.md)

## Feedback loops

- [Visual shell feedback loop](visual-shell-feedback-loop.md): a PTY-driven
  surface for validating the visible behavior of steel thread 0001 before REPL
  polish.
- [Browser shared perception](browser-shared-perception.md): a loopback-only
  SvelteKit and Ghostty surface over the same real PTY, with semantic and pixel
  browser evidence.
