# agent-lab

A Rust laboratory for exploring, running, and evaluating agents.

Agent Lab is an experimental home for two related activities:

- interactive, structured exploration of tools and capabilities;
- reproducible execution and evaluation of agents against controlled hosts.

The project begins with MCP and Nushell because they put useful pressure on
discovery, structured data, completion, streaming, and session semantics. The
architecture is intended to admit other capability sources, shell engines,
workspace hosts, and agent implementations without making one of them the core
contract.

## Method

Agent Lab advances through steel threads: small end-to-end slices that test one
architectural hypothesis against real behavior. Each thread records its
acceptance evidence and boundaries before the project generalizes the result.

The initial sequence is:

1. Embed Nushell and operate a modern MCP session without flattening structured
   results.
2. Define a neutral driver boundary for running an agent under controlled
   workspace and capability conditions.
3. Exercise real MCP servers and editor-semantic capabilities.
4. Compare one agent, tool, context, or caching change through repeatable trials.

See [RFC 0001](docs/rfcs/0001-product-thesis.md) for the provisional product
thesis and [RFC 0002](docs/rfcs/0002-steel-thread-method.md) for the working
method. Active experiments are indexed under
[steel threads](docs/steel-threads/README.md).

## Public boundary

This is a public repository. It contains general harness contracts,
open-source integrations, synthetic fixtures, and evidence that is safe to
publish.

Private product adapters, proprietary tool definitions, private source code,
credentials, logs, prompts, and derived evidence stay in their owning private
repositories. They may connect to Agent Lab through a public protocol without
being copied here.

## Status

The first embedded Nushell/MCP steel thread is implemented. A PTY-driven visual
shell feedback loop now makes its prompt, structured rendering, generated help,
live capability refresh, persistent state, and errors directly inspectable
before REPL polish. Names and contracts remain provisional as later steel
threads produce evidence.

## Browser shared perception

Install the web dependencies, then build and start the loopback-only SvelteKit
lab bench with:

```console
$ pnpm install
$ pnpm web:demo
Agent Lab web surface: http://127.0.0.1:…
Local Nushell + fixture MCP sessions; press Ctrl-C to stop.
```

The server fixes its provider to the repository's visual-shell binary and the
synthetic MCP fixture; clients cannot select another executable or configure an
arbitrary MCP server through the gateway. The visual shell is still a full
local Nushell session, including filesystem and external-command capabilities.
Treat access to the workbench as local shell access, and do not expose this
gateway to untrusted or remote clients. Run the browser acceptance with
`pnpm web:test`.
