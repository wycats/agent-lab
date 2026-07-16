# agent-lab

Agent Lab is an open workbench for understanding and improving agent harnesses.
It lets builders explore the same workspaces and capabilities their agents use,
run real harnesses under controlled conditions, and preserve what they learn as
repeatable evaluations backed by inspectable evidence.

The project begins with MCP and Nushell because they put useful pressure on
discovery, structured data, completion, streaming, and session semantics. The
architecture is intended to admit other capability sources, shell engines,
workspace hosts, and agent implementations without making one of them the core
contract.

## Method

Agent Lab advances through steel threads: small end-to-end slices that test one
architectural hypothesis against real behavior. Each thread records its
acceptance evidence and boundaries before the project generalizes the result.

See [RFC 0001](docs/rfcs/0001-product-thesis.md) for the provisional product
thesis and [RFC 0002](docs/rfcs/0002-steel-thread-method.md) for the working
method and current evidence sequence. Active experiments are indexed under
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

The embedded Nushell/MCP and browser feedback threads are implemented. A
neutral external-driver experiment now preserves process lifecycle, raw JSON
Lines transcripts, stderr, and named comparison projections in reopenable
evidence directories. The next boundary binds a real harness to a controlled
workspace and capability source through the workbench. Names and contracts
remain provisional as later steel threads produce evidence.

## Browser workbench

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
