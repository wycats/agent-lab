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

The embedded Nushell/MCP, browser feedback, neutral external-driver, catalog,
and shared two-harness workbench threads are implemented. The workbench can
capture one Explore revision, run two configured harnesses sequentially, show
their behavioral differences, and reopen the paired evidence after restart.
Names and contracts remain provisional as later steel threads produce
evidence.

## Browser workbench

Install the web dependencies, then build and start the loopback-only SvelteKit
lab bench with:

```console
$ pnpm install
$ pnpm web:demo
Agent Lab web surface: http://127.0.0.1:…
Local Nushell and scenario runs; press Ctrl-C to stop.
```

The server fixes its provider to the repository's visual-shell binary and the
synthetic MCP fixture; clients cannot select another executable or configure an
arbitrary MCP server through the gateway. Explore uses Nushell's structured
language and projected capabilities without exposing ambient filesystem,
script-loading, file-redirection, or external-command access. A future physical
workspace projection needs a controller-mediated, attributable interface rather
than ambient shell access. Keep the loopback workbench local and run the browser
acceptance with `pnpm web:test`.

## The opening loop

Agent Lab begins with a real environment rather than a prewritten agent run.
The browser and Nushell are two projections of the same active workspace,
capabilities, harness selection, model profile, and evaluation history.

The catalog scenario demonstrates the loop in four commands:

```nu
catalog list | where active
catalog list | where active | analysis summarize
lab assembly
lab compare
```

The first two commands explore and compose the capabilities available to every
harness. `lab assembly` makes the controlled inputs explicit. `lab compare`
captures the current workspace revision, runs the selected harness pair, and
streams structured progress into both Nushell and the browser. `lab evaluation`
reopens the durable result without rerunning it.

## Model access

Model access is part of the workbench assembly. Agent Lab establishes readiness
before starting a run and resolves fresh credentials only for the child harness
that needs them. Credential values stay out of browser traffic, shell state,
events, logs, and evidence bundles.

The included AI Gateway resolver accepts an ambient `AI_GATEWAY_API_KEY`,
`VERCEL_OIDC_TOKEN`, or `ANTHROPIC_API_KEY`. For refreshable local Vercel OIDC,
link the repository to the Vercel project that should own model usage:

```console
pnpm dlx vercel link --team <team> --project <project>
```

The generated `.vercel` directory is local-only and ignored by Git. Once the
project is linked, reload the workbench; Model access will move to **Ready**.

The run-capable controller currently requires Unix. Its evidence and scoring
paths use handle-relative, no-follow file reads to keep concurrent workspace
processes confined; startup reports an unsupported-platform error until an
equivalent Windows implementation exists.
