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
shared two-harness workbench, persistent interactive-agent session, and manual
evaluation-promotion threads are implemented. A builder can continue a real
harness-native session from Nushell, inspect its attributable turns in the
browser, create and revise an evaluation from a turn, retain a failed
validation, promote a passing revision, run the definition through two
configured harnesses, and reopen the evidence after restart. Names and
contracts remain provisional as later steel threads produce evidence.
Attributable AI-assisted proposal drafting is the next product boundary in RFC
0004.

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

For a managed development service, use the checked-in locald configuration:

```console
$ locald up
```

Open `https://workbench.agent-lab.localhost/`. locald supplies a sticky `$PORT`
to the development launcher while the canonical HTTPS origin remains stable for
the browser, cookies, and same-origin authorization. The random loopback port is
the private listener behind that browser surface; it is not a second public
workbench URL.

## The opening loop

Agent Lab begins with a real environment rather than a prewritten agent run.
The browser and Nushell are two projections of the same active workspace,
capabilities, harness selection, model profile, and evaluation history.

The catalog scenario demonstrates the loop without requiring the builder to
assemble context for the agent first:

```nu
agent "Which catalog items are active, and which one matters most?"
agent "What did you conclude earlier?"
agent turn
catalog list | where active
lab assembly
lab compare
```

`agent` lazily opens the selected harness's native session. The harness receives
the catalog and analysis capabilities directly, so it can decide what to
inspect; the second turn demonstrates native conversational continuity.
`agent turn` exposes the answer, attributable capability activity, usage, and
workspace effects as structured evidence. The builder can then query the same
capability state directly and inspect the controlled assembly. `lab compare`
captures the current workspace revision, runs the selected harness pair, and
streams structured progress into both Nushell and the browser. `lab evaluation`
reopens the durable result without rerunning it.

At the prompt, a completed `agent "..."` displays the assistant response as
rendered Markdown. Its pipeline value remains a structured `agent-answer`
record, so binding or piping it preserves the response, session and turn
identities, activity, usage, outcome, and evidence:

```nu
let answer = (agent "Summarize the workspace")
$answer.response | from md
```

Use `--stream` when a consumer wants assistant Markdown as a text stream, or
`--raw` when it wants attributable session events. These projections are
mutually exclusive, and each turn remains durable and reopenable with
`agent turn`.

Structured pipelines remain available when the builder intentionally wants to
provide context rather than ask the harness to discover it:

```nu
catalog list | where active | agent "Analyze this exact selection"
```

Use `agent new`, `agent sessions`, `agent switch`, `agent cancel`, and
`agent close` to manage workspace-scoped sessions explicitly. Session identity,
turn input, native activity, cancellation, and workspace effects are retained as
local evidence; server restart preserves the history and marks any live native
process as interrupted.

## Promote a turn into an evaluation

A completed turn can become an editable local evaluation without rewriting its
starting state:

```nu
let draft = (lab evaluation new --from <turn-id> --through <turn-id>)
lab evaluation draft $draft.id
let validation = ($draft | lab evaluation validate)
let saved = (lab evaluation save $draft.id --name "Catalog result")
lab evaluation run $saved.definitionId v0 eve --model haiku-4-5
```

The browser **Draft** view edits the same controller-owned resource as
`lab evaluation draft`. Each material edit creates an immutable revision.
Validation replays that exact revision from its captured pre-turn workspace;
complete assertion failures remain in history beside later passing attempts.
Saving promotes only the passing revision selected by the builder. Definitions
remain portable across compatible harness/model selections, while the source
harness and model remain provenance and validation defaults.

`lab evaluation drafts` and `lab evaluation definitions` list the local
library. Bare `lab evaluation` continues to inspect paired evaluation attempts.
All drafts, revisions, validations, definitions, and paired runs reopen from
durable evidence after restart.

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
