# Steel thread 0003: Catalog feedback loop

- Status: demonstrated locally

## Hypothesis

A person and a real external agent can explore and operate the same seeded
workspace and capability-source state without collapsing the human surface,
agent driver, execution host, or MCP sessions into one runtime.

## Boundary

The catalog-to-file scenario provides two authenticated loopback MCP sources:
`catalog` lists structured items and `analysis` summarizes a selected table.
Nushell projects both as structured commands. The catalog tool explicitly
declares its sole collection as table-first for direct pipelines, while
`--envelope` preserves the exact MCP structured result for schema-sensitive
inspection. The run controller gives an
external driver a separate MCP session, a root-confined physical workspace,
declared limits, and a prompt requiring a checked `result.json` artifact.

The browser keeps Explore available while the run streams model, capability,
native-action, workspace, usage, and scoring events. Completed runs reopen from
immutable evidence after the live driver and sources are gone.

In this slice, Explore is capability-oriented rather than an ambient local
shell. Its Nushell context keeps structured pipelines, help, and formatting but
does not expose filesystem commands, script loading, file redirection, or
external commands. The browser session therefore cannot mutate the physical
workspace used for agent scoring. Adding human filesystem access requires a
controller-mediated projection that can confine and attribute each mutation.

## Evidence

The deterministic fixture proves lifecycle, cancellation, malformed-event
handling, root confinement, scoring, redaction, and offline replay. A local
real-v0 acceptance run used the same catalog and analysis source revisions as
the attached Nushell session, created the expected artifact, passed the
task-specific scorer, and reopened with the same score.

The live bundle is intentionally not committed. Public tests contain only the
synthetic scenario, driver fixture, and capability data.

## Conclusion

The controller can own experiment identity, workspace snapshots, limits, and
evidence while the harness retains its model loop and native tools. Capability
observations must remain separate from driver-native events, and human and
agent MCP sessions can share authoritative source state without sharing
protocol-session identity.

The result establishes one real-harness feedback loop. It does not yet prove
cross-harness comparison, shared model profiles, or promotion of an exploratory
discovery into a reusable evaluation.
