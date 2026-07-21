# Steel thread 0004: Two-harness workbench

- Status: demonstrated locally

## Hypothesis

Agent Lab can run two real harnesses from one controlled snapshot, preserve
their native behavior, and make their differences inspectable through one
shared human workbench without standardizing either agent loop.

## Boundary

The controller owns the Explore workspace, capability revision, harness and
model selection, sequential evaluation lifecycle, and immutable evidence. v0
and Eve run through independent external drivers and native execution hosts.
Nushell and the browser inspect the same assembly and can start the same paired
evaluation operation.

## Evidence

A local acceptance evaluation ran v0 followed by Eve against the same
catalog-to-file snapshot, Haiku 4.5 profile, prompt, source revisions, and
limits. Both arms passed and produced identical structured output: active items
`alpha` and `gamma`, active count 2, and total score 11.

v0 used five model turns and completed in 20.475 seconds. Eve used six turns and
completed in 14.891 seconds. Both recorded two capability calls, two native
actions, and one workspace effect. Eve reported token and cache usage; v0 did
not. The paired comparison reopened from stored evidence with the same scores
and output projection.

The live evidence bundle and credentials remain local. Public acceptance uses
two synthetic external drivers and contains no v0 or Eve implementation code.

## Conclusion

The external-driver and evidence boundaries support more than one real harness.
Behavioral comparison should present correctness and observable differences
without manufacturing unavailable metrics or a universal winner. Model access
belongs to the server-side assembly and must resolve fresh credentials only for
the child process that needs them.

This is the first complete run-and-review loop. Promotion into editable
evaluations, repeated behavioral claims, and a harness change followed by a
controlled rerun remain future product work.
