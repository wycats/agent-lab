# Steel thread 0005: Manual evaluation promotion

- Status: demonstrated locally

## Hypothesis

Agent Lab can preserve a consequential interactive turn as an editable
evaluation, distinguish a behavioral failure from an execution failure,
promote exactly one passing revision, and rerun that definition through other
harnesses without reconstructing its task or starting state.

## Boundary

The controller owns draft, revision, validation-attempt, definition, and
evaluation-attempt identities. A revision owns its sanitized pre-turn workspace
snapshot, capability recipe, standalone task, limits, reviewed evaluator, and
provenance. The first evaluator is `catalog-to-file@1`; this thread does not
introduce generated assertion code or a general evaluator language.

Nushell and the browser edit and inspect the same draft. Validation uses the
source harness and model by default. Explicit save promotes only an exact
revision with one complete passing validation. Definition runs delegate to the
existing paired-evaluation controller.

## Evidence

A local real-model v0 turn used the catalog and analysis capabilities to create
and verify the expected `result.json`. The turn became a draft whose source
revision and pre-turn workspace were copied into revision-owned storage.

The builder changed the expected active names to an intentionally incorrect
value. That revision replayed to completion: schema, score, capability-use,
composition, and analysis-consistency checks passed, while the name assertion
failed with actual values `alpha` and `gamma`. Agent Lab retained this as a
complete failed validation rather than treating it as an infrastructure error.

The builder corrected the names through a structured Nushell pipeline, creating
a new immutable revision. Its replay passed every catalog assertion. Explicit
save promoted that exact revision into a local definition.

The saved definition then ran v0 followed by Eve with the shared Haiku 4.5
profile. Both arms started from the same stored revision, used the catalog and
analysis sources, produced the expected active items, count 2, and total score
11, and passed the task-specific scorer. The paired result preserved each
harness's native events and reported usage separately.

After a locald restart, the browser reopened the draft with its revision and
validation history, the saved definition, and the paired evaluation entirely
from stored evidence. A scan of manifests, events, transcripts, logs, and
bundles found no credential or workspace-control token markers. Live evidence,
credentials, and private adapter code remain local; deterministic fixtures
cover the public contracts.

## Conclusion

Evaluation promotion is a resource lifecycle, not a transcript export. Stable
draft identity and immutable revisions let a builder learn from failed
assertions without weakening the evidence for a later pass. Separating
execution status from assertion status makes complete behavioral failures useful
data. A definition can remain harness/model portable while retaining the source
harness and model as provenance and validation defaults.

The next product slice can add an attributable, read-only `ProposalSession`
that advises the builder by producing this same editable draft resource.
Code-server diagnostics remains the next richer capability experiment after
that assistance layer.
