# Steel thread 0005: Manual evaluation promotion

- Status: demonstrated locally
- Timebox: one implementation and acceptance slice, capped at five working days

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

The public conformance walkthrough uses neutral synthetic external drivers and
the checked-in catalog and analysis sources. A deterministic completed turn
became a draft whose source revision, pre-turn workspace, file modes,
capability revisions, and source event sequences were copied into
revision-owned storage.

The test changed the expected active names to an intentionally incorrect value.
That revision replayed to completion: schema, score, capability-use,
composition, and analysis-consistency checks passed, while the name assertion
failed with actual values `alpha` and `gamma`. Agent Lab retained this as a
complete failed validation rather than treating it as an infrastructure error.

The test then corrected the names through the same structured revision
contract. Its replay passed every catalog assertion, and explicit save promoted
that exact revision into a local definition. Two synthetic harness profiles ran
the saved definition sequentially from independent copies of the same stored
revision. Both produced the expected active items, count 2, and total score 11;
the paired result retained each profile's native events separately.

Controller restart coverage reopens the draft with its revision and validation
history, the saved definition, and the paired evaluation entirely from stored
evidence. Failure coverage retains assertion failures, repairs interrupted
validations, rejects stale edits, rolls back failed publication, preserves file
modes, and quarantines credential-contaminated evidence. Browser coverage
exercises the shared editor, validation history, save, rerun, and offline
reopening. Private adapter observations remain in their owning repositories and
are not evidence for this public thread.

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
