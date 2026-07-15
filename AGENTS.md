# Agent Lab contributor guidance

## Public repository boundary

This repository is public. Do not add private source code, proprietary tool or
prompt definitions, credentials, production data, private logs, or evidence
derived from private systems.

Private agent drivers and product adapters belong in their owning repositories.
Keep the public boundary neutral and connect private integrations through
documented protocols and synthetic conformance fixtures.

## Development method

- Prefer a working steel thread over a broad speculative framework.
- State the hypothesis, timebox, acceptance evidence, and non-goals for each
  thread.
- Preserve structured values and lifecycle events; avoid flattening data merely
  to simplify an adapter.
- Record failures and negative results when they change an architectural
  conclusion.
- Update RFCs after implementation evidence changes the project model.

## Pull requests

- Use pull requests for changes after the repository bootstrap.
- Open pull requests ready for review unless the work is intentionally parked.
- Keep each pull request attached to one evidence-producing boundary.
- Use the repository pull request template and report the exact validation run.

## Rust validation

Before publishing a change, run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```
