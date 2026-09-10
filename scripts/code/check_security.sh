#!/bin/bash
# Run the supply-chain and CI-hygiene checks that the Security workflow runs.
# Exit code 0 if clean, non-zero if any check fails.
#
# Install the tools once with:
#   cargo install cargo-deny --locked
#   uv tool install zizmor      # or: pipx install zizmor

set -e

echo "==> cargo-deny (workspace)"
cargo deny check

echo "==> cargo-deny (datui-pyo3)"
cargo deny --manifest-path crates/datui-pyo3/Cargo.toml check

echo "==> cargo-deny (fuzz)"
# Its own workspace, like datui-pyo3, so `cargo deny check` above does not reach it and
# its lockfile would go unaudited. Nothing here ships in a binary, but the advisories
# still matter for anyone running the fuzzers.
cargo deny --manifest-path fuzz/Cargo.toml check

echo "==> zizmor (workflow analysis)"
# --min-severity high matches the CI gate. Drop the flag to see everything.
zizmor --config zizmor.yml --min-severity high .github/workflows/
