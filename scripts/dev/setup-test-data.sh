#!/usr/bin/env bash
# Create the Python environment the test suite needs, and generate sample data.
#
# Several integration tests (statistics, distribution detection, pivot/melt) read
# fixtures from tests/sample-data/. Those are generated rather than committed, by
# scripts/generate_sample_data.py, which needs Polars and friends. Without them the
# tests fail with a bare "Sample data generation failed" and no clue what to do.
#
# This mirrors what CI does, so a green local run means the same thing as a green CI
# run. Safe to re-run: the venv is reused and only missing fixtures are regenerated.
#
#   ./scripts/dev/setup-test-data.sh          # set up and generate
#   ./scripts/dev/setup-test-data.sh --force  # regenerate fixtures from scratch

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

VENV=".venv"
REQUIREMENTS="scripts/requirements.txt"
SAMPLE_DATA="tests/sample-data"

force=false
[[ ${1:-} == "--force" ]] && force=true

if [[ ! -f $REQUIREMENTS ]]; then
  echo "error: $REQUIREMENTS not found; run this from a datui checkout" >&2
  exit 1
fi

# uv is dramatically faster and handles the interpreter itself; fall back to the
# stdlib venv module so this works on a machine that only has Python.
if command -v uv >/dev/null 2>&1; then
  echo "==> Creating $VENV with uv"
  uv venv "$VENV"
  echo "==> Installing $REQUIREMENTS"
  VIRTUAL_ENV="$VENV" uv pip install -q -r "$REQUIREMENTS"
else
  echo "==> Creating $VENV with python -m venv"
  python3 -m venv "$VENV"
  echo "==> Installing $REQUIREMENTS (this is slow; install uv to speed it up)"
  "$VENV/bin/python" -m pip install --quiet --upgrade pip
  "$VENV/bin/pip" install --quiet -r "$REQUIREMENTS"
fi

if $force; then
  echo "==> Removing $SAMPLE_DATA"
  rm -rf "$SAMPLE_DATA"
fi

echo "==> Generating sample data"
"$VENV/bin/python" scripts/generate_sample_data.py

count=$(find "$SAMPLE_DATA" -type f 2>/dev/null | wc -l)
echo
echo "Done. $count fixtures in $SAMPLE_DATA/"
echo "Run the suite with: cargo test"
