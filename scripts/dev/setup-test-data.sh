#!/usr/bin/env bash
# Kept for old links: scripts/setup_dev.py sets up .venv and the fixtures.
exec "${PYTHON:-python3}" "$(dirname "${BASH_SOURCE[0]}")/../setup_dev.py" --no-hooks "$@"
