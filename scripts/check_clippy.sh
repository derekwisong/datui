#!/bin/bash
# Run clippy linting checks
# Exit code 0 if no warnings, 1 if warnings found

set -e

cargo clippy --all-targets --locked -- -D warnings
