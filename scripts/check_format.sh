#!/bin/bash
# Check Rust code formatting
# Exit code 0 if formatted correctly, 1 if not

set -e

cargo fmt --check
