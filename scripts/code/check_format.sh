#!/bin/bash
# Check Rust code formatting
# Exit code 0 if formatted correctly, 1 if not

set -e

cargo fmt --check

# The fuzz targets are their own Cargo workspace (see fuzz/Cargo.toml), so the check
# above does not reach them. Checked here rather than left to rot.
cargo fmt --check --manifest-path fuzz/Cargo.toml
