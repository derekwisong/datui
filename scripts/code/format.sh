#!/bin/bash
# Format Rust code
# This will format the code in place

set -e

cargo fmt

# The fuzz targets are their own Cargo workspace (see fuzz/Cargo.toml).
cargo fmt --manifest-path fuzz/Cargo.toml
