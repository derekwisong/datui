#!/bin/bash
# Drive the fuzz targets in fuzz/. See docs/for-developers/fuzzing.md.
#
# Install the tool once with:
#   cargo install cargo-fuzz --locked
#
# Usage:
#   scripts/code/fuzz.sh build                       build every target
#   scripts/code/fuzz.sh replay                      replay every committed corpus and exit
#   scripts/code/fuzz.sh run <target> [args...]      fuzz one target
#   scripts/code/fuzz.sh cmin <target>               drop inputs that add no coverage
#   scripts/code/fuzz.sh list                        list the targets
#
# Set DATUI_FUZZ_SANITIZER=address for a deeper (much slower, much larger) run. See
# "Sanitizer" below for when that is worth it.

set -euo pipefail

# cargo-fuzz reaches for -Z sanitizer, which is normally nightly-only. Fuzzing on stable
# instead is deliberate: polars-ops has a build script that enables its own `nightly`
# feature whenever it detects a nightly compiler, and that code path uses `core::unicode`
# internals current nightly no longer exposes, so the dependency tree does not compile
# there at all. RUSTC_BOOTSTRAP keeps the fuzzers on the same pinned stable toolchain as
# the rest of CI.
export RUSTC_BOOTSTRAP=1

# Sanitizer: off by default.
#
# datui-lib contains no `unsafe`, so for these targets AddressSanitizer has almost
# nothing to find that a panic would not already report — the bugs here are index
# arithmetic, unbounded recursion and disagreeing width calculations, all of which trap
# on the debug assertions and overflow checks the fuzz profile turns on. It costs about
# double the build output (measured: 12 GB against 6.0 GB) and runs the target itself
# noticeably slower, which buys less coverage per minute of CI.
#
# It is still worth turning on for a long run, where it can reach the unsafe code inside
# polars and arrow that these targets feed. The Nightly workflow does exactly that.
SANITIZER="${DATUI_FUZZ_SANITIZER:-none}"

cd "$(dirname "$0")/../.."

TARGETS=(parse_query number_format fuzzy_match glob_match config_parse)

case "${1:-}" in
    list)
        printf '%s\n' "${TARGETS[@]}"
        ;;
    build)
        for t in "${TARGETS[@]}"; do
            echo "==> building $t (sanitizer: $SANITIZER)"
            cargo fuzz build --sanitizer "$SANITIZER" "$t"
        done
        ;;
    replay)
        # -runs=0 means "load the corpus, run each input once, generate nothing new".
        # Deterministic, fast, and it fails if a previously fixed crash comes back.
        for t in "${TARGETS[@]}"; do
            echo "==> replaying $t corpus (sanitizer: $SANITIZER)"
            cargo fuzz run --sanitizer "$SANITIZER" "$t" "fuzz/corpus/$t" -- -runs=0
        done
        ;;
    cmin)
        shift
        [ $# -ge 1 ] || { echo "usage: $0 cmin <target>" >&2; exit 2; }
        # Rewrites corpus/<target> in place, keeping the smallest set of inputs that
        # still reaches everything the whole set reached. Worth running before a corpus
        # is stored anywhere, or it grows without bound.
        cargo fuzz cmin --sanitizer "$SANITIZER" "$1" "fuzz/corpus/$1"
        ;;
    run)
        shift
        [ $# -ge 1 ] || { echo "usage: $0 run <target> [args...]" >&2; exit 2; }
        target="$1"; shift
        cargo fuzz run --sanitizer "$SANITIZER" "$target" "$@"
        ;;
    *)
        sed -n '2,15p' "$0"
        exit 2
        ;;
esac
