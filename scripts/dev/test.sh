#!/usr/bin/env bash
# Select targets before filtering tests so the edit loop does not link the suite.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/dev/test.sh [--print] COMMAND [ARGS...]

  check [PACKAGE]          Check one package (default: datui-lib), without linking
  unit [FILTER...]         Run datui-lib's library tests
  integration TARGET [ARGS...]
                          Run one root integration target, with optional filter/flags
  cli [FILTER...]          Run datui-cli's library tests
  preflight               Check formatting and lint all workspace targets
  features [--test] [FEATURE...]
                          Lint datui and datui-lib with no default features, then
                          each feature alone (default: none cloud http sql sqlite streaming);
                          --test also runs datui-lib's library tests in each
  full                    Run the full workspace suite (CI runs it under nextest)

Examples:
  scripts/dev/test.sh unit data_quality::
  scripts/dev/test.sh integration integration_test test_data_quality
  scripts/dev/test.sh integration home_test test_recognises_data_extensions
  scripts/dev/test.sh integration data statistics::
  scripts/dev/test.sh --print preflight

--print shows commands without running them. Ignored/live-cloud tests remain
opt-in. Existing tests may generate missing fixtures; prepare them once with
scripts/dev/setup-test-data.sh. See docs/for-developers/tests.md for scope policy.

Heavy runs (unit, integration, preflight, features, full, and anything with
--release) take one of DATUI_TEST_HEAVY_SLOTS locks (default 2) shared by every
checkout on the machine; a run that has to wait says so once. check and cli do not take it, nor
does --print. Without flock (macOS without util-linux) they run unlocked.
EOF
}

print_only=false
if [[ ${1:-} == --print ]]; then
    print_only=true
    shift
fi

# Heavy runs link Polars test executables or build the whole workspace; several at
# once exhausted the machine's memory (#513), so at most a few run together. The lock
# is per user and outside the checkout and the target dir, which differ per worktree,
# and not under TMPDIR, which runs often set for themselves.
heavy=false
held=false

lock_if_heavy() {
    if ! $heavy || $held || $print_only || [[ -n ${DATUI_TEST_LOCK_HELD:-} ]]; then
        return 0
    fi
    held=true
    if ! command -v flock >/dev/null 2>&1; then
        printf 'flock not found; running without the heavy-run lock.\n' >&2
        return 0
    fi
    local base
    if [[ -n ${XDG_RUNTIME_DIR:-} ]]; then
        base=$XDG_RUNTIME_DIR/datui-test-heavy
    else
        base=/tmp/datui-test-heavy-$(id -u)
    fi
    # DATUI_TEST_HEAVY_SLOTS runs at once (default 2), one lock file per slot. Slot 1
    # keeps the old name, so a checkout with the one-slot script still counts.
    local slots=${DATUI_TEST_HEAVY_SLOTS:-2} slot lock waited=false
    [[ $slots =~ ^[1-9][0-9]*$ ]] || slots=2
    # Held on fd 9 until this script exits, however it exits. Commands run with fd 9
    # closed (run, run_tests): a daemon one starts, such as sccache's server, would
    # otherwise hold the lock after the run.
    while :; do
        for ((slot = 1; slot <= slots; slot++)); do
            if ((slot == 1)); then lock=$base.lock; else lock=$base.$slot.lock; fi
            exec 9>>"$lock"
            flock -n 9 && break 2
            exec 9>&-
        done
        if ! $waited; then
            printf 'Waiting for one of %d heavy test runs to finish (%s*.lock)...\n' "$slots" "$base" >&2
            waited=true
        fi
        sleep 2
    done
    # A test.sh run from inside this one is part of it, and would wait on it forever.
    export DATUI_TEST_LOCK_HELD=1
}

run() {
    if $print_only; then
        printf '%q ' "$@"
        printf '\n'
    else
        lock_if_heavy
        "$@" 9>&-
    fi
}

# Tests read tests/sample-data and write their own files elsewhere: another test
# process may have these memory-mapped, and rewriting one kills it with SIGBUS
# (#486). Fail a run that wrote there, unless the generator ran: it rewrites
# every fixture, people.parquet among them.
run_tests() {
    if $print_only; then
        run "$@"
        return
    fi
    lock_if_heavy
    local written status=0
    marker=$(mktemp)
    trap 'rm -f "$marker"' EXIT
    "$@" 9>&- || status=$?
    # -H: worktrees often link tests/sample-data to one shared copy.
    written=$(find -H tests/sample-data -newer "$marker" 2>/dev/null) || true
    if [[ tests/sample-data/people.parquet -nt $marker ]]; then
        written=
    fi
    # `features --test` runs several; the trap only removes the last marker.
    rm -f "$marker"
    if [[ -n $written ]]; then
        printf 'Tests wrote into tests/sample-data (use common::fixture_dir() or a tempdir):\n%s\n' \
            "$written" >&2
        (( status != 0 )) || status=1
    fi
    return "$status"
}

command=${1:-}
if [[ -z $command || $command == --help || $command == -h ]]; then
    usage
    exit 0
fi
shift

case "$command" in
    unit | integration | preflight | features | full) heavy=true ;;
esac
for arg in "$@"; do
    if [[ $arg == --release ]]; then
        heavy=true
    fi
done

case "$command" in
    check)
        if (( $# > 1 )); then usage >&2; exit 2; fi
        run cargo check --locked -p "${1:-datui-lib}"
        ;;
    unit)
        run_tests cargo test --locked -p datui-lib --lib "$@"
        ;;
    integration)
        target=${1:-}
        if [[ -z $target || ! ( -f tests/$target.rs || -f tests/$target/main.rs ) ]]; then
            printf 'Choose an existing integration target: tests/*.rs or tests/*/main.rs.\n' >&2
            exit 2
        fi
        shift
        run_tests cargo test --locked -p datui --test "$target" "$@"
        ;;
    cli)
        run_tests cargo test --locked -p datui-cli --lib "$@"
        ;;
    preflight)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        # Both workspaces: the fuzz targets are their own, and CI checks them too.
        run scripts/code/check_format.sh
        run cargo clippy --workspace --all-targets --locked -- -D warnings
        ;;
    features)
        # The binary and datui-lib share feature names; datui-cli has none.
        test=false
        if [[ ${1:-} == --test ]]; then
            test=true
            shift
        fi
        features=("$@")
        if (( ${#features[@]} == 0 )); then
            features=(none cloud http sql sqlite streaming)
        fi
        # Every combination runs, so one failure does not hide the next.
        failed=0
        for feature in "${features[@]}"; do
            args=(-p datui -p datui-lib --all-targets --locked --no-default-features)
            if [[ $feature != none ]]; then
                args+=(--features "$feature")
            fi
            run cargo clippy "${args[@]}" -- -D warnings || failed=1
            if $test; then
                # Library tests only: nightly tests the binary without features apart.
                args=(-p datui-lib --lib --locked --no-default-features)
                if [[ $feature != none ]]; then
                    args+=(--features "$feature")
                fi
                run_tests cargo test "${args[@]}" || failed=1
            fi
        done
        exit "$failed"
        ;;
    full)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        run_tests cargo test --workspace --locked --no-fail-fast
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
