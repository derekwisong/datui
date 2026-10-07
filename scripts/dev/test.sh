#!/usr/bin/env bash
# Select targets before filtering tests so the edit loop does not link the suite.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/dev/test.sh [--print] COMMAND [ARGS...]

Set up
  setup [ARGS...]          Create .venv, install the Python tools and generate the
                           fixtures (scripts/setup_dev.py; --help lists its options)

Edit loop
  check [PACKAGE]          Check one package (default: datui-lib), without linking
  unit [FILTER...]         Run datui-lib's library tests
  integration TARGET [ARGS...]
                           Run one root integration target, with optional filter/flags
  cli [FILTER...]          Run datui-cli's library tests
  fmt [--check]            Format the workspace, the fuzz targets and datui-pyo3
                           (--check: only check)

What CI checks
  lint                     Check formatting and run clippy in the workspace, the fuzz
                           targets and datui-pyo3 (preflight is the same)
  clippy                   Clippy on the root workspace alone (the pre-commit hook)
  msrv                     Check the workspace with the rust-version in Cargo.toml
  docs [--require]         Lint the docs, their examples and the manpages, and run the
                           docs and demo scripts' tests (--require: fail without mandoc)
  ci                       The linux job's tests: nextest (profile ci) and doctests,
                           or cargo test without nextest
  python                   Build the CLI and the wheel into .venv, run its tests and
                           the docs' Python examples (needs setup --wheel)
  features [--test] [FEATURE...]
                           Lint datui and datui-lib with no default features, then
                           each feature alone (default: none cloud http sql sqlite streaming);
                           --test also runs datui-lib's library tests in each
  full                     Run the full workspace suite with cargo test

Examples:
  scripts/dev/test.sh unit data_quality::
  scripts/dev/test.sh integration app data_quality::
  scripts/dev/test.sh integration data statistics::
  scripts/dev/test.sh --print lint

--print shows commands without running them. Ignored/live-cloud tests remain
opt-in. Tests generate missing or stale fixtures themselves; setup does it ahead.
See docs/for-developers/tests.md for scope policy.

Runs that build or link much (unit, integration, lint, clippy, msrv, ci, python,
features, full, and anything with --release) wait for one of
DATUI_TEST_HEAVY_SLOTS slots (default 2) shared by every checkout on the machine,
and say so once if they wait. DATUI_TEST_HEAVY_SLOTS=0 turns the wait off.
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
    [[ $slots == 0 ]] && return 0
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

# The project virtualenv's Python, as setup makes it.
venv_python() {
    if [[ -x .venv/Scripts/python.exe ]]; then
        printf '%s\n' .venv/Scripts/python.exe
    else
        printf '%s\n' .venv/bin/python
    fi
}

need_venv() {
    if ! $print_only && [[ ! -x $(venv_python) ]]; then
        printf 'No .venv; run scripts/dev/test.sh setup first.\n' >&2
        exit 1
    fi
}

# Workspaces of their own, which the root's `cargo fmt` and clippy do not reach.
OTHER_WORKSPACES=(fuzz/Cargo.toml crates/datui-pyo3/Cargo.toml)

clippy_workspace() {
    run cargo clippy --workspace --all-targets --locked -- -D warnings
}

command=${1:-}
if [[ -z $command || $command == --help || $command == -h ]]; then
    usage
    exit 0
fi
shift

case "$command" in
    unit | integration | lint | preflight | clippy | msrv | ci | python | features | full) heavy=true ;;
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
    setup)
        # Plain Python, not the venv's: this is what makes the venv.
        python=python3
        command -v python3 >/dev/null 2>&1 || python=python
        run "$python" scripts/setup_dev.py "$@"
        ;;
    fmt)
        check=()
        if [[ ${1:-} == --check ]]; then
            check=(--check)
            shift
        fi
        if (( $# != 0 )); then usage >&2; exit 2; fi
        run cargo fmt ${check[@]+"${check[@]}"}
        for manifest in "${OTHER_WORKSPACES[@]}"; do
            run cargo fmt ${check[@]+"${check[@]}"} --manifest-path "$manifest"
        done
        ;;
    lint | preflight)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        # Every check runs, so one failure does not hide the next.
        failed=0
        run cargo fmt --check || failed=1
        for manifest in "${OTHER_WORKSPACES[@]}"; do
            run cargo fmt --check --manifest-path "$manifest" || failed=1
        done
        clippy_workspace || failed=1
        for manifest in "${OTHER_WORKSPACES[@]}"; do
            run cargo clippy --manifest-path "$manifest" --all-targets --locked -- -D warnings \
                || failed=1
        done
        exit "$failed"
        ;;
    clippy)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        clippy_workspace
        ;;
    msrv)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        version=$(sed -n 's/^rust-version *= *"\([^"]*\)".*/\1/p' Cargo.toml | head -1)
        if [[ -z $version ]]; then
            printf 'No rust-version in Cargo.toml.\n' >&2
            exit 1
        fi
        if ! $print_only && command -v rustup >/dev/null 2>&1 \
            && ! rustup toolchain list | grep -q "^$version[-.]"; then
            printf 'Rust %s is not installed: rustup toolchain install %s --profile minimal\n' \
                "$version" "$version" >&2
            exit 1
        fi
        run cargo "+$version" check --workspace --locked
        ;;
    docs)
        require=()
        if [[ ${1:-} == --require ]]; then
            require=(--require)
            shift
        fi
        if (( $# != 0 )); then usage >&2; exit 2; fi
        need_venv
        py=$(venv_python)
        # Every check runs, so one failure does not hide the next.
        failed=0
        run "$py" scripts/docs/lint_docs.py || failed=1
        run "$py" scripts/docs/doc_examples.py --lint || failed=1
        run "$py" scripts/docs/lint_manpages.py ${require[@]+"${require[@]}"} || failed=1
        run "$py" -m unittest discover -s scripts/docs -p 'test_*.py' || failed=1
        run "$py" -m unittest discover -s scripts/demos -p 'test_*.py' || failed=1
        exit "$failed"
        ;;
    ci)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        if cargo nextest --version >/dev/null 2>&1; then
            # One process per test, as CI runs them; nextest does not run doctests.
            run_tests cargo nextest run --workspace --locked --no-fail-fast --profile ci
            run_tests cargo test --doc --workspace --locked
        else
            printf 'cargo-nextest not found; running cargo test, which shares a process per binary.\n' >&2
            run_tests cargo test --workspace --locked --no-fail-fast
        fi
        ;;
    python)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        need_venv
        py=$(venv_python)
        venv_bin=$(dirname "$py")
        if ! $print_only && [[ ! -x $venv_bin/maturin && ! -x $venv_bin/maturin.exe ]]; then
            printf 'maturin is not in .venv; run scripts/dev/test.sh setup --wheel.\n' >&2
            exit 1
        fi
        # The wheel runs the binary bundled beside the package, not the one on PATH.
        run cargo build -p datui --locked
        target=${CARGO_TARGET_DIR:-target}
        exe=datui
        [[ -f $target/debug/datui.exe ]] && exe=datui.exe
        run mkdir -p python/datui_bin
        run cp "$target/debug/$exe" python/datui_bin/
        run cp LICENSE python/LICENSE
        # maturin installs into the venv VIRTUAL_ENV names.
        if $print_only; then
            printf '(cd python && VIRTUAL_ENV=../.venv ../%s/maturin develop)\n' "$venv_bin"
        else
            (cd python && VIRTUAL_ENV="$OLDPWD/.venv" "$OLDPWD/$venv_bin/maturin" develop 9>&-)
        fi
        run "$py" -m pytest python/tests/ -v
        run "$py" scripts/docs/doc_examples.py --python
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
