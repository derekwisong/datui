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
  full                    Run the full workspace suite, as CI does

Examples:
  scripts/dev/test.sh unit data_quality::
  scripts/dev/test.sh integration integration_test test_data_quality
  scripts/dev/test.sh integration home_test test_recognises_data_extensions
  scripts/dev/test.sh --print preflight

--print shows commands without running them. Ignored/live-cloud tests remain
opt-in. Existing tests may generate missing fixtures; prepare them once with
scripts/dev/setup-test-data.sh. See docs/for-developers/tests.md for scope policy.
EOF
}

print_only=false
if [[ ${1:-} == --print ]]; then
    print_only=true
    shift
fi

run() {
    if $print_only; then
        printf '%q ' "$@"
        printf '\n'
    else
        "$@"
    fi
}

command=${1:-}
if [[ -z $command || $command == --help || $command == -h ]]; then
    usage
    exit 0
fi
shift

case "$command" in
    check)
        if (( $# > 1 )); then usage >&2; exit 2; fi
        run cargo check --locked -p "${1:-datui-lib}"
        ;;
    unit)
        run cargo test --locked -p datui-lib --lib "$@"
        ;;
    integration)
        target=${1:-}
        if [[ -z $target || ! -f tests/$target.rs ]]; then
            printf 'Choose an existing integration target from tests/*.rs.\n' >&2
            exit 2
        fi
        shift
        run cargo test --locked -p datui --test "$target" "$@"
        ;;
    cli)
        run cargo test --locked -p datui-cli --lib "$@"
        ;;
    preflight)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        run cargo fmt --check
        run cargo clippy --workspace --all-targets --locked -- -D warnings
        ;;
    full)
        if (( $# != 0 )); then usage >&2; exit 2; fi
        run cargo test --workspace --locked --no-fail-fast
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
