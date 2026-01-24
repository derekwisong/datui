#!/bin/bash
# Check built documentation for broken links using lychee.
#
# Usage: check_doc_links.sh [--build] [--online] [PATH]
#   --build   Build docs for main first (build_single_version_docs.sh main).
#   --online  Also check external URLs (default: --offline, internal links only).
#   PATH      Directory to check (default: book/main relative to repo root).
#
# Install lychee: cargo install lychee
# Exit code: 0 if all links OK, non-zero if broken links or lychee missing.

set -e

USE_BUILD=false
USE_ONLINE=false
CHECK_PATH=""

for arg in "$@"; do
    case "$arg" in
        --build)  USE_BUILD=true ;;
        --online) USE_ONLINE=true ;;
        -*)
            echo "Unknown option: $arg" >&2
            echo "Usage: $0 [--build] [--online] [PATH]" >&2
            exit 1
            ;;
        *) CHECK_PATH="$arg" ;;
    esac
done

if [ -n "$DATUI_REPO_ROOT" ]; then
    REPO_ROOT="$DATUI_REPO_ROOT"
else
    REPO_ROOT=$(git rev-parse --show-toplevel)
fi
cd "$REPO_ROOT"

if [ -z "$CHECK_PATH" ]; then
    CHECK_PATH="book/main"
fi
if [[ "$CHECK_PATH" != /* ]]; then
    CHECK_PATH="${REPO_ROOT}/${CHECK_PATH}"
fi

if ! command -v lychee &> /dev/null; then
    if [ -f "${HOME}/.cargo/bin/lychee" ]; then
        export PATH="${HOME}/.cargo/bin:$PATH"
    else
        echo "Error: lychee not found. Install with: cargo install lychee" >&2
        exit 1
    fi
fi

if [ "$USE_BUILD" = true ]; then
    echo "Building documentation for main..."
    ./scripts/docs/build_single_version_docs.sh main
    echo ""
fi

if [ ! -d "$CHECK_PATH" ]; then
    echo "Error: Directory not found: $CHECK_PATH" >&2
    echo "Build docs first (e.g. $0 --build) or pass a valid PATH." >&2
    exit 1
fi

echo "Checking links in $CHECK_PATH"
if [ "$USE_ONLINE" = true ]; then
    echo "(including external URLs)"
else
    echo "(internal links only, use --online to check external URLs)"
fi
echo ""

LYCHEE_OPTS=(--no-progress --include-fragments --root-dir "$CHECK_PATH" "$CHECK_PATH")
if [ "$USE_ONLINE" != true ]; then
    LYCHEE_OPTS=(--offline "${LYCHEE_OPTS[@]}")
fi

exec lychee "${LYCHEE_OPTS[@]}"
