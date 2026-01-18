#!/bin/bash
# Generate all demo GIFs from VHS .tape files
# Must be run from repository root for paths to work correctly

set -e


SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TAPES_DIR="${SCRIPT_DIR}"
DEMOS_DIR="${REPO_ROOT}/demos"

# ensure the "target/release/datui" binary is built, and is on the PATH
# dont build it here, just check if it exists if so, make sure it is
# on the PATH so that vhs can find datui easily.

if [ ! -f "${REPO_ROOT}/target/release/datui" ]; then
    echo "Error: datui binary not found. Please build it first."
    exit 1
fi

export PATH="${REPO_ROOT}/target/release:${PATH}"


# Change to repository root (required for VHS paths to work)
cd "${REPO_ROOT}"

# Create demos directory if it doesn't exist
mkdir -p "${DEMOS_DIR}"

# Check if vhs is installed
if ! command -v vhs &> /dev/null; then
    echo "Error: vhs not found. Please install VHS and ensure it's in your PATH."
    exit 1
fi

# Find all .tape files and generate GIFs
cd "${TAPES_DIR}"

# Check if any .tape files exist before processing
if ! ls *.tape 1> /dev/null 2>&1; then
    echo "Error: No .tape files found in ${TAPES_DIR}"
    exit 1
fi

# Process each .tape file
for tape_file in *.tape; do
    # Skip if not a regular file (handles case where glob doesn't match)
    [ ! -f "$tape_file" ] && continue
    
    echo "Generating GIF from ${tape_file}..."
    # Run from repo root so output paths work correctly
    cd "${REPO_ROOT}"
    vhs "${TAPES_DIR}/${tape_file}"
    # Change back to TAPES_DIR for next iteration's glob expansion
    cd "${TAPES_DIR}"
done

echo ""
echo "✅ All demos generated in ${DEMOS_DIR}/"
ls -lh "${DEMOS_DIR}"/*.gif 2>/dev/null || echo "No GIFs found (check VHS output paths)"
