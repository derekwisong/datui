#!/bin/bash
# Build documentation for a single version (main branch or a specific tag)
# This is used in CI to update only the changed version without rebuilding all tags
#
# Usage: build_single_version_docs.sh [VERSION] [--worktree]
#   VERSION: version tag (e.g., v1.0.0) or branch name (default: main)
#   --worktree: flag indicating this is being run from a git worktree (skips checkout/restore)

set -e

OUTPUT_DIR="book"
VERSION_NAME="${1:-main}"  # Default to "main" if no argument provided
USE_WORKTREE=false
IS_TAG=false

# Parse arguments
for arg in "$@"; do
    if [ "$arg" = "--worktree" ]; then
        USE_WORKTREE=true
        # Remove --worktree from VERSION_NAME if it was set there
        if [ "$VERSION_NAME" = "--worktree" ]; then
            VERSION_NAME="main"
        fi
    fi
done

# Get repo root
# If DATUI_REPO_ROOT is set (from worktree caller), use it
# Otherwise, detect it normally
CURRENT_DIR=$(pwd)
if [ -n "$DATUI_REPO_ROOT" ]; then
    REPO_ROOT="$DATUI_REPO_ROOT"
else
    REPO_ROOT=$(git rev-parse --show-toplevel)
fi

# When using worktree, we build from the worktree but output to main repo
# When not using worktree, we work from repo root
if [ "$USE_WORKTREE" = true ]; then
    # Stay in worktree directory for building (has docs/, book.toml, etc.)
    BUILD_DIR="$CURRENT_DIR"
else
    # Work from repo root
    cd "$REPO_ROOT"
    BUILD_DIR="$REPO_ROOT"
fi

# Create output directory (use repo root, not worktree root)
mkdir -p "${REPO_ROOT}/${OUTPUT_DIR}"

# Determine if we're building a tag or main branch
if [[ "$VERSION_NAME" =~ ^v[0-9] ]]; then
    IS_TAG=true
    echo "Building docs for tag: $VERSION_NAME"
    
    if [ "$USE_WORKTREE" = false ]; then
        # Save the original commit/branch we started from
        ORIGINAL_COMMIT=$(git rev-parse HEAD)
        ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")
        echo "Original commit: $ORIGINAL_COMMIT"
        echo "Original branch: $ORIGINAL_BRANCH"
        
        # Checkout the tag
        if ! git checkout "$VERSION_NAME" 2>/dev/null; then
            echo "Error: Could not checkout tag $VERSION_NAME"
            exit 1
        fi
    else
        echo "Running from worktree (already checked out)"
    fi
else
    echo "Building docs for branch: $VERSION_NAME"
    # We're already on the correct branch/commit
fi

# Verify docs directory exists
if [ ! -d "$BUILD_DIR/docs/" ]; then
    echo "Error: docs/ directory not found in $BUILD_DIR"
    exit 1
fi

# Verify book.toml exists
if [ ! -f "$BUILD_DIR/book.toml" ]; then
    echo "Error: book.toml not found in $BUILD_DIR"
    exit 1
fi

# Build mdbook for this version
# Always use repo root for output, not current directory (which might be a worktree)
OUTPUT_PATH="${REPO_ROOT}/${OUTPUT_DIR}/${VERSION_NAME}"
mkdir -p "${OUTPUT_PATH}"

# Clean up any existing demos in the global output directory to avoid conflicts
rm -rf "${REPO_ROOT}/${OUTPUT_DIR}/demos"

# Use a temp docs tree so we never overwrite repo docs (incl. command-line-options.md).
# Copy docs + book.toml, generate CLI options into temp, then mdbook from temp.
DOCS_TEMP=$(mktemp -d)
trap 'rm -rf "$DOCS_TEMP"' EXIT
cp -r "$BUILD_DIR/docs" "$DOCS_TEMP/docs"
cp "$BUILD_DIR/book.toml" "$DOCS_TEMP/book.toml"

# Generate command-line-options.md into temp docs (never touch repo docs).
if [ -f "$BUILD_DIR/scripts/docs/generate_command_line_options.py" ]; then
    if (cd "$BUILD_DIR" && python3 scripts/docs/generate_command_line_options.py -o "$DOCS_TEMP/docs/reference/command-line-options.md"); then
        echo "✓ Generated command-line-options.md (temp)"
    else
        echo "Error: generate_command_line_options.py failed (cargo build required)"
        exit 1
    fi
else
    echo "  Warning: generate_command_line_options.py not found - skipping"
fi

# Build mdbook from temp book root (src = docs in temp)
if mdbook build "$DOCS_TEMP" --dest-dir "${OUTPUT_PATH}"; then
    echo "✓ Built docs for $VERSION_NAME"
else
    echo "Error: mdbook build failed for $VERSION_NAME"
    exit 1
fi

# Copy demos directory into this version's output directory
# (from build dir; temp docs reference demos/ and we place them in output)
if [ -d "$BUILD_DIR/demos" ]; then
    rm -rf "${OUTPUT_PATH}/demos"
    cp -r "$BUILD_DIR/demos" "${OUTPUT_PATH}/demos"
    echo "✓ Copied demos directory to ${VERSION_NAME}/demos"
else
    echo "  Warning: demos directory not found for ${VERSION_NAME} - skipping"
fi

rm -rf "${REPO_ROOT}/${OUTPUT_DIR}/demos"

# If we checked out a tag and we're not using worktree, return to original commit/branch
if [ "$IS_TAG" = true ] && [ "$USE_WORKTREE" = false ]; then
    echo "Returning to original commit: $ORIGINAL_COMMIT"
    # Always return to the exact commit we started from
    git checkout "$ORIGINAL_COMMIT" 2>/dev/null || \
    git checkout "$ORIGINAL_BRANCH" 2>/dev/null || \
    git checkout main 2>/dev/null || \
    (echo "Warning: Could not return to original commit, continuing..." && true)
    
    # Verify we're back and scripts exist
    if [ ! -f "$REPO_ROOT/scripts/docs/build_single_version_docs.sh" ]; then
        echo "Error: Scripts missing after returning from tag checkout"
        echo "Current commit: $(git rev-parse HEAD)"
        echo "Current branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'detached HEAD')"
        exit 1
    fi
fi

echo "✓ Single version build complete: $VERSION_NAME"
