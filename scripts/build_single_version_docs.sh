#!/bin/bash
# Build documentation for a single version (main branch or a specific tag)
# This is used in CI to update only the changed version without rebuilding all tags

set -e

OUTPUT_DIR="book"
VERSION_NAME="${1:-main}"  # Default to "main" if no argument provided
IS_TAG=false

# Ensure we're in repo root
cd "$(git rev-parse --show-toplevel)"

# Save the original commit/branch we started from
ORIGINAL_COMMIT=$(git rev-parse HEAD)
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Determine if we're building a tag or main branch
if [[ "$VERSION_NAME" =~ ^v[0-9] ]]; then
    IS_TAG=true
    echo "Building docs for tag: $VERSION_NAME"
    echo "Original commit: $ORIGINAL_COMMIT"
    echo "Original branch: $ORIGINAL_BRANCH"
    
    # Checkout the tag
    if ! git checkout "$VERSION_NAME" 2>/dev/null; then
        echo "Error: Could not checkout tag $VERSION_NAME"
        exit 1
    fi
else
    echo "Building docs for branch: $VERSION_NAME"
    # We're already on the correct branch/commit
fi

# Verify docs directory exists
if [ ! -d "docs/" ]; then
    echo "Error: docs/ directory not found"
    exit 1
fi

# Verify book.toml exists
if [ ! -f "book.toml" ]; then
    echo "Error: book.toml not found"
    exit 1
fi

# Build mdbook for this version
OUTPUT_PATH="$(pwd)/${OUTPUT_DIR}/${VERSION_NAME}"
mkdir -p "${OUTPUT_PATH}"

if mdbook build --dest-dir "${OUTPUT_PATH}"; then
    echo "✓ Built docs for $VERSION_NAME"
else
    echo "Error: mdbook build failed for $VERSION_NAME"
    exit 1
fi

# Copy demos directory to book output (shared across all versions)
# Always copy from current checkout if demos exist
# For main: updates to latest demos
# For tags: preserves demos from that tag (if they exist) or keeps existing from artifact
if [ -d "demos" ]; then
    cp -r demos "${OUTPUT_DIR}/demos"
    echo "✓ Copied demos directory"
elif [ -d "${OUTPUT_DIR}/demos" ]; then
    echo "✓ Demos already exist in artifact, preserving"
else
    echo "  Warning: demos directory not found - skipping"
fi

# If we checked out a tag, return to original commit/branch
if [ "$IS_TAG" = true ]; then
    echo "Returning to original commit: $ORIGINAL_COMMIT"
    # Always return to the exact commit we started from
    git checkout "$ORIGINAL_COMMIT" 2>/dev/null || \
    git checkout "$ORIGINAL_BRANCH" 2>/dev/null || \
    git checkout main 2>/dev/null || \
    (echo "Warning: Could not return to original commit, continuing..." && true)
    
    # Verify we're back and scripts exist
    if [ ! -f "scripts/build_single_version_docs.sh" ]; then
        echo "Error: Scripts missing after returning from tag checkout"
        echo "Current commit: $(git rev-parse HEAD)"
        echo "Current branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'detached HEAD')"
        exit 1
    fi
fi

echo "✓ Single version build complete: $VERSION_NAME"
