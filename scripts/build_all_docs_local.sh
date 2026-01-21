#!/bin/bash
# Build all documentation locally for testing
# Builds all historical tags and main branch, then serves locally for browsing

set -e

# Ensure we're in repo root
cd "$(git rev-parse --show-toplevel)"

# mdbook might be installed in the ~/.cargo/bin which may not be on the path.
# if mdbook is not found, try there
if ! command -v mdbook &> /dev/null; then
    echo "mdbook not found, trying ${HOME}/.cargo/bin"
    if [ -f "${HOME}/.cargo/bin/mdbook" ]; then
        export PATH="${HOME}/.cargo/bin:$PATH"
        echo "mdbook found in ${HOME}/.cargo/bin"
    else
        echo "mdbook not found in ${HOME}/.cargo/bin"
        exit 1
    fi
fi

echo "Building all documentation locally..."
echo ""

# Save original state
ORIGINAL_COMMIT=$(git rev-parse HEAD)
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Starting from: $ORIGINAL_BRANCH at commit $ORIGINAL_COMMIT"
echo ""

# Clean up any existing book directory
if [ -d "book" ]; then
    echo "Cleaning up existing book directory..."
    rm -rf book
fi

mkdir -p book

# Make scripts executable
chmod +x scripts/build_single_version_docs.sh scripts/rebuild_index.sh

# Get all version tags
VERSION_TAGS=$(git tag -l "v*" | sort -V)
echo "Found $(echo "$VERSION_TAGS" | wc -w) version tags"
echo ""

# Build all historical tags
if [ -n "$VERSION_TAGS" ]; then
    echo "Building documentation for all historical tags..."
    for tag in $VERSION_TAGS; do
        echo "  Building $tag..."
        ./scripts/build_single_version_docs.sh "$tag" || echo "    Warning: Failed to build $tag"
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || git checkout "$ORIGINAL_COMMIT" 2>/dev/null || true
    done
    echo ""
fi

# Build main branch
echo "Building documentation for main branch..."
git checkout "$ORIGINAL_BRANCH" 2>/dev/null || git checkout "$ORIGINAL_COMMIT" 2>/dev/null || true
./scripts/build_single_version_docs.sh "main"
echo ""

# Rebuild index
echo "Rebuilding index page..."
./scripts/rebuild_index.sh
echo ""

# Clean up any global demos directory
rm -rf book/demos

echo "✓ Documentation build complete!"
echo ""
echo "Documentation is available in: $(pwd)/book/"
echo ""
echo "To view locally, you can:"
echo "  1. Open book/index.html in your browser"
echo "  2. Or use a simple HTTP server:"
echo "     python3 -m http.server 8000 --directory book"
echo "     Then visit: http://localhost:8000"
echo ""

# Optionally serve with Python if available
if command -v python3 &> /dev/null; then
    read -p "Start a local HTTP server to view the docs? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Starting server at http://localhost:8000"
        echo "Press Ctrl+C to stop"
        python3 -m http.server 8000 --directory book
    fi
fi
