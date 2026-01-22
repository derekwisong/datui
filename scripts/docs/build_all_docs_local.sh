#!/bin/bash
# Build all documentation locally for testing
# Builds all historical tags and main branch, then serves locally for browsing
# Uses git worktree to avoid affecting the current working directory

set -e

# Ensure we're in repo root
REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

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

# Save original state (for reference, but we won't need to restore)
ORIGINAL_COMMIT=$(git rev-parse HEAD)
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")
echo "Current working directory: $ORIGINAL_BRANCH at commit $ORIGINAL_COMMIT"
echo "Using git worktree to avoid affecting your working directory"
echo ""

# Clean up any existing book directory
if [ -d "book" ]; then
    echo "Cleaning up existing book directory..."
    rm -rf book
fi

mkdir -p book

# Make scripts executable
chmod +x scripts/docs/build_single_version_docs.sh scripts/docs/rebuild_index.py

# Set up temporary worktree directory
WORKTREE_DIR="${REPO_ROOT}/.docs-build-worktree"
WORKTREE_CLEANUP=true

# Cleanup function
cleanup_worktree() {
    if [ "$WORKTREE_CLEANUP" = true ] && [ -d "$WORKTREE_DIR" ]; then
        echo ""
        echo "Cleaning up worktree..."
        cd "$REPO_ROOT"
        git worktree remove -f "$WORKTREE_DIR" 2>/dev/null || rm -rf "$WORKTREE_DIR"
    fi
}

# Set trap to cleanup on exit
trap cleanup_worktree EXIT

# Get all version tags
VERSION_TAGS=$(git tag -l "v*" | sort -V)
echo "Found $(echo "$VERSION_TAGS" | wc -w) version tags"
echo ""

# Build all historical tags using worktree
if [ -n "$VERSION_TAGS" ]; then
    echo "Building documentation for all historical tags..."
    for tag in $VERSION_TAGS; do
        echo "  Building $tag..."
        
        # Remove existing worktree if it exists
        if [ -d "$WORKTREE_DIR" ]; then
            git worktree remove -f "$WORKTREE_DIR" 2>/dev/null || rm -rf "$WORKTREE_DIR"
        fi
        
        # Create worktree for this tag
        if git worktree add "$WORKTREE_DIR" "$tag" 2>/dev/null; then
            # Build docs in the worktree, passing repo root as env var
            (cd "$WORKTREE_DIR" && DATUI_REPO_ROOT="$REPO_ROOT" "$REPO_ROOT/scripts/docs/build_single_version_docs.sh" "$tag" --worktree) || \
                echo "    Warning: Failed to build $tag"
            
            # Remove worktree after building
            git worktree remove -f "$WORKTREE_DIR" 2>/dev/null || rm -rf "$WORKTREE_DIR"
        else
            echo "    Warning: Could not create worktree for $tag"
        fi
    done
    echo ""
fi

# Build main branch (use current directory, no worktree needed)
echo "Building documentation for main branch..."
./scripts/docs/build_single_version_docs.sh "main"
echo ""

# Rebuild index
echo "Rebuilding index page..."
python3 scripts/docs/rebuild_index.py
echo ""

# Clean up any global demos directory
rm -rf book/demos

# Disable cleanup since we're done successfully
WORKTREE_CLEANUP=false

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

# Optionally serve with Python if available (only if running interactively)
# Skip server prompt if stdin is not a TTY (non-interactive mode)
if command -v python3 &> /dev/null && [ -t 0 ]; then
    read -p "Start a local HTTP server to view the docs? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Starting server at http://localhost:8000"
        echo "Press Ctrl+C to stop"
        python3 -m http.server 8000 --directory book
    fi
fi
