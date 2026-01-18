#!/bin/bash
# Build versioned documentation for all version tags and main branch
# Generates an index page listing all versions

# Don't use set -e - we want to continue even if some builds fail
set +e

OUTPUT_DIR="book"
SOURCE_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_COMMIT=$(git rev-parse HEAD)

# Ensure we're in repo root
cd "$(git rev-parse --show-toplevel)"

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Get all version tags sorted by version
VERSION_TAGS=$(git tag -l "v*" | sort -V)

echo "Found version tags: ${VERSION_TAGS}"

# Build docs for each version tag
for tag in $VERSION_TAGS; do
    echo "Building docs for $tag..."
    if ! git checkout "$tag" 2>/dev/null; then
        echo "  Warning: Could not checkout tag $tag, skipping..."
        continue
    fi
    
    # Skip if docs/ directory doesn't exist
    if [ ! -d "docs/" ]; then
        echo "  Warning: docs/ directory not found for $tag, skipping..."
        git checkout "$SOURCE_BRANCH" 2>/dev/null || git checkout "$CURRENT_COMMIT" 2>/dev/null || true
        continue
    fi
    
    # Skip if book.toml doesn't exist
    if [ ! -f "book.toml" ]; then
        echo "  Warning: book.toml not found for $tag, skipping..."
        git checkout "$SOURCE_BRANCH" 2>/dev/null || git checkout "$CURRENT_COMMIT" 2>/dev/null || true
        continue
    fi
    
    # Build mdbook (using absolute path for output to avoid issues with checkout)
    OUTPUT_PATH="$(pwd)/${OUTPUT_DIR}/${tag}"
    mkdir -p "${OUTPUT_PATH}"
    
    if mdbook build --dest-dir "${OUTPUT_PATH}" 2>/dev/null; then
        echo "  ✓ Built docs for $tag"
    else
        echo "  Warning: mdbook build failed for $tag, skipping..."
        # Clean up partial output
        rm -rf "${OUTPUT_PATH}"
        # Return to source branch for next iteration
        git checkout "$SOURCE_BRANCH" 2>/dev/null || git checkout "$CURRENT_COMMIT" 2>/dev/null || true
        continue
    fi
    
    # Return to source branch for next iteration
    git checkout "$SOURCE_BRANCH" 2>/dev/null || git checkout "$CURRENT_COMMIT" 2>/dev/null || true
done

# Ensure we're back on the original commit before building current branch docs
echo "Returning to original commit ${CURRENT_COMMIT}..."
git checkout "$CURRENT_COMMIT" 2>/dev/null || git checkout "$SOURCE_BRANCH" 2>/dev/null || true

# Build latest/current branch docs
echo "Building latest docs from current branch/commit..."
if [ ! -d "docs/" ]; then
    echo "  Warning: docs/ directory not found in current branch - skipping latest docs build"
    echo "  This is normal if documentation hasn't been added to this branch yet"
else
    if [ ! -f "book.toml" ]; then
        echo "  Warning: book.toml not found - skipping latest docs build"
    else
        OUTPUT_PATH="$(pwd)/${OUTPUT_DIR}/main"
        mkdir -p "${OUTPUT_PATH}"
        if mdbook build --dest-dir "${OUTPUT_PATH}" 2>/dev/null; then
            echo "  ✓ Built docs for main"
        else
            echo "  Warning: mdbook build failed for current branch - skipping"
        fi
    fi
fi

# Generate index page
echo "Generating index page..."

cat > "${OUTPUT_DIR}/index.html" << 'INDEX_HEADER'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>datui Documentation</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #fff;
            padding: 2rem;
            max-width: 1200px;
            margin: 0 auto;
        }
        
        h1 {
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
            color: #1a1a1a;
        }
        
        .subtitle {
            color: #666;
            margin-bottom: 2rem;
        }
        
        .version-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 1.5rem;
            margin-top: 2rem;
        }
        
        .version-card {
            border: 1px solid #ddd;
            border-radius: 6px;
            padding: 1.5rem;
            transition: all 0.2s;
            text-decoration: none;
            display: block;
            color: inherit;
        }
        
        .version-card:hover {
            border-color: #4a9eff;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            transform: translateY(-2px);
        }
        
        .version-card.latest {
            border-color: #4a9eff;
            border-width: 2px;
            background: #f8fbff;
        }
        
        .version-card h2 {
            font-size: 1.5rem;
            margin-bottom: 0.5rem;
            color: #1a1a1a;
        }
        
        .version-card .badge {
            display: inline-block;
            background: #4a9eff;
            color: white;
            padding: 0.2rem 0.6rem;
            border-radius: 3px;
            font-size: 0.75rem;
            margin-left: 0.5rem;
            font-weight: 600;
        }
        
        .version-card p {
            color: #666;
            margin-top: 0.5rem;
        }
        
        @media (max-width: 768px) {
            body {
                padding: 1rem;
            }
            
            .version-list {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <h1>datui Documentation</h1>
    <p class="subtitle">Select a version to view its documentation:</p>
    
    <div class="version-list">
INDEX_HEADER

# Add main/latest version first
if [ -d "${OUTPUT_DIR}/main" ]; then
    cat >> "${OUTPUT_DIR}/index.html" << 'INDEX_MAIN'
        <a href="main/index.html" class="version-card latest">
            <h2>
                main
                <span class="badge">latest</span>
            </h2>
            <p>Development version - most current features</p>
        </a>
INDEX_MAIN
fi

# Add version tags in reverse order (newest first)
for tag in $(echo "$VERSION_TAGS" | sort -Vr); do
    if [ -d "${OUTPUT_DIR}/${tag}" ]; then
        # Try to get tag date if available
        TAG_DATE=$(git log -1 --format=%ai "${tag}" 2>/dev/null | cut -d' ' -f1 || echo "")
        
        if [ -n "$TAG_DATE" ]; then
            DATE_STR="Released: ${TAG_DATE}"
        else
            DATE_STR="Release version"
        fi
        
        cat >> "${OUTPUT_DIR}/index.html" << INDEX_VERSION
        <a href="${tag}/index.html" class="version-card">
            <h2>${tag}</h2>
            <p>${DATE_STR}</p>
        </a>
INDEX_VERSION
    fi
done

cat >> "${OUTPUT_DIR}/index.html" << 'INDEX_FOOTER'
    </div>
</body>
</html>
INDEX_FOOTER

echo "  ✓ Generated index.html"

# Restore original state
git checkout "$CURRENT_COMMIT" 2>/dev/null || true

echo "Done! Documentation built in ${OUTPUT_DIR}/"
