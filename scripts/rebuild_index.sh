#!/bin/bash
# Rebuild the index page for the documentation
# Scans the book directory for all version directories and generates index.html

set -e

OUTPUT_DIR="book"

# Ensure we're in repo root
cd "$(git rev-parse --show-toplevel)"

# Ensure output directory exists
mkdir -p "${OUTPUT_DIR}"

echo "Rebuilding index page..."

# Generate index page
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

# Add main/latest version first if it exists
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

# Get all version tags from the book directory and sort them
if [ -d "${OUTPUT_DIR}" ]; then
    # Find all directories that look like version tags (v*), sort them reverse
    for version_dir in $(find "${OUTPUT_DIR}" -maxdepth 1 -type d -name "v*" | sort -Vr); do
        if [ -d "$version_dir" ]; then
            VERSION_NAME=$(basename "$version_dir")
            
            # Try to get tag date if available (requires git access)
            TAG_DATE=""
            if git rev-parse "$VERSION_NAME" >/dev/null 2>&1; then
                TAG_DATE=$(git log -1 --format=%ai "$VERSION_NAME" 2>/dev/null | cut -d' ' -f1 || echo "")
            fi
            
            if [ -n "$TAG_DATE" ]; then
                DATE_STR="Released: ${TAG_DATE}"
            else
                DATE_STR="Release version"
            fi
            
            cat >> "${OUTPUT_DIR}/index.html" << INDEX_VERSION
        <a href="${VERSION_NAME}/index.html" class="version-card">
            <h2>${VERSION_NAME}</h2>
            <p>${DATE_STR}</p>
        </a>
INDEX_VERSION
        fi
    done
fi

cat >> "${OUTPUT_DIR}/index.html" << 'INDEX_FOOTER'
    </div>
</body>
</html>
INDEX_FOOTER

echo "✓ Index page regenerated"
