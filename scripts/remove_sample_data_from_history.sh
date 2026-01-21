#!/bin/bash
# Script to remove test sample data files from git history
# 
# WARNING: This rewrites git history. Make sure you:
# 1. Have a backup of your repository
# 2. Coordinate with any collaborators (they'll need to re-clone)
# 3. Understand that this cannot be undone easily
#
# Usage: Run this script from the repository root

set -e

echo "=========================================="
echo "Removing sample data from git history"
echo "=========================================="
echo ""
echo "This will:"
echo "  1. Remove all files in tests/sample-data/ (except README.md) from git history"
echo "  2. Rewrite all commits that touched these files"
echo "  3. Require a force push to update the remote"
echo ""
read -p "Have you backed up your repository? (yes/no): " backup_confirm
if [ "$backup_confirm" != "yes" ]; then
    echo "Please backup your repository first!"
    exit 1
fi

# Check if git-filter-repo is installed
if ! command -v git-filter-repo &> /dev/null; then
    echo ""
    echo "git-filter-repo is not installed."
    echo "Install it with one of these methods:"
    echo ""
    echo "  pip install git-filter-repo"
    echo "  # or"
    echo "  sudo apt install git-filter-repo  # Debian/Ubuntu"
    echo "  # or"
    echo "  brew install git-filter-repo  # macOS"
    echo ""
    exit 1
fi

# List files that will be removed (for confirmation)
echo ""
echo "Files that will be removed from git history:"
git ls-files tests/sample-data/ | grep -v README.md
echo ""

read -p "Continue? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

# Remove files from history (keeping README.md)
echo ""
echo "Removing files from git history..."
git filter-repo \
    --path tests/sample-data/ \
    --invert-paths \
    --path tests/sample-data/README.md \
    --force

echo ""
echo "=========================================="
echo "Done! Files removed from git history."
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Verify the repository looks correct: git log --oneline"
echo "  2. Check that files are gone: git ls-files tests/sample-data/"
echo "  3. Force push to update remote (WARNING: This rewrites remote history):"
echo "     git push origin --force --all"
echo "     git push origin --force --tags"
echo ""
echo "IMPORTANT:"
echo "  - All collaborators must re-clone the repository"
echo "  - Any forks/PRs will need to be rebased"
echo "  - Consider creating a new branch first to test"
echo ""
