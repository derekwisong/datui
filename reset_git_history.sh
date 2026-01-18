#!/bin/bash
# Script to wipe git history and create a fresh initial commit
# 
# Prerequisites:
# 1. Delete the repository on GitHub (Settings -> Danger Zone -> Delete)
# 2. Recreate the repository on GitHub (same name, empty)
# 3. Run this script from the repository root

set -e  # Exit on error

echo "⚠️  WARNING: This will delete all git history!"
echo "Make sure you've deleted and recreated the GitHub repository first."
echo ""
read -p "Continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Creating orphan branch..."

# Create orphan branch with no history
git checkout --orphan fresh-start

# Clear the staging area (safer than git rm -rf, which can delete files)
# This removes any files that might be staged from the previous branch
git reset

# Add all current files (respects .gitignore)
echo "Adding all files..."
git add -A

# Check if there are any files to commit
if [ -z "$(git diff --cached --name-only)" ]; then
    echo "Error: No files to commit. Make sure you're in the repository root."
    exit 1
fi

# Make the initial commit
echo "Creating initial commit..."
git commit -m "Initial commit - datui v0.2.0"

# Delete old main branch if it exists
git branch -D main 2>/dev/null || true

# Rename current branch to main
git branch -m main

echo ""
echo "✅ Local repository reset complete!"
echo ""
echo "Next steps:"
echo "1. Make sure you've created an empty repository on GitHub with the same name"
echo "2. Verify your remote: git remote -v"
echo "3. Push with: git push -u origin main --force"
echo ""
