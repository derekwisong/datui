# Instructions to Wipe Git History

## ⚠️ WARNING
This will **permanently delete** all commit history. Make sure:
1. All your work is committed
2. You have a backup if needed
3. Any collaborators are aware (they'll need to re-clone)
4. You're okay with losing all historical commits, branches, PRs, etc.

## Steps

### 1. Ensure everything is committed and you're on main

```bash
# Make sure all changes are committed
git add -A
git commit -m "Save current state before history wipe"

# Switch to main branch
git checkout main

# Pull latest if needed
git pull origin main
```

### 2. Create orphan branch (no history)

```bash
# Create new orphan branch with no history
git checkout --orphan fresh-start

# Remove all tracked files (they'll be re-added)
git rm -rf .
```

### 3. Add all current files and commit

```bash
# Add all current files
git add .

# Make the initial commit
git commit -m "Initial commit"

# Or use a more descriptive message:
# git commit -m "Initial commit - datui v0.2.0"
```

### 4. Replace main branch with fresh-start

```bash
# Delete old main branch locally
git branch -D main

# Rename fresh-start to main
git branch -m main
```

### 5. Force push to GitHub

```bash
# Force push to replace remote main
git push -f origin main
```

### 6. Clean up (optional)

If you want to delete other branches on GitHub:

```bash
# List remote branches
git branch -r

# Delete specific remote branches (example)
git push origin --delete dev
git push origin --delete dev2
# ... etc for branches you don't need
```

## Alternative: Backup First

If you want to keep a backup of the old history:

```bash
# Before step 1, create a backup branch
git checkout main
git branch backup-old-history

# Then follow steps above

# To restore later (if needed):
# git checkout backup-old-history
# git checkout -b main
# git push -f origin main
```

## Verify

After pushing:

1. Check GitHub - you should see only 1 commit
2. Check commit history: `git log --oneline` (should show only your initial commit)
3. Verify all files are present: `git ls-files`

## If Something Goes Wrong

If you need to undo this locally (before force push):

```bash
# Go back to old main
git checkout main
# Or checkout a specific commit: git checkout <commit-hash>
```
