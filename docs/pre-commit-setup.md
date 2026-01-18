# Pre-commit Hooks Setup

This project uses [pre-commit](https://pre-commit.com/) to automatically run code quality checks before commits.

## Installation

1. **Install pre-commit**:
   ```bash
   # Using pip
   pip install pre-commit
   
   # Or using homebrew (macOS)
   brew install pre-commit
   
   # Or using conda
   conda install -c conda-forge pre-commit
   ```

2. **Install the git hooks**:
   ```bash
   pre-commit install
   ```

   This installs the hooks into `.git/hooks/` so they run automatically on commit.

## Hooks

The following hooks are configured:

- **cargo-fmt**: Automatically formats Rust code with `cargo fmt`
  - If code needs formatting, it will be formatted and the commit will fail
  - Stage the formatted changes and commit again

- **cargo-clippy**: Runs `cargo clippy --all-targets -- -D warnings`
  - Fails if clippy finds any warnings
  - Fix any clippy warnings before committing

## Usage

### Automatic (after installation)

Hooks run automatically when you `git commit`. If any hook fails, the commit is aborted.

### Manual

Run all hooks manually:
```bash
pre-commit run --all-files
```

Run a specific hook:
```bash
pre-commit run cargo-fmt --all-files
pre-commit run cargo-clippy --all-files
```

### Skipping hooks

If you need to skip hooks for a specific commit (not recommended):
```bash
git commit --no-verify -m "Emergency fix"
```

### Updating hooks

Update hook versions and configurations:
```bash
pre-commit autoupdate
```

## Troubleshooting

**Hook not running?**
- Make sure you ran `pre-commit install`
- Check `.git/hooks/pre-commit` exists

**Hooks too slow?**
- Only changed files are checked by default
- Use `SKIP=hook-name git commit` to skip specific hooks

**Need to run hooks on CI?**
- Pre-commit hooks run automatically in CI if configured
- Or add `pre-commit run --all-files` to your CI workflow
