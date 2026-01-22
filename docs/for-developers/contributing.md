# Contributing

Thank you for your interest in contributing to Datui!

Once you've gotten [the repo](https://www.github.com/derekwisong/datui) cloned
for the first time, follow the [Setup](#setup) instructions below to get started.

## Setup

**TLDR:** The entire setup process can be automated by running
```
python scripts/setup-dev.py
```

The script will:

- Set up the [Python Virtual Enviroment](#python-virtual-environment)
  - Updates it if it already exists
- Generates sample data needed to run the tests
- Set up the pre-requisites for and build the [documentation](documentation.md)

### Python Virtual Environment

There are Python scripts in the `/scripts` directory that are
used to do things like build test data, documentation, and demo gifs.

Setting up a virtual environment with dependencies for these scripts will
ensure you can run them all.

A common convention is to create a virtual environment in the `.venv/` directory
of the repository. In fact, the `.gitignore` is already set up to ignore this location
so that files there aren't added by mistake.

```bash
python -m venv .venv
```

Then activate the virtual environment.

```bash
source .venv/bin/activate
```

Once activated, install dependencies used to run the availble Python scripts.

```bash
pip install -r scripts/requirements.txt
```

You're now ready to run the [tests](tests.md).


### Pre-commit Hooks

To encourage consistency and quality, the CI build checks the source code of the
application for formatting and linter warnings.

This project uses [pre-commit](https://pre-commit.com/) to manage git pre-commit hooks 
which automatically run the same code quality checks in your repository before commits
are made.

#### Installing Pre-commit and Hooks

1. **Install pre-commit**:

   If you set up a Python virtual environment using the instructions above then you
   already have everything you need. **Activate it and skip this step.**

   Otherwise, install `pre-commit` using your desired method.

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

   **Note:** You only need the `pre-commit` command accessible when you need
   to use it to manually run or update the hooks. Once installed into your repo, 
   the hooks themselves do not require `pre-commit`.

   See the `pre-commit` documentation for more information about its features.

The following hooks are configured:

- **cargo-fmt**: Automatically formats Rust code with `cargo fmt`
  - If code needs formatting, it will be formatted and the commit will fail
  - Stage the formatted changes and commit again

- **cargo-clippy**: Runs `cargo clippy --all-targets -- -D warnings`
  - Fails if clippy finds any warnings
  - Fix them and commit again

Hooks run automatically when you `git commit`. If any hook fails, the commit is aborted.

#### Running Hooks

Run all hooks manually:
```bash
pre-commit run --all-files
```

Run a specific hook:
```bash
pre-commit run cargo-fmt --all-files
pre-commit run cargo-clippy --all-files
```

#### Skipping Hooks

If you need to skip hooks for a specific commit (not recommended):
```bash
git commit --no-verify -m "Emergency fix"
```

#### Updating hooks

Update hook versions and configurations:
```bash
pre-commit autoupdate
```

#### Troubleshooting

**Hook not running?**
- Make sure you ran `pre-commit install`
- Check `.git/hooks/pre-commit` exists

**Hooks too slow?**
- Only changed files are checked by default
- Use `SKIP=hook-name git commit` to skip specific hooks
