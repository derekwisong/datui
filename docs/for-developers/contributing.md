# Set up and contribute

## Setup

From a checkout with Rust and Python installed, run:

```bash
python scripts/setup_dev.py
```

The script creates `.venv`, installs script and wheel-building dependencies,
installs pre-commit hooks, prepares test data and builds the documentation.
It can be rerun. To prepare only the fixtures needed by Rust tests, use
`./scripts/dev/setup-test-data.sh` instead.

For manual setup, use the commands below.

## Python environment

The scripts in `scripts/` generate test data, build the docs and record the
demos. They need a virtual environment with `scripts/requirements.txt`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

`.venv/` is gitignored, and the test harness finds `.venv/bin/python` on its
own, so activating is optional after this.

## Pre-commit hooks

CI rejects code that is not formatted or that has clippy warnings. The
[pre-commit](https://pre-commit.com/) hooks run the same checks before each
commit to catch those problems locally:

```bash
pre-commit install          # pre-commit is in scripts/requirements.txt
pre-commit run --all-files  # run them by hand
```

| Hook | Runs | On failure |
|---|---|---|
| `cargo-fmt` | `cargo fmt --check` | Run `cargo fmt`, then stage the changes |
| `cargo-clippy` | `cargo clippy --workspace --all-targets --locked -- -D warnings` | Fix the warnings and commit again |

The hooks also check trailing whitespace and unexpectedly large files.

## Before opening a pull request

```bash
cargo fmt
cargo clippy --workspace --all-targets --locked -- -D warnings
./scripts/dev/test.sh integration TARGET FILTER  # select the changed behavior's tests
./scripts/code/fuzz.sh replay    # if you touched a parser or matcher
```

Use the [test selection policy](tests.md#select-the-checks) to broaden related
tests. Run `./scripts/dev/test.sh full` for cross-cutting changes, shared test
infrastructure or test reorganization. Isolated changes can use scoped local
tests with the full workspace covered by CI; state the checks run in the PR.

Keep commits and pull request text terse. If you add a feature, update the
in-app help strings in `crates/datui-lib/src/help-strings/` and the relevant
page in `docs/`. If you add a config option, follow
[Adding Configuration Options](adding-configuration-options.md).

## Workflow timeouts

Every job sets `timeout-minutes`, and every apt step its own 5-minute limit, so a
hang fails fast instead of holding a required check. Size a job's limit at 1.5×
its slowest recent run or more (`gh run list --workflow FILE --limit 50`, then
`gh run view RUN_ID --json jobs`).

## Reporting

Bugs and feature requests go to the
[issue tracker](https://github.com/derekwisong/datui/issues). A suspected
vulnerability does not: see
[SECURITY.md](https://github.com/derekwisong/datui/blob/main/SECURITY.md).

Datui is MIT licensed and contributions are accepted under the same terms.
