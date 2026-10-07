# Set up and contribute

<a id="setup"></a>

From a checkout, with Rust and Python 3.10+ installed (the
[first build](https://github.com/derekwisong/datui/blob/main/CONTRIBUTING.md#first-build)
lists the prerequisites):

```bash,repo
./scripts/dev/test.sh setup
```

On Windows without Git Bash, run `python scripts\setup_dev.py`, which is what
`setup` runs. It creates `.venv` (with uv when installed, otherwise
`python -m venv`), installs `scripts/requirements.txt`, installs the pre-commit
hooks and generates the test fixtures. Rerunning it is cheap.

| Option | Adds |
|---|---|
| `--wheel` | maturin and pytest, for [the Python package](python-bindings.md) |
| `--docs` | mdBook at CI's version, and a docs build into `book/` |
| `--force` | Regenerates the fixtures even when current |
| `--no-hooks` | Skips the pre-commit hooks |

`.venv/` is gitignored, and the test harness and scripts find
`.venv/bin/python` on their own, so it need not be activated.

## Commands

`./scripts/dev/test.sh` is the one entry point; `--help` lists every command
and `--print` shows what one runs. Each check CI makes has one:

| CI checks | Locally |
|---|---|
| Formatting and clippy (`check`) | `./scripts/dev/test.sh lint` |
| The oldest supported Rust (`msrv`) | `./scripts/dev/test.sh msrv` |
| Tests (`linux`) | `./scripts/dev/test.sh ci` |
| Docs and their scripts (`python`) | `./scripts/dev/test.sh docs` |
| The Python package (`python`) | `./scripts/dev/test.sh python` |
| No default features | `./scripts/dev/test.sh features none` |

## Pre-commit hooks

CI rejects unformatted code and any clippy warning; the hooks run the same
checks before each commit. `.venv/bin/pre-commit run --all-files` runs them by
hand.

| Hook | Runs | On failure |
|---|---|---|
| `cargo-fmt` | `scripts/dev/test.sh fmt --check` | Run `scripts/dev/test.sh fmt`, stage the changes |
| `cargo-clippy` | `scripts/dev/test.sh clippy` | Fix the warnings; never add an `#[allow]` |
| `check-added-large-files`, `trailing-whitespace` | pre-commit's own | As it says |

## Before opening a pull request

```bash,repo
./scripts/dev/test.sh fmt
./scripts/dev/test.sh lint
```

`lint` checks formatting and runs clippy in the workspace, the fuzz targets and
`crates/datui-pyo3`, as CI does. Then it runs
[ruff](https://docs.astral.sh/ruff/) on the Python scripts (`ruff.toml`),
[shellcheck](https://www.shellcheck.net) on the shell scripts and
[typos](https://github.com/crate-ci/typos) on everything (`_typos.toml` holds
the words spelled on purpose), each only when installed; CI installs all three, at
the versions in `scripts/requirements-lint.txt` and `.github/tool-versions`. Then:

| Changed | Also |
|---|---|
| A behavior | Its tests, chosen as [Run tests](tests.md#select-the-checks) says |
| A parser or matcher | `./scripts/dev/test.sh integration fuzz_corpus_test` |
| Keys or a screen | The screen's entries in the key registry, `crates/datui-cli/src/keys.rs`, then `cargo run -p datui-cli --bin gen_docs -- write` |
| A flag, setting, format or environment variable | `gen_docs write`, which rewrites the reference pages ([Build documentation](documentation.md#generated-pages)) |
| A config option | [Add configuration options](adding-configuration-options.md) |
| The docs | `./scripts/dev/test.sh docs`, and the examples ([Build documentation](documentation.md#run-the-checks)) |
| The Python package | `./scripts/dev/test.sh python` |
| Something users should hear about | A line in `release-notes/v<next>.md` |

Run `./scripts/dev/test.sh ci` (or `full`) for cross-cutting changes. Otherwise
run the scoped checks, let CI cover the workspace, and say in the PR what ran.
Keep commit and PR text terse.

## Rust toolchain

`rust-toolchain.toml` pins the Rust that builds and lints datui, locally and in
CI; `rustup install` in the checkout fetches it. Bump it on purpose, in its own
pull request with whatever the new clippy asks for. CI's MSRV job checks
`rust-version` separately (`./scripts/dev/test.sh msrv`).

## CI setup

Jobs set up through `.github/actions/setup` (Rust, apt packages, the cargo
cache, Python, the test fixtures) and install cargo tools through
`.github/actions/install-tools`, at the versions in `.github/tool-versions`.

## Workflow timeouts

Every job sets `timeout-minutes`, and every apt step its own 5-minute limit, so
a hang fails fast instead of holding a required check. Size a job's limit at
1.5× its slowest recent run or more (`gh run list --workflow FILE --limit 50`,
then `gh run view RUN_ID --json jobs`).

## Reporting

Bugs and feature requests go to the
[issue tracker](https://github.com/derekwisong/datui/issues). A suspected
vulnerability does not: see
[SECURITY.md](https://github.com/derekwisong/datui/blob/main/SECURITY.md).

datui is MIT licensed, and contributions are accepted under the same terms.
