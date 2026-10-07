# Set up and contribute

<a id="setup"></a>

From a checkout, with Rust and Python installed:

```bash,repo
python scripts/setup_dev.py
```

It creates `.venv`, installs `scripts/requirements.txt` (and the wheel-building
tools on Linux and macOS), installs the pre-commit hooks and the mdBook version
CI uses, generates the test fixtures and builds the docs. It can be rerun. For
the Rust tests' fixtures alone, run `./scripts/dev/setup-test-data.sh`.

## Set up by hand

```bash,repo
python -m venv .venv
.venv/bin/pip install -r scripts/requirements.txt
.venv/bin/pre-commit install
```

`.venv/` is gitignored, and the test harness and scripts find
`.venv/bin/python` on their own, so it need not be activated.

## Pre-commit hooks

CI rejects unformatted code and any clippy warning; the hooks run the same
checks before each commit. `.venv/bin/pre-commit run --all-files` runs them by
hand.

| Hook | Runs | On failure |
|---|---|---|
| `cargo-fmt` | `cargo fmt --check` | Run `cargo fmt`, stage the changes |
| `cargo-clippy` | `cargo clippy --workspace --all-targets --locked -- -D warnings` | Fix the warnings; never add an `#[allow]` |
| `check-added-large-files`, `trailing-whitespace` | pre-commit's own | As it says |

## Before opening a pull request

```bash,repo
cargo fmt
./scripts/dev/test.sh preflight
```

`preflight` checks formatting and runs workspace clippy, as CI does. Then:

| Changed | Also |
|---|---|
| A behavior | Its tests, chosen as [Run tests](tests.md#select-the-checks) says |
| A parser or matcher | `./scripts/dev/test.sh integration fuzz_corpus_test` |
| Keys or a screen | The screen's entries in the key registry, `crates/datui-cli/src/keys.rs`, then `cargo run -p datui-cli --bin gen_docs -- write` |
| A flag, setting, format or environment variable | `gen_docs write`, which rewrites the reference pages ([Build documentation](documentation.md#generated-pages)) |
| A config option | [Add configuration options](adding-configuration-options.md) |
| The docs | `scripts/docs/doc_examples.py` and `lint_docs.py` ([Build documentation](documentation.md#run-the-checks)) |
| Something users should hear about | A line in `release-notes/v<next>.md` |

Run `./scripts/dev/test.sh full` for cross-cutting changes. Otherwise run the
scoped checks, let CI cover the workspace, and say in the PR what ran. Keep
commit and PR text terse.

## Rust toolchain

`rust-toolchain.toml` pins the Rust that builds and lints datui, locally and in
CI; `rustup install` in the checkout fetches it. Bump it on purpose, in its own
pull request with whatever the new clippy asks for. CI's MSRV job checks
`rust-version` separately.

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
