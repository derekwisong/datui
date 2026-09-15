# Contributing

Contributions are welcome. Clone [the repository](https://github.com/derekwisong/datui),
run the [setup script](setup-script.md), and you are ready to build, test and
commit. This page describes what the script does, for doing it by hand.

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
commit, so CI never fails on them:

```bash
pre-commit install          # pre-commit is in scripts/requirements.txt
pre-commit run --all-files  # run them by hand
```

| Hook | Runs | On failure |
|---|---|---|
| `cargo-fmt` | `cargo fmt` | Formats the files; stage them and commit again |
| `cargo-clippy` | `cargo clippy --workspace --all-targets --locked -- -D warnings` | Fix the warnings and commit again |

`git commit --no-verify` skips them; `pre-commit autoupdate` updates them.

## Before opening a pull request

```bash
cargo fmt
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace           # see Tests for the fixtures it needs
./scripts/code/fuzz.sh replay    # if you touched a parser or matcher
```

Keep commits and pull request text terse. If you add a feature, update the
in-app help strings in `crates/datui-lib/src/help-strings/` and the relevant
page in `docs/`. If you add a config option, follow
[Adding Configuration Options](adding-configuration-options.md).

## Reporting

Bugs and feature requests go to the
[issue tracker](https://github.com/derekwisong/datui/issues). A suspected
vulnerability does not: see
[SECURITY.md](https://github.com/derekwisong/datui/blob/main/SECURITY.md).

Datui is MIT licensed and contributions are accepted under the same terms.
