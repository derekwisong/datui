# Contributing

Contributions are welcome. Everything you need is in the
[developer guide](https://derekwisong.github.io/datui/latest/for-developers/contributing.html),
source at [docs/for-developers/contributing.md](docs/for-developers/contributing.md):
setup, tests, what the pre-commit hooks enforce, and how changes land.

## First build

Prerequisites:

| Tool | Notes |
|---|---|
| Rust, through [rustup](https://rustup.rs) | `rust-toolchain.toml` picks the version; rustup fetches it on first use |
| Python 3.10 or newer | For the test fixtures and the docs scripts |
| A C compiler and `pkg-config` | `build-essential pkg-config` on Debian and Ubuntu; Xcode command line tools on macOS |
| [uv](https://docs.astral.sh/uv/) (optional) | Makes setup faster; pip is used without it |

Then, from a clone:

```bash
./scripts/dev/test.sh setup   # .venv, the pre-commit hooks, the test fixtures
cargo build                   # target/debug/datui
./scripts/dev/test.sh full    # the whole suite
```

A cold `cargo build` takes about 3 minutes on a 6-job build, and building every
test executable about as long again; budget 12 GB for `target/` and 1.5 GB for
Cargo's download cache. Later builds are incremental.

On Windows, use Git Bash for `scripts/dev/test.sh`, or run the steps directly:
`python scripts\setup_dev.py`, `cargo build`, `cargo test --workspace`. The
fixtures need no Bash.

`./scripts/dev/test.sh --help` lists every check CI makes and the command that
makes it locally.

## Reporting

Bugs and feature requests go to
[the issue tracker](https://github.com/derekwisong/datui/issues).

A suspected vulnerability does not. [SECURITY.md](SECURITY.md) has the private
channel and the scope.

datui is MIT licensed; see [LICENSE](LICENSE). Contributions are accepted under
the same terms.
