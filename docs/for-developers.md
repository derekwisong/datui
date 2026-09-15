# For Developers

Datui is written in [Rust](https://www.rust-lang.org/). The UI is
[Ratatui](https://github.com/ratatui/ratatui), the data engine is
[Polars](https://github.com/pola-rs/polars), the docs are
[mdBook](https://rust-lang.github.io/mdBook) and the demos are recorded with
[vhs](https://github.com/charmbracelet/vhs).

## Build

Install Rust with [rustup](https://rustup.rs/), then:

```bash
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build              # debug binary at target/debug/datui
cargo build --release    # the binary that gets installed and packaged
cargo run -- data.csv    # build and run
```

The release build takes much longer and produces a much smaller, faster binary.

## Workspace

| Package | Path | Role |
|---|---|---|
| `datui` | repository root | The CLI binary. `cargo build` and `cargo run` build and run it |
| `datui-lib` | `crates/datui-lib` | Everything else: UI, data handling, config |
| `datui-cli` | `crates/datui-cli` | Shared CLI definitions, and `gen_docs`, which writes the command-line reference |
| `datui-pyo3` | `crates/datui-pyo3` | Python bindings. Not a workspace member; see [Python Bindings](for-developers/python-bindings.md) |

`cargo build --workspace` and `cargo test --workspace` cover the first three.

## Guides

| Page | What it covers |
|---|---|
| [Setup Script](for-developers/setup-script.md) | One command that prepares a Python environment, hooks, test data and docs |
| [Tests](for-developers/tests.md) | Running the tests and generating the fixtures they need |
| [Contributing](for-developers/contributing.md) | Pre-commit hooks and how changes land |
| [Security Checks](for-developers/security-checks.md) | cargo-deny and zizmor, locally and in CI |
| [Fuzzing](for-developers/fuzzing.md) | The fuzz targets and how to run them |
| [Adding Configuration Options](for-developers/adding-configuration-options.md) | The seven places a new option touches |
| [Documentation](for-developers/documentation.md) | Building this site |
| [Generating the Demos](for-developers/demos.md) | Re-recording the GIFs |
| [Building Packages](for-developers/packaging.md) | deb, rpm, AUR and winget |
| [Python Bindings](for-developers/python-bindings.md) | Building and testing the extension |
| [PyPI Deployment](for-developers/pypi-deployment.md) | How the wheel is published |
