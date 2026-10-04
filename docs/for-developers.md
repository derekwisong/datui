# Development overview

datui is Rust: [Ratatui](https://github.com/ratatui/ratatui) draws it,
[Polars](https://github.com/pola-rs/polars) reads and computes, the book is
[mdBook](https://rust-lang.github.io/mdBook) and the demos are recorded with
[VHS](https://github.com/charmbracelet/vhs).

```bash,repo
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build
cargo run -- --help
```

`cargo build` writes the debug binary to `target/debug/datui`;
`cargo build --release` builds the one that is packaged, slower to build and
faster to run. Install Rust with [rustup](https://rustup.rs/).

## Workspace

| Package | Path | Role |
|---|---|---|
| `datui` | repository root | The binary: `src/main.rs` parses the arguments and runs `datui_lib::run` |
| `datui-lib` | `crates/datui-lib` | Everything else: the app, its screens, the readers, config |
| `datui-cli` | `crates/datui-cli` | The clap `Args`, the option registry, the format descriptors, and `gen_docs`, which writes the generated docs |
| `datui-pyo3` | `crates/datui-pyo3` | The Python bindings. Not a workspace member; see [Build Python bindings](for-developers/python-bindings.md) |

`cargo build --workspace` and `cargo test --workspace` cover the first three.

## Guides

| Page | Covers |
|---|---|
| [Set up and contribute](for-developers/contributing.md) | The setup script, pre-commit hooks, what a pull request needs |
| [Run tests](for-developers/tests.md) | Choosing tests, fixtures, the heavy-run queue |
| [Build documentation](for-developers/documentation.md) | The book, its generated pages and its checked code blocks |
| [Check the examples](for-developers/examples.md) | The numbers the guides quote |
| [Record demos](for-developers/demos.md) | The GIFs |
| [Add configuration options](for-developers/adding-configuration-options.md) | The option registry |
| [Add a format](for-developers/adding-a-format.md) | A descriptor and a reader |
| [Run benchmarks](for-developers/benchmarks.md) | Time to first rows and peak memory |
| [Build Python bindings](for-developers/python-bindings.md) | The extension and its tests |
| [Build and publish packages](for-developers/packaging.md) | deb, rpm, AUR, PyPI, WinGet |
| [Security checks](for-developers/security-checks.md) | cargo-deny and zizmor |
| [Fuzzing](for-developers/fuzzing.md) | The fuzz targets |
| [Check glyph coverage](for-developers/glyph-audit.md) | Symbols and their ASCII twins |
