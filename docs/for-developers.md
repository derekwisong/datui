# For Developers

- Written in [Rust](https://www.rust-lang.org/)
- Terminal UI made with [Ratatui](https://github.com/ratatui/ratatui)
- Powered by [Polars](https://github.com/pola-rs/polars)
- Documented using [mdBook](https://rust-lang.github.io/mdBook)
- Demo GIFs created with [vhs](https://github.com/charmbracelet/vhs)

## Install Rust

If you don't have Rust installed, please see the
[Rust Installation Instructions](https://rust-lang.org/tools/install/).

## Workspace and crates

The repo is a **Cargo workspace**. The **root package** is the CLI binary; the core logic lives in a library crate:

| Package        | Path                 | Role |
|----------------|----------------------|------|
| **datui**      | (root)               | The **datui** CLI binary (the one you run). `cargo build` and `cargo run` from the root build/run it. |
| **datui-lib**  | `crates/datui-lib`   | Core library (TUI, data handling, config, etc.). |
| **datui-cli**  | `crates/datui-cli`   | Shared CLI definitions (Args, etc.) and the **gen_docs** binary used by the docs build. |
| **datui-pyo3** | `crates/datui-pyo3`  | Python bindings. See [Python Bindings](for-developers/python-bindings.md) |

**From the repo root:**

- `cargo build` — build the datui CLI binary (and its dependency, datui-lib).
- `cargo run` — run the CLI (e.g. `cargo run -- --help`).
- `cargo build --workspace` — build all workspace packages (root + crates/datui-lib + crates/datui-cli).
- `cargo test --workspace` — test all workspace packages.
- `cargo run -p datui-cli --bin gen_docs` — run the docs generator (used by the docs build script).

No special config (e.g. `default-members` or `.cargo/config`) is needed; the root package is the binary.

## Compiling

Compile Datui using `cargo`:

```bash
# Build the CLI binary (default from root)
cargo build

# Build everything (CLI + library + gen_docs)
cargo build --workspace

# Release build of the CLI (what gets installed / packaged)
cargo build --release
```

- The **datui** CLI binary is at `target/debug/datui` or `target/release/datui` (built from the root package).
- The **gen_docs** binary is built from **datui-cli** and is used by the documentation build.
- **datui-pyo3** is the Python binding crate; it is **not** a workspace member. See [Python Bindings](for-developers/python-bindings.md) for how to build and test it.

> The release build will take significantly longer to compile than debug. But, the release build is
> faster and has **significantly** smaller size.

## More Resources

- The [Setup Script](for-developers/setup-script.md) will help you get your environment ready
- Learn how to [run the tests](for-developers/tests.md)
- [Python Bindings](for-developers/python-bindings.md) — build, test, and run the Python extension
- Build OS packages (deb, rpm, AUR) with [Building Packages](for-developers/packaging.md)
- See the [Contributing Guide](for-developers/contributing.md)
