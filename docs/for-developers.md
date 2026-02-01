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

The repo is a **Cargo workspace** with three packages:

| Package       | Path               | Role |
|---------------|--------------------|------|
| **datui**     | (root)             | Core library; no binary. |
| **datui-cli** | `crates/datui-cli` | Shared CLI definitions (Args, etc.) and the **gen_docs** binary used by the docs build. |
| **datui-bin** | `crates/datui-bin` | The **datui** CLI binary (the one you run). |

**When to use `--workspace` or `-p`:**

- **From the repo root**, `cargo build` and `cargo test` build/test **only the root package** (datui, the library). They do **not** build or test `datui-bin` or `datui-cli` by default.
- To build or test **everything** (library + CLI + gen_docs), use:
  - `cargo build --workspace` (or `cargo build --workspace --tests` to include tests)
  - `cargo test --workspace`
- To build or run a **specific package**, use `-p <name>`:
  - `cargo build -p datui-bin` — build the datui CLI binary
  - `cargo run -p datui-bin` — run the CLI (same as `cargo run` when the root had the binary)
  - `cargo run -p datui-cli --bin gen_docs` — run the docs generator (used by the docs build script)

So: use **`--workspace`** when you want “all crates” (e.g. CI, full check, release). Use **`-p <package>`** when you want one crate (e.g. “just the CLI” or “just gen_docs”). You do **not** need `--workspace` for normal library-only work from the root.

## Compiling

Compile Datui using `cargo`:

```bash
# Build everything (library + CLI + gen_docs)
cargo build --workspace

# Build only the CLI binary (faster if you're iterating on the app)
cargo build -p datui-bin

# Release build of the CLI (what gets installed / packaged)
cargo build --release -p datui-bin
```

- The **datui** CLI binary is at `target/debug/datui` or `target/release/datui` (built from **datui-bin**).
- The **gen_docs** binary is built from **datui-cli** and is used by the documentation build; it only needs the CLI definitions, so it compiles quickly.

> The release build will take significantly longer to compile than debug. But, the release build is
> faster and has **significantly** smaller size.

## More Resources

- There is a [Setup Script](for-developers/contributing.md#setup-script) that will help you get your environment ready
- Learn how to [run the tests](for-developers/tests.md)
- Build OS packages (deb, rpm, AUR) with [Building Packages](for-developers/packaging.md)
- See the [Contributing Guide](for-developers/contributing.md) for more
