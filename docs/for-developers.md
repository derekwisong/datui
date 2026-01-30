# For Developers

- Written in [Rust](https://www.rust-lang.org/)
- Terminal UI made with [Ratatui](https://github.com/ratatui/ratatui)
- Powered by [Polars](https://github.com/pola-rs/polars)
- Documented using [mdBook](https://rust-lang.github.io/mdBook)
- Demo GIFs created with [vhs](https://github.com/charmbracelet/vhs)

## Install Rust

If you don't have Rust installed, please see the
[Rust Installation Instructions](https://rust-lang.org/tools/install/).

## Compiling

Compile Datui using `cargo`:

```bash
cargo build              # Debug build (fast build, large binary, debugging extras)
cargo build --release    # Optimized release build (slow build, small binary, optimized)
```

- The debug build will be available in the `target/debug` directory
- The release build will be available in the `target/release` directory

> The release build will take significantly longer to compile than debug. But, the release build is
> faster and has **significantly** smaller size.

## More Resources

- There is a [Setup Script](for-developers/contributing.md#setup-script) that will help you get your environment ready
- Learn how to [run the tests](for-developers/tests.md)
- Build OS packages (deb, rpm, AUR) with [Building Packages](for-developers/packaging.md)
- See the [Contributing Guide](for-developers/contributing.md) for more
