# Installation

You can obtain Datui from [pre-built releases](#installing-a-pre-built-release) or by
[comping from source](#compiling-from-source).

Once installed, have a look at the [Quick Start Guide](quick-start.md).

## Compiling from Source

Datui is built using [Rust](https://www.rust-lang.org/), leveraging its
[Cargo](https://doc.rust-lang.org/cargo/index.html) toolkit for compilation.

To compile a release-quality executable, clone the repository and use `cargo` to build:

```bash
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build --release
```

If desired, before building you could check out a specific release tag

```bash
git tag --list
git checkout <tag from the list>
cargo build --release
```

The `datui` executable can be found in the `target/release` directory.

Run it directly, or copy the file to a location on your `PATH` environment variable to make it
discoverable by your terminal.

### Using `cargo install`

You may use `cargo` to install Datui locally into Cargo's binary cache. (The `~/.cargo/bin`
directory on Linux).

```bash
cargo install --path .
```

> To run the application you will need to add the Cargo bin directory to your `PATH`.
>
> On Linux: `export PATH=$PATH:~/.cargo/bin`


## Installing a Pre-built Release

To acquire a pre-built copy of Datui, you may download one from the
[Datui Releases Page on GitHub](https://github.com/derekwisong/datui/releases).

## Package Managers

> Datui is not yet installable with package managers.
