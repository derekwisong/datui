# Installation

You can obtain Datui from [pre-built releases](#installing-a-pre-built-release) or by
[compiling from source](#compiling-from-source).

Once installed, have a look at the [Quick Start Guide](quick-start.md).

## Installing a Pre-built Release

To acquire a pre-built copy of Datui, download one from the
[Datui Releases Page on GitHub][datui-releases].

## Package Managers

### Pip

Datui can be installed on Linux and Windows
using pip.

```
pip install datui
```

> Datui can be used as a python module to view
> Polars DataFrame and LazyFrame in the terminal.
>
> See [Python Module](../user-guide/python-module.md).

### Arch Linux (AUR)

Datui is available in the [Arch User Repository](https://aur.archlinux.org/) as `datui-bin`.
Install it with an AUR helper such as [paru](https://github.com/Morganamilo/paru) or [yay](https://github.com/Jguer/yay):

```bash
paru -S datui-bin
```

or

```bash
yay -S datui-bin
```

### RPM-based (Fedora, RedHat)

> Datui is not yet available in the any of the offical repositores for RPM-based distributions.

Get the link to the `.rpm` file for the release version you want from the [Releases Page][datui-releases].

Use `dnf` to install that link.
```bash
dnf install https://github.com/derekwisong/datui/releases/download/vX.Y.Z/datui-X.Y.Z-1.x86_64.rpm
```

### Deb-based (Debian, Ubuntu)

> Datui is not yet available in the any of the offical repositores for Deb-based distributions.

Download the `.deb` file for the release version you want from the [Releases Page][datui-releases].

Use `apt` to install that file:
```bash
apt install ./datui-X.Y.Z-1.x86_64.deb
```

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


[datui-releases]: https://github.com/derekwisong/datui/releases