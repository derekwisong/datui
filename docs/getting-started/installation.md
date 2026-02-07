# Installation

Once installed, have a look at the [Quick Start Guide](quick-start.md).

## ✨ Quick Install for Linux and macOS

```bash
curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh
```

> Don't like piping to shell? See the alternative methods below.

## Releases

Download a ready-to-use copy from the [Latest Release][latest-release] on GitHub.

> Datui runs on Linux, macOS, and Windows

## Package Managers

### Homebrew (macOS)

Install via the [derekwisong/datui](https://github.com/derekwisong/homebrew-datui) tap:

```bash
brew tap derekwisong/datui
brew install datui
```

### Arch Linux (AUR)

Datui is available in the [Arch User Repository](https://aur.archlinux.org/packages/datui-bin)
as `datui-bin`. Install it with an AUR helper such as
[paru](https://github.com/Morganamilo/paru) or [yay](https://github.com/Jguer/yay):

```bash
paru -S datui-bin
```

or

```bash
yay -S datui-bin
```

### Pip

Get the module from PyPI and launch Datui right from a Python console.

```
pip install datui
```

> See [Python Module](../user-guide/python-module.md).

### RPM-based (Fedora, RedHat)

Get the link to the `.rpm` file for the release version you want from the [Latest Release][latest-release].

Use `dnf` to install that link.
```bash
dnf install https://github.com/derekwisong/datui/releases/download/vX.Y.Z/datui-X.Y.Z-1.x86_64.rpm
```

### Deb-based (Debian, Ubuntu)

Download the `.deb` file for the release version you want from the [Latest Release][latest-release].

Use `apt` to install that file:
```bash
apt install datui-X.Y.Z-1.x86_64.deb
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



[latest-release]: https://github.com/derekwisong/datui/releases/latest