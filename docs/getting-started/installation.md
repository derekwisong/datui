# Installation

Datui runs on Linux, macOS and Windows. Pick one method, then check it with
`datui --version` and move on to the [Quick Start](quick-start.md).

## Linux and macOS, one line

```bash
curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh
```

The script downloads the latest release for your platform and installs it: a
`.deb` on Debian and Ubuntu, an `.rpm` on Fedora and RHEL, the binary elsewhere.
It asks before using `sudo`. Prefer not to pipe to a shell? Any method below
does the same thing.

## Package managers

| Platform | Command |
|---|---|
| macOS, [Homebrew](https://github.com/derekwisong/homebrew-datui) | `brew tap derekwisong/datui && brew trust derekwisong/datui && brew install datui` |
| Windows, WinGet | `winget install derekwisong.datui` |
| Arch Linux, [AUR](https://aur.archlinux.org/packages/datui-bin) | `paru -S datui-bin` or `yay -S datui-bin` |
| Debian, Ubuntu | See [apt repository](#apt-repository) below |
| Fedora, RHEL | `dnf install <url of the .rpm from the latest release>` |
| Python, [PyPI](https://pypi.org/project/datui/) | `pip install datui` |
| Rust, [crates.io](https://crates.io/crates/datui) | `cargo install datui --locked` |

Homebrew needs `brew trust` before it will install from a third-party tap. The
pip package installs the `datui` command and the [Python module](../user-guide/python-module.md).

### Apt repository

Add the signing key and source once:

```bash
curl -fsSL https://derekwisong.github.io/datui-apt/public.key | sudo gpg --dearmor -o /usr/share/keyrings/datui-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/datui-archive-keyring.gpg] https://derekwisong.github.io/datui-apt/ ./" | sudo tee /etc/apt/sources.list.d/datui.list
sudo apt update
sudo apt install datui
```

After that, `apt upgrade` keeps datui current.

## Pre-built binaries

Every release on [GitHub][latest-release] carries binaries for Linux, macOS
(Intel and Apple silicon) and Windows, plus `.deb`, `.rpm` and Arch tarballs.
Download, unpack, and put `datui` somewhere on your `PATH`.

```bash
# .deb
sudo apt install ./datui_X.Y.Z-1_amd64.deb
# .rpm
sudo dnf install https://github.com/derekwisong/datui/releases/download/vX.Y.Z/datui-X.Y.Z-1.x86_64.rpm
```

## From source

Needs a [Rust toolchain](https://www.rust-lang.org/tools/install).

```bash
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build --release --locked
```

The binary is `target/release/datui`. To build a specific release, check out
its tag first (`git tag --list`, then `git checkout vX.Y.Z`). To install into
`~/.cargo/bin` instead, run `cargo install --path . --locked` from the checkout.

Cloud storage support (S3, GCS, HTTP) is on by default. To leave it out:

```bash
cargo build --release --locked --no-default-features
```

[latest-release]: https://github.com/derekwisong/datui/releases/latest
