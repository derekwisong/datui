# Install datui

datui runs on Linux, macOS and Windows. Pick one method, check it with
`datui --version`, then go on to the [quick start](quick-start.md).

## Linux and macOS, one line

<!-- generated: install-script -->
```bash,install
curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh
```
<!-- end generated: install-script -->

The script downloads the latest release for your platform and installs it: a
`.deb` on Debian and Ubuntu, an `.rpm` on Fedora and RHEL, the binary elsewhere.
Package-manager and binary-download options are below.

### Without root

Where there is no `sudo`, as in Azure Cloud Shell, the script installs the binary
into `~/.local/bin` (or `$XDG_BIN_HOME`) instead, and says how to put that on your
`PATH` if it is not there. Pass `--user` to do the same on a machine that has
`sudo`:

```bash,install
curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh -s -- --user
```

## Package managers

<!-- generated: install-table -->
| Platform | Command |
|---|---|
| Windows, WinGet | `winget install derekwisong.datui` |
| [macOS, Homebrew](https://github.com/derekwisong/homebrew-datui) | `brew tap derekwisong/datui && brew trust derekwisong/datui && brew install datui` |
| [Python, PyPI](https://pypi.org/project/datui/) | `pip install datui` |
| [Rust, crates.io](https://crates.io/crates/datui) | `cargo install datui --locked` |
| [Arch Linux, AUR](https://aur.archlinux.org/packages/datui-bin) | `paru -S datui-bin` |
| Debian, Ubuntu | [Apt repository](#apt-repository) |
| Binaries | Linux, macOS and Windows binaries, `.deb`, `.rpm` and Arch tarballs on the [latest release](https://github.com/derekwisong/datui/releases/latest) |
<!-- end generated: install-table -->

On Fedora and RHEL, the one-line script installs the `.rpm`; or install it from
the release, as [below](#pre-built-binaries).

Homebrew needs `brew trust` before it will install from a third-party tap. The
pip package installs the `datui` command and the [Python module](../user-guide/python-module.md).

### Apt repository

Add the signing key and source once:

<!-- generated: install-apt -->
```bash,install
curl -fsSL https://derekwisong.github.io/datui-apt/public.key | sudo gpg --dearmor -o /usr/share/keyrings/datui-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/datui-archive-keyring.gpg] https://derekwisong.github.io/datui-apt/ ./" | sudo tee /etc/apt/sources.list.d/datui.list
sudo apt update
sudo apt install datui
```
<!-- end generated: install-apt -->

After that, `apt upgrade` keeps datui current.

## Pre-built binaries

Every release on [GitHub][latest-release] carries binaries for Linux (x86_64
and arm64), macOS (Intel and Apple silicon) and Windows, plus `.deb`, `.rpm`
and Arch tarballs.
Download, unpack, and put `datui` somewhere on your `PATH`. To install a
package instead, replace `<VERSION>` with the release's version, such as `0.4.0`:

```bash,template
sudo apt install ./datui_<VERSION>-1_amd64.deb
sudo dnf install https://github.com/derekwisong/datui/releases/download/v<VERSION>/datui-<VERSION>-1.x86_64.rpm
```

## From source

Needs a [Rust toolchain](https://www.rust-lang.org/tools/install), 1.95 or newer.

```bash,install
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build --release --locked
```

The binary is `target/release/datui`. To build a specific release, check out
its tag first (`git tag --list`, then `git checkout vX.Y.Z`). To install into
`~/.cargo/bin` instead, run `cargo install --path . --locked` from the checkout.

Five features are on by default. `--no-default-features` leaves them all out;
add back the ones you want with `--features`:

```bash,install
cargo build --release --locked --no-default-features --features sql,streaming
```

| Feature | Without it |
|---|---|
| `cloud` | S3, GCS and Azure URLs fail to open; no cloud sources on the home screen |
| `http` | HTTP(S) URLs fail to open |
| `sql` | The command line has no `sql:`; a view saved with SQL fails to apply |
| `sqlite` | SQLite databases fail to open |
| `streaming` | No Polars streaming engine: an export reads the whole view first, and a Data Quality read runs to its end on <kbd>Esc</kbd> |

Public datasets lists only what the build can open.

## Manual pages

The `.deb`, `.rpm`, AUR package, Homebrew formula and release archives install
the [manual pages](../reference/manual-pages.md): `man datui`, `man
datui-config`, `man 5 datui-config`, `man datui-keys`. `pip install datui` puts
them in the environment's `share/man`, which `man` searches while its `bin` is
on your `PATH`.

After `cargo install` or a build from source, `datui man` shows them, and this
installs them where `man` finds them for your user:

```bash
datui man --dir ~/.local/share/man
```

`datui man --list` lists the pages.

## Shell completions

The packages, Homebrew and the release archives (`completions/`) install the
completion scripts for bash, zsh and fish. Otherwise `datui completions SHELL`
prints the script for the flags, commands and format names. Set it up once per
shell:

| Shell | Setup |
|---|---|
| bash | `source <(datui completions bash)` in `~/.bashrc` |
| zsh | `source <(datui completions zsh)` in `~/.zshrc`, after `compinit` |
| fish | `datui completions fish > ~/.config/fish/completions/datui.fish` |
| PowerShell | `datui completions powershell \| Out-String \| Invoke-Expression` in your `$PROFILE` |
| elvish | `eval (datui completions elvish \| slurp)` in `~/.config/elvish/rc.elv` |

## Windows

```powershell,install
winget install derekwisong.datui
```

| | On Windows |
|---|---|
| Terminal | Windows Terminal and the classic console window draw 24-bit color; with "Use legacy console" checked, 16 colors. Windows Terminal draws datui's glyphs; the classic console draws ASCII unless its code page is UTF-8 (`chcp 65001`). `[display] unicode` overrides either way ([Glyphs or ASCII](../user-guide/configuration.md#glyphs-or-ascii)) |
| Config file | `%APPDATA%\datui\config.toml` |
| Format specs | `%APPDATA%\datui\formats` ([Format specs](../formats/format-specs.md)) |
| Cache and log | `%LOCALAPPDATA%\datui` |
| `~` | `datui ~\data\a.csv` opens from your user folder in cmd and PowerShell too |
| Mouse | <kbd>Shift</kbd>+drag selects text in Windows Terminal while datui has the mouse ([Mouse and text selection](../user-guide/configuration.md#mouse-and-text-selection)) |
| Globs | cmd and PowerShell pass `*.csv` to datui as typed; quote it in Git Bash, as on Linux |
| A file open in another program | A spreadsheet app or database that holds a file exclusively stops datui reading it: datui says so. Close it there and reopen |
| A file datui has open | datui reads files through memory maps, and Windows lets no program truncate, rename or delete a file while it is mapped. A program rotating a log datui has open can fail; close it in datui first (<kbd>Ctrl+O</kbd> or <kbd>q</kbd>) |

[latest-release]: https://github.com/derekwisong/datui/releases/latest
