# Install datui

datui runs on Linux, macOS and Windows. Pick one method, check it with
`datui --version`, then go on to the [quick start](quick-start.md).

| System | Needs |
|---|---|
| Linux | glibc 2.28 or newer, on x86_64 or arm64: Debian 10, Ubuntu 20.04, RHEL 8 and later, and their derivatives. Alpine and other musl systems: [build from source](#from-source) |
| macOS | 10.12 on Intel, 11 on Apple silicon |
| Windows | Windows 10 or newer, x64 |

## Linux and macOS, one line

<!-- generated: install-script -->
```bash,install
curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh
```
<!-- end generated: install-script -->

The script installs the latest release for your platform:

| System | What it does |
|---|---|
| Debian, Ubuntu | Adds the [apt repository](#apt-repository) and its signing key with `sudo`, then installs the package with apt; `apt upgrade` keeps it current |
| Fedora, RHEL, Amazon Linux | Adds the [dnf repository](#dnf-repository) with `sudo`, then installs the package with dnf; `dnf upgrade` keeps it current |
| Arch Linux and derivatives, x86_64 | Asks, then installs `datui-bin` from the AUR with `yay` or `paru`; pacman owns it and the helper upgrades it. With neither, it says how and offers the archive |
| Other Linux, macOS | Unpacks the release archive: `datui` into `/usr/local/bin`, the manual pages into `/usr/local/share/man` |

It checks each download against the release's `SHA256SUMS`, asks before it
changes apt or dnf or runs an AUR helper (with no terminal to ask on, it goes ahead),
and runs the installed binary before it reports success. Options go after `sh -s --`:

| Option | Effect |
|---|---|
| `--user` | Install into `~/.local/bin` (or `$XDG_BIN_HOME`) without root; the default where there is no `sudo` |
| `-y`, `--yes` | Answer yes to every question |
| `--no-verify` | Install without checking the download against `SHA256SUMS` |
| `-h`, `--help` | Print the options and exit without installing |

Package-manager and binary-download options are below; [Uninstall](#uninstall)
says how to remove each.

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
| Debian, Ubuntu | [Apt repository](#apt-repository) |
| Fedora, RHEL | [Dnf repository](#dnf-repository) |
| [Arch Linux, AUR](https://aur.archlinux.org/packages/datui-bin) | `yay -S datui-bin   # or: paru -S datui-bin` |
| [macOS, Linux, Homebrew](https://github.com/derekwisong/homebrew-datui) | `brew tap derekwisong/datui && brew trust derekwisong/datui && brew install datui` |
| [Python, PyPI](https://pypi.org/project/datui/) | `pip install datui` |
| [Rust, crates.io](https://crates.io/crates/datui) | `cargo install datui --locked` |
| Binaries | Linux, macOS and Windows binaries, `.deb`, `.rpm` and Arch tarballs on the [latest release](https://github.com/derekwisong/datui/releases/latest) |
<!-- end generated: install-table -->

Homebrew needs `brew trust` before it will install from a third-party tap. The
pip package installs the `datui` command and the [Python module](../user-guide/python-module.md).
`cargo install` compiles datui, which takes a C compiler and some minutes; with
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall) installed,
`cargo binstall datui` fetches the release archive for your platform instead.

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

### Dnf repository

Fedora, RHEL 8 and later, Amazon Linux 2023 and their derivatives. Add the
repository once:

<!-- generated: install-dnf -->
```bash,install
sudo curl -fsSL -o /etc/yum.repos.d/datui.repo https://derekwisong.github.io/datui-apt/rpm/datui.repo
sudo dnf install datui
```
<!-- end generated: install-dnf -->

dnf asks to import the signing key the first time. After that, `dnf upgrade`
keeps datui current.

## Pre-built binaries

Every release on [GitHub][latest-release] carries these, with a `SHA256SUMS`
file and its Sigstore signature. `<VERSION>` is the release's version, such as
`0.4.0`:

| Asset | For |
|---|---|
| `datui-v<VERSION>-x86_64-unknown-linux-gnu.tar.gz` | Linux x86_64, glibc 2.28 or newer |
| `datui-v<VERSION>-aarch64-unknown-linux-gnu.tar.gz` | Linux arm64, glibc 2.28 or newer |
| `datui_<VERSION>-1_amd64.deb`, `datui_<VERSION>-1_arm64.deb` | Debian, Ubuntu |
| `datui-<VERSION>-1.x86_64.rpm`, `datui-<VERSION>-1.aarch64.rpm` | Fedora, RHEL |
| `datui-v<VERSION>-aarch64-apple-darwin.tar.gz` | macOS, Apple silicon |
| `datui-v<VERSION>-x86_64-apple-darwin.tar.gz` | macOS, Intel |
| `datui-v<VERSION>-x86_64-pc-windows-msvc.zip` | Windows |
| `PKGBUILD` | The AUR package's build file |
| `datui-<VERSION>-cp38-abi3-*.whl` | The Python wheels PyPI serves |

An archive holds `datui`, `LICENSE`, `man/` and `completions/`. Unpack one and
put `datui` somewhere on your `PATH`, or install a package:

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

Example datasets lists only what the build can open.

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
| Terminal | Windows Terminal and the classic console window draw 24-bit color; with "Use legacy console" checked, 16 colors. Both draw datui's Unicode glyphs whatever the code page; `[display] unicode = "never"` draws ASCII ([Glyphs or ASCII](../user-guide/configuration.md#glyphs-or-ascii)) |
| Config file | `%APPDATA%\datui\config.toml` |
| Format specs | `%APPDATA%\datui\formats` ([Format specs](../formats/format-specs.md)) |
| Cache and log | `%LOCALAPPDATA%\datui` |
| `~` | `datui ~\data\a.csv` opens from your user folder in cmd and PowerShell too |
| Mouse | <kbd>Shift</kbd>+drag selects text in Windows Terminal while datui has the mouse ([Mouse and text selection](../user-guide/configuration.md#mouse-and-text-selection)) |
| Globs | cmd and PowerShell pass `*.csv` to datui as typed; quote it in Git Bash, as on Linux |
| A file open in another program | A spreadsheet app or database that holds a file exclusively stops datui reading it: datui says so. Close it there and reopen |
| A file datui has open | datui reads files through memory maps, and Windows lets no program truncate, rename or delete a file while it is mapped. A program rotating a log datui has open can fail; close it in datui first (<kbd>Ctrl+O</kbd> or <kbd>q</kbd>) |

## Uninstall

| Installed by | Remove with |
|---|---|
| The script, on Debian or Ubuntu | `sudo apt remove datui`; then `sudo rm /etc/apt/sources.list.d/datui.list /usr/share/keyrings/datui-archive-keyring.gpg` drops the repository |
| The script, on Fedora, RHEL or Amazon Linux | `sudo dnf remove datui`; then `sudo rm /etc/yum.repos.d/datui.repo` drops the repository |
| The script, elsewhere | `sudo rm /usr/local/bin/datui /usr/local/share/man/man*/datui*` |
| The script with `--user` | `rm ~/.local/bin/datui ~/.local/share/man/man*/datui*` |
| A `.deb` or `.rpm` | `sudo apt remove datui` or `sudo dnf remove datui` |
| WinGet | `winget uninstall derekwisong.datui` |
| Homebrew | `brew uninstall datui`, then `brew untap derekwisong/datui` |
| pip | `pip uninstall datui` |
| cargo | `cargo uninstall datui` |
| AUR | `sudo pacman -R datui-bin` |
| An archive | Delete `datui` from where you put it |

None of these touch your config, themes, saved views, cache or log. `datui
config path` names the config files; the directories are:

| | Config | Cache and log |
|---|---|---|
| Linux | `~/.config/datui` | `~/.cache/datui` |
| macOS | `~/Library/Application Support/datui` | `~/Library/Caches/datui` |
| Windows | `%APPDATA%\datui` | `%LOCALAPPDATA%\datui` |

[latest-release]: https://github.com/derekwisong/datui/releases/latest
