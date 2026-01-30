# Building Packages

Datui can be packaged for Debian/Ubuntu (`.deb`), Fedora/RHEL (`.rpm`), and Arch Linux (AUR).

## Prerequisites

- **Rust**: Install via [rustup](https://rustup.rs/)
- **Python 3**: For running the build script
- **Cargo packaging tools**: Install as needed:

```bash
cargo install cargo-deb           # For .deb packages
cargo install cargo-generate-rpm  # For .rpm packages
cargo install cargo-aur           # For AUR packages
```

## Building Packages

Run from the repository root:

```bash
# Build a .deb package (Debian/Ubuntu)
python3 scripts/build_package.py deb

# Build a .rpm package (Fedora/RHEL)
python3 scripts/build_package.py rpm

# Build AUR package (Arch Linux)
python3 scripts/build_package.py aur
```

The script automatically:
1. Runs `cargo build --release`
2. Generates and compresses the manpage
3. Invokes the appropriate cargo packaging tool
4. Reports the output file locations

### Options

- `--no-build`: Skip `cargo build --release` (use when artifacts already exist)
- `--repo-root PATH`: Specify repository root (default: auto-detected via git)

```bash
# Example: build .deb without rebuilding (artifacts must exist)
python3 scripts/build_package.py deb --no-build
```

## Output Locations

| Package | Output Directory | Example Filename |
|---------|-----------------|------------------|
| deb | `target/debian/` | `datui_0.2.11-dev-1_amd64.deb` |
| rpm | `target/generate-rpm/` | `datui-0.2.11-dev-1.x86_64.rpm` |
| aur | `target/cargo-aur/` | `PKGBUILD`, `datui-0.2.11-dev-x86_64.tar.gz` |

## CI and Releases

The same script is used in GitHub Actions:
- **CI** (`ci.yml`): Builds and uploads dev packages (`.deb`, `.rpm`, `.tar.gz`) on push to `main`
- **Release** (`release.yml`): Attaches `.deb`, `.rpm`, and Arch `.tar.gz` to GitHub releases

### Arch Linux Installation

Arch users can install from the release tarball:

```bash
# Download the tarball from a release, then extract and install
tar xf datui-X.Y.Z-x86_64.tar.gz
sudo install -Dm755 datui /usr/bin/datui
sudo install -Dm644 target/release/datui.1.gz /usr/share/man/man1/datui.1.gz
```

Or use the included `PKGBUILD` with `makepkg`.

## More Information

For detailed information about packaging metadata, policies, and AUR submission, see
[plans/packaging-deb-rpm-aur-plan.md](https://github.com/derekwisong/datui/blob/main/plans/packaging-deb-rpm-aur-plan.md).
