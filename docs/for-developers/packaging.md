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
python3 scripts/packaging/build_package.py deb

# Build a .rpm package (Fedora/RHEL)
python3 scripts/packaging/build_package.py rpm

# Build AUR package (Arch Linux)
python3 scripts/packaging/build_package.py aur
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
python3 scripts/packaging/build_package.py deb --no-build
```

## License and metadata

All packages include the MIT license as required:

- **deb**: `[package.metadata.deb]` sets `license-file = ["LICENSE", "0"]`; cargo-deb installs it in the package.
- **rpm**: `[[package.metadata.generate-rpm.assets]]` includes `LICENSE` at `/usr/share/licenses/datui/LICENSE`.
- **aur**: `[package.metadata.aur]` `files` includes `["LICENSE", "/usr/share/licenses/datui/LICENSE"]`.
- **desktop entry**: `scripts/packaging/datui.desktop` installs to
  `/usr/share/applications/datui.desktop` in all three package formats. This is
  what puts datui in desktop launchers — including Omarchy's menu, which lists
  applications from the freedesktop database rather than anything Omarchy-specific.
  `tests/desktop_entry_test.rs` checks the file is valid and wired into every
  packager, because a broken entry fails silently: launchers just ignore it.
- **Python wheel**: `python/pyproject.toml` uses `license = { file = "LICENSE" }` and `sdist-include = ["LICENSE"]`. CI and release workflows copy the root `LICENSE` into `python/LICENSE`.

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
# Install runtime dependency (required for terminal rendering)
sudo pacman -S fontconfig
# Download the tarball from a release, then extract and install
tar xf datui-X.Y.Z-x86_64.tar.gz
sudo install -Dm755 datui /usr/bin/datui
sudo install -Dm644 target/release/datui.1.gz /usr/share/man/man1/datui.1.gz
sudo install -Dm644 LICENSE /usr/share/licenses/datui/LICENSE
```

Or use the included `PKGBUILD` with `makepkg` (it declares `fontconfig` as a dependency).

### AUR Release Workflow

To update the AUR package when you release a new version:

1. Checkout the release tag and build the AUR package:
   ```bash
   git checkout vX.Y.Z
   cargo build --release --locked
   python3 scripts/packaging/build_package.py aur --no-build
   ```

2. Generate `.SRCINFO` and copy to your AUR repo:
   ```bash
   cd target/cargo-aur
   makepkg --printsrcinfo > .SRCINFO
   cp PKGBUILD .SRCINFO /path/to/aur-datui-bin/
   ```

3. Commit and push to the AUR:
   ```bash
   cd /path/to/aur-datui-bin
   git add PKGBUILD .SRCINFO
   git commit -m "Upstream update: X.Y.Z"
   git push
   ```

Use **stable** release tags only (e.g. `v0.2.11`); the AUR package fetches the tarball from the GitHub release. Dev builds are available from the `dev` release tag.

### Automated AUR updates (GitHub Actions)

The release workflow can push PKGBUILD and .SRCINFO to the AUR automatically when you push a version tag. It publishes to the **datui-bin** AUR package (per AUR convention for pre-built binaries). It uses [KSXGitHub/github-actions-deploy-aur](https://github.com/KSXGitHub/github-actions-deploy-aur): the action clones the AUR repo, copies our PKGBUILD and tarball, runs `makepkg --printsrcinfo > .SRCINFO`, then commits and pushes via SSH.

**Required repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Description |
|--------|-------------|
| `AUR_SSH_PRIVATE_KEY` | Your SSH **private** key. Add the matching **public** key to your [AUR account](https://aur.archlinux.org/account/) (My Account → SSH Public Key). |
| `AUR_USERNAME` | Your AUR account name (used as git commit author). |
| `AUR_EMAIL` | Email for the AUR git commit (can be a noreply address). |

If these secrets are not set, the "Publish to AUR" step will fail. To disable automated AUR updates, remove or comment out that step in `.github/workflows/release.yml`.

## WinGet releases

The `publish-winget` job in `.github/workflows/publish-packages.yml` uses
[winget-releaser](https://github.com/vedantmgoyal9/winget-releaser), which drives
[komac](https://github.com/russellbanks/Komac) to open a manifest PR against
[microsoft/winget-pkgs](https://github.com/microsoft/winget-pkgs) from our fork at
`derekwisong/winget-pkgs`.

**Required repository secret:**

| Secret | Description |
|--------|-------------|
| `WINGET_TOKEN` | Classic PAT with `public_repo` scope. Fine-grained PATs do not work — they cannot open a cross-fork PR against a repo you don't own. |

At least one version of `derekwisong.datui` must already exist in winget-pkgs; the
action refuses to create a brand-new package.

### Recovering from "does not have the correct permissions to execute `UpdateRef`"

Before opening the PR, komac fast-forwards our fork from upstream. GitHub blocks any
ref update that touches `.github/workflows/` unless the token carries `workflow`
scope, and upstream winget-pkgs edits its own workflows every few weeks — so the sync
fails once enough time has passed since the last release. The error names a
permissions problem, but **`WINGET_TOKEN` is fine; do not rotate it.**

We can't just add the scope: GitHub's classic-PAT UI force-selects full `repo`
(private repos included) whenever `workflow` is checked.

The preflight step attempts the sync itself and, when blocked, fails fast with these
steps in the job log:

1. Open <https://github.com/derekwisong/winget-pkgs> and click **Sync fork** →
   **Update branch**. A browser session has permissions the PAT doesn't.
2. Re-run just the failed job:
   ```bash
   gh run rerun <run-id> --failed
   ```
3. Confirm the PR opened:
   ```bash
   gh pr list --repo microsoft/winget-pkgs --author derekwisong
   ```

Being a few commits behind upstream at job start is harmless — winget-pkgs merges
manifest PRs constantly and those never touch workflow files.

If this becomes a recurring nuisance, the durable fix is a dedicated machine account
that owns the fork and holds a `repo` + `workflow` PAT (full `repo` scope is harmless
on an account with no private repos), wired up via the action's `fork-user` input.

## More Information

For detailed information about packaging metadata, policies, and AUR submission, see
[plans/packaging-deb-rpm-aur-plan.md](https://github.com/derekwisong/datui/blob/main/plans/packaging-deb-rpm-aur-plan.md).
