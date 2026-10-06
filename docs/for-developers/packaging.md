# Build and publish packages

`scripts/packaging/build_package.py` builds a Debian/Ubuntu `.deb`, a
Fedora/RHEL `.rpm`, the release tarball, or the tarball and the Arch Linux
AUR package's `PKGBUILD`, from the repository root:

```bash,repo
python3 scripts/packaging/build_package.py deb
python3 scripts/packaging/build_package.py rpm
python3 scripts/packaging/build_package.py tarball
python3 scripts/packaging/build_package.py aur
```

It runs `cargo build --release`, stages the manpages and shell completions in
`target/dist` (below), runs the packaging tool and prints where the package
went. The tarball and the PKGBUILD need no tool; the others:

```bash,repo
cargo install cargo-deb
cargo install cargo-generate-rpm
```

| Option | Effect |
|---|---|
| `--no-build` | Skip `cargo build --release`; `target/release/datui` must exist. The release puts its glibc 2.28 build there (below) |
| `--repo-root PATH` | The repository root, when not the one `git` finds |

## Linux builds and glibc

A binary built on a machine needs that machine's glibc or newer, so one built on
the Ubuntu 24.04 runner failed on Ubuntu 22.04, Debian 12, RHEL 9 and Amazon
Linux 2023. The release builds the Linux binaries with
[cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild): zig links them
against glibc 2.28 (`--target x86_64-unknown-linux-gnu.2.28`), the oldest a
supported distribution ships (Debian 10, Ubuntu 20.04, RHEL 8), whatever the
runner has. liblzma is compiled in (`xz2`'s `static` feature), as zstd, bzip2,
zlib and SQLite already were, so the binary needs nothing beyond glibc. The
wheels' extension is built the same way, as `manylinux_2_28` wheels, for x86_64
and arm64. `scripts/requirements-release.txt` pins zig, cargo-zigbuild and
maturin; the Nightly workflow builds the same way, so its cache serves the
release. To build one yourself:

```bash,repo
pip install -r scripts/requirements-release.txt
cargo zigbuild --release --locked -p datui --target x86_64-unknown-linux-gnu.2.28
install -D target/x86_64-unknown-linux-gnu/release/datui target/release/datui
python3 scripts/packaging/build_package.py deb --no-build
```

`scripts/packaging/check_linux_release.py` is the release gate, which `publish`
waits on. `symbols BINARY...` fails on any `GLIBC_` symbol version above 2.28,
or a `NEEDED` library beyond glibc and libgcc_s, in the tarball's binary, the
wheel's copy of it and the wheel's extension. `smoke DIR` runs the tarball's
binary and installs the `.deb` or `.rpm` on Ubuntu 20.04 and 22.04, Debian 11
and 12, Rocky 8 and 9 and Amazon Linux 2023, in docker, on x86_64 and arm64
runners; `datui --version`, then `datui formats check` over a CSV, which reads
the file and exits.

The `.deb` states `Depends: libc6 (>= 2.28)` in `Cargo.toml` and the `.rpm` takes
its `Requires` from ldd (`libc.so.6(GLIBC_2.28)`), so the package managers refuse
an older system instead of installing a binary that cannot load. The `.deb` does
not use `$auto`: dpkg-shlibdeps maps the pthread and dl symbols, which moved into
libc in 2.34, to `libc6 (>= 2.34)`, and Ubuntu 20.04 and Debian 11 refuse it.

The archives are named by target triple, `datui-vX.Y.Z-TRIPLE.tar.gz` and
`.zip`, with `datui` at the root; `[package.metadata.binstall]` in `Cargo.toml`
tells `cargo binstall` so. `install.sh`, the Homebrew formula, the PKGBUILD,
`publish-packages.yml`'s winget regex and Nightly's startup guard all read these
names; change them together.

`Release` runs by hand (Actions → Release → Run workflow) as a dry run from any
branch: every build and the gate, no fuzz replay, no docs, and nothing
published. Run one before a tag depends on a change to the builds.

## Manpages and completions

The manpages are rendered from the sources the docs are (clap's definitions, the
option, environment and key registries, the format descriptors,
`examples.toml`, the query and format-spec references) by `gen_docs write`, and
committed in `crates/datui-cli/man/`. Committed, they need no build step: a
crates.io build cannot read the docs, and every channel ships
the same files. `the_generated_docs_are_current` fails while one is stale;
`crates/datui-cli/src/man/tests.rs` checks their sections and that every flag,
command, key, setting, variable and exit status appears; CI's
`scripts/docs/lint_manpages.py --require` runs mandoc and groff over them. Their
date is `crates/datui-cli/release-date.txt`, which `bump_version.py` sets.

`cargo run -p datui-cli --bin gen_docs -- dist DIR` stages them for a package:
`DIR/man/manN/` and `DIR/completions/` (`datui.bash`, `_datui`, `datui.fish`,
`_datui.ps1`, `datui.elv`).

| Channel | Manpages | Completions | Staged by |
|---|---|---|---|
| deb | `/usr/share/man/man{1,5,7}`, gzipped | bash, zsh (`vendor-completions`), fish | `build_package.py` (`target/dist`) |
| rpm | `/usr/share/man/man{1,5,7}`, gzipped | bash, zsh (`site-functions`), fish | `build_package.py` |
| AUR | `/usr/share/man/man{1,5,7}` (makepkg gzips them) | bash, zsh, fish | `PKGBUILD.in`, from the Linux x86_64 tarball |
| Linux and macOS archives | `man/manN/` | `completions/` | `build_package.py tarball` and `release.yml`; `install.sh` installs the pages |
| Windows zip | `man/manN/` | `completions/` (`_datui.ps1`) | `release.yml` |
| Homebrew | `man1`, `man5`, `man7` | bash, zsh, fish | the formula, from the macOS and Linux archives |
| PyPI wheel | `<prefix>/share/man/manN/` | none | `scripts/packaging/wheel_manpages.py` (Linux and macOS wheels) |
| `cargo install` | `datui man`, or `datui man --dir ~/.local/share/man` | `datui completions SHELL` | the binary |
| winget | none: Windows has no `man` | none | |

The docs build (`build_single_version_docs.py`) renders the same pages to HTML
with mandoc (groff when mandoc is missing) into `reference/man/`, linked from
[Manual pages](../reference/manual-pages.md).

## License and metadata

All packages include the MIT license as required:

- **deb**: `[package.metadata.deb]` sets `license-file = ["LICENSE", "0"]`; cargo-deb installs it in the package.
- **rpm**: `[[package.metadata.generate-rpm.assets]]` includes `LICENSE` at `/usr/share/licenses/datui/LICENSE`.
- **aur**: `scripts/packaging/PKGBUILD.in` installs the tarball's `LICENSE` at `/usr/share/licenses/datui-bin/LICENSE`.
- **desktop entry**: `scripts/packaging/datui.desktop` installs to
  `/usr/share/applications/datui.desktop` in all three package formats, putting
  datui in desktop launchers. `tests/desktop_entry_test.rs` validates the file and
  checks it is wired into every packager.
- **Python wheel**: `python/pyproject.toml` uses `license = { file = "LICENSE" }` and `sdist-include = ["LICENSE"]`. CI and release workflows copy the root `LICENSE` into `python/LICENSE`.

## Output locations

| Package | Output Directory | Example Filename |
|---------|-----------------|------------------|
| deb | `target/debian/` | `datui_X.Y.Z-1_amd64.deb` |
| rpm | `target/generate-rpm/` | `datui-X.Y.Z-1.x86_64.rpm` |
| tarball | `target/tarball/` | `datui-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz`, named for the host's triple |
| aur | `target/aur/` | `PKGBUILD`, filled in from `scripts/packaging/PKGBUILD.in` with the tarball's sha256 |

## CI and releases

| Workflow | Packages |
|---|---|
| Nightly (`nightly.yml`) | Builds the `.deb`, `.rpm`, tarball, PKGBUILD and wheel from `main`, kept as the run's artifacts |
| Release (`release.yml`) | Attaches the `.deb`, `.rpm`, tarball, PKGBUILD and wheel for Linux x86_64 and arm64, the macOS tarballs and wheels, the Windows zip and wheel, and `SHA256SUMS` with its signature, to the GitHub release, once the Linux gate passes |

Release publishing requires every committed fuzz corpus to pass an AddressSanitizer
replay on the tagged commit, regardless of the latest Nightly result.

### Release notes

`release.yml` composes the release body before creating the release. It uses
`release-notes/v<version>.md` when that file is committed, and otherwise
generates a body from the commit subjects since the previous tag. The body is
therefore never empty, and hand-written notes are always optional.

Write notes before tagging: `publish-packages.yml` copies the release body into
the winget manifest. Editing the GitHub release afterward does not update winget.

To write notes for a release, run `python scripts/bump_version.py notes` and
commit the file with the release. `tests/release_notes_test.rs` checks the
wiring in CI, which runs on the release commit before the tag is pushed, and the
winget job refuses to run komac against an empty release body. See
[the release-notes guide](https://github.com/derekwisong/datui/blob/main/release-notes/README.md).

### AUR by hand

The release does this itself (below). To do it by hand, from the release tag,
replacing `<VERSION>` and `<AUR_REPO>` (a clone of `datui-bin` from the AUR):

```bash,template
git checkout v<VERSION>
cargo build --release --locked
python3 scripts/packaging/build_package.py aur --no-build
cd target/aur
makepkg --printsrcinfo > .SRCINFO
cp PKGBUILD .SRCINFO <AUR_REPO>/
cd <AUR_REPO>
git add PKGBUILD .SRCINFO
git commit -m "Upstream update: <VERSION>"
git push
```

Use stable release tags only (`v0.3.2`): the package fetches the tarball from
the GitHub release.

### Automated AUR updates

The release workflow calls `publish-packages.yml` to push PKGBUILD and .SRCINFO to the AUR after creating the release. It publishes to the **datui-bin** AUR package (per AUR convention for pre-built binaries). It uses [KSXGitHub/github-actions-deploy-aur](https://github.com/KSXGitHub/github-actions-deploy-aur): the action clones the AUR repo, copies our PKGBUILD and tarball, runs `makepkg --printsrcinfo > .SRCINFO`, then commits and pushes via SSH.

**Required repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Description |
|--------|-------------|
| `AUR_SSH_PRIVATE_KEY` | Your SSH **private** key. Add the matching **public** key to your [AUR account](https://aur.archlinux.org/account/) (My Account → SSH Public Key). |
| `AUR_USERNAME` | Your AUR account name (used as git commit author). |
| `AUR_EMAIL` | Email for the AUR git commit (can be a noreply address). |

If these secrets are not set, the "Publish to AUR" step will fail. To disable automated AUR updates, change the `publish-aur` job in `.github/workflows/publish-packages.yml`.

### Apt and dnf repositories

`publish-packages.yml`'s `publish-apt` job builds both repositories from the
release's `.deb` and `.rpm` files and replaces the `gh-pages` branch of
`derekwisong/datui-apt` with them:

| Path | Holds |
|---|---|
| `/` | The apt repository: `Packages`, `Release`, `InRelease`, the `.deb` files |
| `/public.key` | The signing key, for both |
| `/rpm/` | The dnf repository: `repodata/`, its `repomd.xml.asc` signature, the `.rpm` files, signed |
| `/rpm/datui.repo` | The file users put in `/etc/yum.repos.d/` |

apt checks the signed index, which holds each package's checksum. dnf checks the
signed `repomd.xml` and each package's signature, which the job adds to the
repository's copies; the release's `.rpm` files stay unsigned. The job needs
`GPG_PRIVATE_KEY`, `GPG_EMAIL` and `DATUI_APT_TOKEN` (a token that can push to
`datui-apt`).

## PyPI

The release workflow builds Linux x86_64 and arm64 (`manylinux_2_28`),
Windows x86_64 and macOS ARM64/x86_64 wheels with maturin. Each contains the
Python extension and a bundled datui binary.

After the GitHub release is created, `publish-packages.yml` downloads the wheels
and uploads them with twine using `PYPI_API_TOKEN`. The workflow also accepts a
tag when run manually; a blank tag selects the latest release.

Use `scripts/bump_version.py` to keep Rust and Python versions in sync.
For local wheel development, see [Python bindings](python-bindings.md).

## WinGet releases

The `publish-winget` job in `.github/workflows/publish-packages.yml` uses
[winget-releaser](https://github.com/vedantmgoyal9/winget-releaser), which drives
[komac](https://github.com/russellbanks/Komac) to open a manifest PR against
[microsoft/winget-pkgs](https://github.com/microsoft/winget-pkgs) from our fork at
`derekwisong/winget-pkgs`.

**Required repository secret:**

| Secret | Description |
|--------|-------------|
| `WINGET_TOKEN` | Classic PAT with `public_repo` scope. Fine-grained PATs do not work here — they cannot open a cross-fork PR against a repo you don't own. |
| `WINGET_SYNC_TOKEN` | Fine-grained PAT, repository access limited to `derekwisong/winget-pkgs`, with **Contents: Read and write** and **Workflows: Read and write**. It only syncs the fork. Optional, but without it a release can stop on a fork sync (below). |

At least one version of `derekwisong.datui` must already exist in winget-pkgs; the
action refuses to create a brand-new package.

komac copies `ShortDescription` and `Description` from the previous manifest, so
they change only by hand, in a manifest PR. Use the crate's `description` for
`ShortDescription` and the deb's `extended-description` for `Description`; both
are generated with the format count.

### Recovering from "does not have the correct permissions to execute `UpdateRef`"

Before opening the PR, komac fast-forwards our fork from upstream. GitHub blocks any
ref update that touches `.github/workflows/` unless the token carries `workflow`
scope, and upstream winget-pkgs edits its own workflows every few weeks — so the sync
fails once enough time has passed since the last release. The error names a
permissions problem, but **`WINGET_TOKEN` is fine; do not rotate it.**

We can't just add the scope: GitHub's classic-PAT UI force-selects full `repo`
(private repos included) whenever `workflow` is checked.

Without `WINGET_SYNC_TOKEN`, the preflight step attempts the sync with
`WINGET_TOKEN` and, when blocked, fails fast with these steps in the job log:

1. Open <https://github.com/derekwisong/winget-pkgs> and click **Sync fork** →
   **Update branch**. A browser session has permissions the PAT doesn't.
2. Re-run just the failed job, replacing `<RUN_ID>` with the run's id:
   `gh run rerun <RUN_ID> --failed`.
3. Confirm the PR opened:
   `gh pr list --repo microsoft/winget-pkgs --author derekwisong`.

Being a few commits behind upstream at job start is harmless — winget-pkgs merges
manifest PRs constantly and those never touch workflow files.

With `WINGET_SYNC_TOKEN` set, none of this happens: the preflight syncs with that
token, which may update workflow files on the fork and touches nothing else, and
`.github/workflows/winget-fork-sync.yml` also syncs the fork every Monday (or on
demand: `gh workflow run winget-fork-sync.yml`).

To create it: GitHub → Settings → Developer settings → Fine-grained tokens →
Generate new token. Resource owner `derekwisong`, repository access **Only select
repositories** → `winget-pkgs`, permissions Contents and Workflows **Read and
write**. Then `gh secret set WINGET_SYNC_TOKEN --repo derekwisong/datui`.
