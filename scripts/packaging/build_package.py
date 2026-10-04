#!/usr/bin/env python3
"""Build OS packages (deb, rpm, aur) for datui.

Single entry point used by CI, release workflows, and local developers.
Run from repo root.

Usage:
    python3 scripts/packaging/build_package.py <deb|rpm|aur> [--no-build] [--repo-root PATH]
    ./scripts/packaging/build_package.py deb

Options:
    --no-build    Skip 'cargo build --release'; use when artifacts already exist.
    --repo-root   Repository root (default: git rev-parse --show-toplevel).
"""

from __future__ import annotations

import argparse
import glob
import gzip
import hashlib
import re
import shutil
import tarfile
import tempfile
import os
import subprocess
import sys
import tomllib
from pathlib import Path

PKG_CHOICES = ("deb", "rpm", "aur")

# (cargo subcommand, check-cmd, output (dir, glob) or "aur", human label)
PKG_CONFIG = {
    "deb": ("deb", ["cargo", "deb", "--help"], ("target/debian", "*.deb"), "deb"),
    "rpm": ("generate-rpm", ["cargo", "generate-rpm", "--help"], ("target/generate-rpm", "*.rpm"), "rpm"),
    "aur": ("aur", ["cargo", "aur", "--help"], "aur", "AUR"),
}


def find_repo_root(cwd: Path | None = None) -> Path:
    """Return git repository root, or cwd if not in a repo."""
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
            cwd=cwd or Path.cwd(),
        )
        return Path(r.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        return Path.cwd()


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def stage_dist(repo_root: Path) -> bool:
    """Stage the manpages and completions in target/dist for the packages.

    `gen_docs dist` writes target/dist/man/manN/PAGE.N (as committed in
    crates/datui-cli/man) and target/dist/completions/. The deb and rpm take the
    gzipped copies in target/dist/gz/manN/, compressed with no timestamp so the
    package is reproducible; the AUR tarball takes man/ and completions/ as they are
    (makepkg compresses manpages itself). Returns True on success.
    """
    dist = repo_root / "target" / "dist"
    if dist.exists():
        shutil.rmtree(dist)
    proc = run(
        ["cargo", "run", "--locked", "-q", "-p", "datui-cli", "--bin", "gen_docs", "--", "dist", str(dist)],
        cwd=repo_root,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        return False
    for page in sorted((dist / "man").glob("man*/*")):
        gz = dist / "gz" / page.parent.name / (page.name + ".gz")
        gz.parent.mkdir(parents=True, exist_ok=True)
        with page.open("rb") as src, gzip.GzipFile(gz, "wb", compresslevel=9, mtime=0) as out:
            shutil.copyfileobj(src, out)
    return any((dist / "gz").glob("man1/*.gz"))


# Where the AUR package installs what stage_dist wrote, from the tarball's root.
AUR_INSTALLS = [
    'install -Dm644 man/man1/*.1 -t "$pkgdir/usr/share/man/man1"',
    'install -Dm644 man/man5/*.5 -t "$pkgdir/usr/share/man/man5"',
    'install -Dm644 man/man7/*.7 -t "$pkgdir/usr/share/man/man7"',
    'install -Dm644 completions/datui.bash "$pkgdir/usr/share/bash-completion/completions/datui"',
    'install -Dm644 completions/_datui "$pkgdir/usr/share/zsh/site-functions/_datui"',
    'install -Dm644 completions/datui.fish "$pkgdir/usr/share/fish/vendor_completions.d/datui.fish"',
]


def add_pkgbuild_installs(pkgbuild: Path) -> bool:
    """Add the manpage and completion installs to the PKGBUILD's package()."""
    lines = pkgbuild.read_text().splitlines()
    try:
        start = next(i for i, l in enumerate(lines) if l.strip().startswith("package()"))
        end = next(i for i in range(start + 1, len(lines)) if lines[i].strip() == "}")
    except StopIteration:
        return False
    if any("share/man/man1" in l for l in lines[start:end]):
        return True
    indent = "    "
    lines[end:end] = [indent + l for l in AUR_INSTALLS]
    pkgbuild.write_text("\n".join(lines) + "\n")
    print("Added manpage and completion installs to PKGBUILD")
    return True


def rpm_version_override(repo_root: Path) -> str | None:
    """Return an RPM-safe version when Cargo.toml's version contains '-'.

    RPM disallows '-' in versions (it separates version from release). Tilde is
    RPM's pre-release marker, so '0.2.54~dev' correctly sorts below '0.2.54'
    — unlike '.dev', which would sort above. cargo-generate-rpm 0.21 enforces
    this strictly; earlier versions did not, which is why the nightly only
    started failing once that release rolled out.
    """
    with (repo_root / "Cargo.toml").open("rb") as f:
        data = tomllib.load(f)
    version = data.get("package", {}).get("version", "")
    if "-" not in version:
        return None
    return version.replace("-", "~")


def fix_aur_pkgbuild(repo_root: Path) -> bool:
    """Fix PKGBUILD for Arch compatibility: replace '-dev' with '.dev' in version.

    Arch pkgver cannot contain hyphens. For dev versions:
    - Rename tarball to use .dev
    - Fix source URL to use 'dev' tag (dev releases use tag 'dev', not 'v0.2.11.dev')
    Returns True on success.
    """
    aur_dir = repo_root / "target" / "cargo-aur"
    pkgbuild = aur_dir / "PKGBUILD"

    if not pkgbuild.exists():
        return False

    content = pkgbuild.read_text()

    # Check if there's a -dev version that needs fixing
    if "-dev" not in content:
        return True  # Nothing to fix

    # Replace -dev with .dev in the PKGBUILD content
    new_content = content.replace("-dev", ".dev")

    # For dev versions, the source URL must use tag 'dev', not 'v$pkgver'
    # (GitHub dev release is at /releases/tag/dev)
    new_content = new_content.replace(
        'releases/download/v$pkgver/',
        'releases/download/dev/',
    )

    pkgbuild.write_text(new_content)

    # Rename the tarball to match (if it exists with -dev in the name)
    for tarball in aur_dir.glob("*-dev*.tar.gz"):
        new_name = tarball.name.replace("-dev", ".dev")
        new_path = tarball.parent / new_name
        tarball.rename(new_path)
        print(f"Renamed: {tarball.name} -> {new_name}")

    print("Fixed PKGBUILD: replaced '-dev' with '.dev', source URL uses 'dev' tag")
    return True


def add_pkgbuild_options(repo_root: Path) -> bool:
    """Stop makepkg from stripping the released binary on the builder's machine.

    The tarball ships a symbol table (see strip = "debuginfo" in Cargo.toml). Arch's
    default OPTIONS has both strip and debug, so without this makepkg strips datui and
    splits the symbols into a datui-bin-debug package -- which pacman marks --asdeps,
    orphans immediately, and removes on the next -Rns cleanup. Either path loses the
    backtraces, so opt out of both. Returns True on success.
    """
    pkgbuild = repo_root / "target" / "cargo-aur" / "PKGBUILD"

    if not pkgbuild.exists():
        return False

    content = pkgbuild.read_text()

    if "options=" in content:
        return True  # cargo-aur emits its own now; don't fight it

    if "\nsource=" not in content:
        return False

    content = content.replace("\nsource=", "\noptions=(!strip !debug)\nsource=", 1)
    pkgbuild.write_text(content)
    print("Added options=(!strip !debug) to PKGBUILD")
    return True


def save_release_binary(repo_root: Path) -> Path | None:
    """Snapshot target/release/datui before cargo-aur strips it in place.

    cargo-aur runs `strip` directly on the release binary (src/main.rs, `strip(&binary)`)
    before copying it into the tarball. That mutates the shared build output, so every
    artifact produced after the AUR step -- notably the CLI bundled into the Python
    wheel -- silently loses its symbol table. Keep a copy so we can put it back.
    """
    binary = repo_root / "target" / "release" / "datui"

    if not binary.exists():
        return None

    backup = binary.with_name("datui.unstripped")
    shutil.copy2(binary, backup)
    return backup


def repack_aur_tarball(repo_root: Path, backup: Path | None) -> bool:
    """Finish the AUR tarball: undo cargo-aur's strip, and add man/ and completions/.

    With `backup`, puts the unstripped binary back at target/release/datui (for later
    steps, such as the wheel) and in the tarball. Adds target/dist's man/ and
    completions/ at the tarball's root, as the macOS and arm64 tarballs have them, then
    refreshes the PKGBUILD's sha256sum to match. Returns True on success.
    """
    aur_dir = repo_root / "target" / "cargo-aur"
    binary = repo_root / "target" / "release" / "datui"
    dist = repo_root / "target" / "dist"

    if backup is not None and backup.exists():
        # 1. Restore the shared build output for downstream steps (wheel bundling).
        shutil.copy2(backup, binary)

    tarballs = list(aur_dir.glob("*.tar.gz"))
    if len(tarballs) != 1:
        sys.stderr.write(f"warning: expected exactly 1 AUR tarball, found {len(tarballs)}\n")
        if backup is not None:
            backup.unlink(missing_ok=True)
        return False
    tarball = tarballs[0]

    # 2. Repack: the unstripped binary, then man/ and completions/, entry order and
    # modes kept.
    with tempfile.TemporaryDirectory() as tmp:
        staging = Path(tmp) / "staging"
        with tarfile.open(tarball, "r:gz") as tar:
            members = tar.getnames()
            tar.extractall(staging)

        staged_binary = staging / "datui"
        if backup is not None and backup.exists():
            if not staged_binary.exists():
                sys.stderr.write("warning: no 'datui' entry in AUR tarball; leaving it stripped\n")
            else:
                mode = staged_binary.stat().st_mode
                shutil.copy2(backup, staged_binary)
                staged_binary.chmod(mode)

        for tree in ("man", "completions"):
            if (dist / tree).is_dir() and tree not in members:
                shutil.copytree(dist / tree, staging / tree)
                members.append(tree)

        with tarfile.open(tarball, "w:gz") as tar:
            for name in members:
                tar.add(staging / name, arcname=name, recursive=name in ("man", "completions"))

    if backup is not None:
        backup.unlink(missing_ok=True)

    # 3. Install them, and refresh the checksum to match the repacked tarball.
    pkgbuild = aur_dir / "PKGBUILD"
    if pkgbuild.exists():
        if not add_pkgbuild_installs(pkgbuild):
            sys.stderr.write("warning: no package() in PKGBUILD to add the manpages to\n")
            return False
        digest = hashlib.sha256(tarball.read_bytes()).hexdigest()
        content = pkgbuild.read_text()
        updated = re.sub(
            r'sha256sums=\("[0-9a-f]{64}"\)',
            f'sha256sums=("{digest}")',
            content,
            count=1,
        )
        if updated == content:
            sys.stderr.write("warning: could not update sha256sums in PKGBUILD\n")
            return False
        pkgbuild.write_text(updated)
        print(f"Repacked AUR tarball with man/ and completions/; sha256 now {digest[:16]}...")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build OS packages (deb, rpm, aur) for datui.",
    )
    parser.add_argument(
        "pkg",
        choices=PKG_CHOICES,
        help="Packaging system: deb, rpm, or aur",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip 'cargo build --release'; caller guarantees release artifacts exist",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Repository root (default: git rev-parse --show-toplevel)",
    )
    args = parser.parse_args()

    repo_root = (args.repo_root or find_repo_root()).resolve()
    if not (repo_root / "Cargo.toml").exists():
        sys.stderr.write(f"error: Cargo.toml not found at {repo_root}\n")
        return 1

    subcmd, check_cmd, out_spec, label = PKG_CONFIG[args.pkg]
    out_dir_or_aur = out_spec

    # 1. Build release unless --no-build.
    if not args.no_build:
        proc = run(
            ["cargo", "build", "--release", "--locked", "--workspace", "-p", "datui"],
            cwd=repo_root,
        )
        if proc.returncode != 0:
            if proc.stderr:
                sys.stderr.write(proc.stderr)
            sys.stderr.write("error: cargo build --release failed\n")
            return 1

    # 2. Stage the manpages and completions
    if not stage_dist(repo_root):
        sys.stderr.write("error: could not stage the manpages and completions in target/dist\n")
        return 1

    # 3. Check packaging tool is installed
    proc = run(check_cmd, cwd=repo_root)
    if proc.returncode != 0:
        sys.stderr.write(
            f"error: cargo {subcmd} not found or failed. "
            f"Install with: cargo install cargo-{subcmd}\n"
        )
        if proc.stderr:
            sys.stderr.write(proc.stderr)
        return 1

    # 4. Run packaging command (package datui for deb/rpm/aur)
    # Run from repo root; datui is the root package. cargo generate-rpm -p datui looks for
    # datui/Cargo.toml (wrong). cargo-aur does not support -p. So only deb uses -p datui.
    saved_binary = None
    if args.pkg == "aur":
        cmd = ["cargo", "aur"]
        # cargo-aur strips target/release/datui in place; snapshot it first.
        saved_binary = save_release_binary(repo_root)
    elif args.pkg == "rpm":
        cmd = ["cargo", subcmd]
        override = rpm_version_override(repo_root)
        if override is not None:
            cmd.extend(["-s", f'version="{override}"'])
    else:
        cmd = ["cargo", subcmd, "-p", "datui"]
    proc = run(cmd, cwd=repo_root)
    if proc.returncode != 0:
        if proc.stderr:
            sys.stderr.write(proc.stderr)
        sys.stderr.write(f"error: cargo {subcmd} failed\n")
        return 1

    # 5. Post-process AUR package for Arch compatibility
    if args.pkg == "aur":
        if not fix_aur_pkgbuild(repo_root):
            sys.stderr.write("warning: failed to fix PKGBUILD for Arch compatibility\n")
        if not add_pkgbuild_options(repo_root):
            sys.stderr.write("warning: failed to add options=(!strip !debug) to PKGBUILD\n")
        if not repack_aur_tarball(repo_root, saved_binary):
            sys.stderr.write("error: could not finish the AUR tarball\n")
            return 1

    # 6. Verify outputs and print paths
    if out_dir_or_aur == "aur":
        out_dir = repo_root / "target" / "cargo-aur"
        if not out_dir.is_dir():
            sys.stderr.write(f"error: expected output directory {out_dir} not found\n")
            return 1
        pkgs = list(out_dir.glob("PKGBUILD"))
        tar = list(out_dir.glob("*.tar.gz"))
        artifacts = pkgs + tar
        if not artifacts:
            sys.stderr.write(f"error: no PKGBUILD or tarball found in {out_dir}\n")
            return 1
    else:
        out_dir, glob_pat = out_dir_or_aur
        pattern = str(repo_root / out_dir / glob_pat)
        artifacts = [Path(p) for p in glob.glob(pattern)]
        if not artifacts:
            sys.stderr.write(f"error: no {label} artifact found matching {pattern}\n")
            return 1

    for p in sorted(artifacts):
        print(p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
