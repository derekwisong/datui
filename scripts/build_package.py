#!/usr/bin/env python3
"""Build OS packages (deb, rpm, aur) for datui.

Single entry point used by CI, release workflows, and local developers.
Run from repo root.

Usage:
    python3 scripts/build_package.py <deb|rpm|aur> [--no-build] [--repo-root PATH]
    ./scripts/build_package.py deb

Options:
    --no-build    Skip 'cargo build --release'; use when artifacts already exist.
    --repo-root   Repository root (default: git rev-parse --show-toplevel).
"""

from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
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


def ensure_gzipped_manpage(repo_root: Path) -> bool:
    """Ensure target/release/datui.1.gz exists. Create from .1 if needed. Return True on success."""
    man = repo_root / "target" / "release" / "datui.1"
    gz = repo_root / "target" / "release" / "datui.1.gz"
    if gz.exists():
        return True
    if not man.exists():
        return False
    proc = run(["gzip", "-9", "-k", "-f", str(man)], cwd=repo_root)
    if proc.returncode != 0:
        if proc.stderr:
            sys.stderr.write(proc.stderr)
        return False
    return (repo_root / "target" / "release" / "datui.1.gz").exists()


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

    # 1. Build release (and manpage) unless --no-build. Build datui so binary and manpage exist.
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
        man = repo_root / "target" / "release" / "datui.1"
        if not man.exists():
            sys.stderr.write("error: manpage target/release/datui.1 not found after build\n")
            return 1

    # 2. Ensure gzipped manpage exists
    if not ensure_gzipped_manpage(repo_root):
        sys.stderr.write(
            "error: target/release/datui.1.gz missing; "
            "ensure target/release/datui.1 exists and gzip is available\n"
        )
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
    if args.pkg == "aur":
        cmd = ["cargo", "aur"]
    elif args.pkg == "rpm":
        cmd = ["cargo", subcmd]
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
