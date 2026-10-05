#!/usr/bin/env python3
"""Build OS packages (deb, rpm, tarball, aur) for datui.

Single entry point used by CI, release workflows, and local developers.
Run from repo root.

Usage:
    python3 scripts/packaging/build_package.py <deb|rpm|tarball|aur> [--no-build] [--repo-root PATH]
    ./scripts/packaging/build_package.py deb

Options:
    --no-build    Skip 'cargo build --release'; use when target/release/datui exists.
                  The release builds it with cargo zigbuild against glibc 2.28 and
                  copies it there, so every package carries that binary.
    --repo-root   Repository root (default: git rev-parse --show-toplevel).

Outputs:
    deb       target/debian/datui_X.Y.Z-1_ARCH.deb (cargo-deb; depends from dpkg-shlibdeps)
    rpm       target/generate-rpm/datui-X.Y.Z-1.ARCH.rpm (cargo-generate-rpm; requires from ldd)
    tarball   target/tarball/datui-vX.Y.Z-TRIPLE.tar.gz: datui, LICENSE, man/manN/,
              completions/ and scripts/packaging/datui.desktop at the root, named
              for the host's target triple
    aur       the tarball, and target/aur/PKGBUILD for the datui-bin AUR package
"""

from __future__ import annotations

import argparse
import glob
import gzip
import hashlib
import shutil
import tarfile
import os
import subprocess
import sys
import tomllib
from pathlib import Path

PKG_CHOICES = ("deb", "rpm", "tarball", "aur")
REPO_URL = "https://github.com/derekwisong/datui"

# (cargo subcommand, check-cmd, output (dir, glob), human label)
PKG_CONFIG = {
    "deb": ("deb", ["cargo", "deb", "--help"], ("target/debian", "*.deb"), "deb"),
    "rpm": ("generate-rpm", ["cargo", "generate-rpm", "--help"], ("target/generate-rpm", "*.rpm"), "rpm"),
}

# What the tarball holds, from the repository root and target/dist. install.sh,
# the Homebrew formula, the PKGBUILD and cargo-binstall all expect the binary at
# the archive's root.
TARBALL_MEMBERS = ("datui", "LICENSE", "man", "completions", "scripts/packaging/datui.desktop")


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


def package_fields(repo_root: Path) -> tuple[str, str]:
    """The crate's version and description from Cargo.toml."""
    with (repo_root / "Cargo.toml").open("rb") as f:
        package = tomllib.load(f)["package"]
    return package["version"], package["description"]


def host_triple(repo_root: Path) -> str | None:
    """The target triple rustc runs on, which names the tarball."""
    proc = run(["rustc", "-vV"], cwd=repo_root)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        return None
    for line in proc.stdout.splitlines():
        if line.startswith("host: "):
            return line.removeprefix("host: ").strip()
    return None


def stage_dist(repo_root: Path) -> bool:
    """Stage the manpages and completions in target/dist for the packages.

    `gen_docs dist` writes target/dist/man/manN/PAGE.N (as committed in
    crates/datui-cli/man) and target/dist/completions/. The deb and rpm take the
    gzipped copies in target/dist/gz/manN/, compressed with no timestamp so the
    package is reproducible; the tarball takes man/ and completions/ as they are
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


def rpm_version_override(version: str) -> str | None:
    """Return an RPM-safe version when Cargo.toml's version contains '-'.

    RPM disallows '-' in versions (it separates version from release). Tilde is
    RPM's pre-release marker, so '0.2.54~dev' correctly sorts below '0.2.54'
    — unlike '.dev', which would sort above. cargo-generate-rpm 0.21 enforces
    this strictly; earlier versions did not, which is why the nightly only
    started failing once that release rolled out.
    """
    if "-" not in version:
        return None
    return version.replace("-", "~")


def tarball_name(version: str, triple: str) -> str:
    """The release asset's name: datui-vX.Y.Z-TRIPLE.tar.gz, as macOS names its."""
    return f"datui-v{version}-{triple}.tar.gz"


def build_tarball(repo_root: Path, version: str, triple: str) -> Path | None:
    """Pack target/release/datui with the staged manpages and completions.

    Entries are stored as the members say, with 0755 on the binary and 0644 on
    the rest, owned by root, so the archive unpacks the same for everyone.
    """
    binary = repo_root / "target" / "release" / "datui"
    if not binary.exists():
        sys.stderr.write(f"error: {binary} not found; build it first, or drop --no-build\n")
        return None
    dist = repo_root / "target" / "dist"
    out_dir = repo_root / "target" / "tarball"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    out = out_dir / tarball_name(version, triple)

    def owned_by_root(info: tarfile.TarInfo) -> tarfile.TarInfo:
        info.uid = info.gid = 0
        info.uname = info.gname = "root"
        info.mode = 0o755 if info.isdir() or info.name == "datui" else 0o644
        return info

    with tarfile.open(out, "w:gz") as tar:
        for member in TARBALL_MEMBERS:
            source = dist / member if member in ("man", "completions") else repo_root / member
            if member == "datui":
                source = binary
            if not source.exists():
                sys.stderr.write(f"error: {source} is missing from the tarball's members\n")
                return None
            tar.add(source, arcname=member, filter=owned_by_root)
    return out


def write_pkgbuild(repo_root: Path, version: str, description: str, tarball: Path) -> Path | None:
    """Fill in scripts/packaging/PKGBUILD.in for the datui-bin AUR package.

    Arch pkgver cannot contain hyphens, so a dev version's '-dev' becomes '.dev'.
    The source URL is the release asset the tarball becomes, under the tag the
    version names.
    """
    template = repo_root / "scripts" / "packaging" / "PKGBUILD.in"
    content = template.read_text()
    url = f"{REPO_URL}/releases/download/v{version}/{tarball.name}"
    digest = hashlib.sha256(tarball.read_bytes()).hexdigest()
    for placeholder, value in (
        ("PKGVER_PLACEHOLDER", version.replace("-", ".")),
        ("DESC_PLACEHOLDER", description.replace('"', '\\"')),
        ("TARBALL_URL_PLACEHOLDER", url),
        ("SHA256_PLACEHOLDER", digest),
    ):
        if placeholder not in content:
            sys.stderr.write(f"error: {template} has no {placeholder}\n")
            return None
        content = content.replace(placeholder, value)
    out_dir = repo_root / "target" / "aur"
    out_dir.mkdir(parents=True, exist_ok=True)
    pkgbuild = out_dir / "PKGBUILD"
    pkgbuild.write_text(content)
    return pkgbuild


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build OS packages (deb, rpm, tarball, aur) for datui.",
    )
    parser.add_argument(
        "pkg",
        choices=PKG_CHOICES,
        help="Packaging system: deb, rpm, tarball, or aur (the tarball and a PKGBUILD)",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip 'cargo build --release'; caller guarantees target/release/datui exists",
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
    version, description = package_fields(repo_root)

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

    # 3. The tarball and the PKGBUILD need no packaging tool.
    if args.pkg in ("tarball", "aur"):
        triple = host_triple(repo_root)
        if triple is None:
            sys.stderr.write("error: rustc -vV did not name the host triple\n")
            return 1
        tarball = build_tarball(repo_root, version, triple)
        if tarball is None:
            return 1
        artifacts = [tarball]
        if args.pkg == "aur":
            pkgbuild = write_pkgbuild(repo_root, version, description, tarball)
            if pkgbuild is None:
                return 1
            artifacts.append(pkgbuild)
        for p in artifacts:
            print(p)
        return 0

    subcmd, check_cmd, (out_dir, glob_pat), label = PKG_CONFIG[args.pkg]

    # 4. Check packaging tool is installed
    proc = run(check_cmd, cwd=repo_root)
    if proc.returncode != 0:
        sys.stderr.write(
            f"error: cargo {subcmd} not found or failed. "
            f"Install with: cargo install cargo-{subcmd}\n"
        )
        if proc.stderr:
            sys.stderr.write(proc.stderr)
        return 1

    # 5. Run packaging command. Run from repo root; datui is the root package.
    # cargo generate-rpm -p datui looks for datui/Cargo.toml (wrong), so only deb
    # uses -p datui. cargo-deb builds unless told not to, and its build would be a
    # native one that replaces the binary the caller put in target/release.
    if args.pkg == "rpm":
        cmd = ["cargo", subcmd]
        override = rpm_version_override(version)
        if override is not None:
            cmd.extend(["-s", f'version="{override}"'])
    else:
        cmd = ["cargo", subcmd, "-p", "datui"]
        if args.no_build:
            cmd.append("--no-build")
    proc = run(cmd, cwd=repo_root)
    if proc.returncode != 0:
        if proc.stderr:
            sys.stderr.write(proc.stderr)
        sys.stderr.write(f"error: cargo {subcmd} failed\n")
        return 1

    # 6. Verify outputs and print paths
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
