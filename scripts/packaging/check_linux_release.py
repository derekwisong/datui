#!/usr/bin/env python3
"""Prove a Linux release runs on glibc 2.28: the symbols it needs, then the distros.

Usage:
    scripts/packaging/check_linux_release.py symbols BINARY... [--max-glibc 2.28]
    scripts/packaging/check_linux_release.py smoke DIR [--image NAME]...

symbols  objdump -T lists every versioned symbol a binary imports. The highest
         GLIBC_x.y among them is the glibc the binary needs, and this fails when it
         is above --max-glibc. It also fails when the binary needs a shared library
         beyond glibc itself (libc, libm, libpthread, libdl, librt, the loader) and
         libgcc_s: everything else, liblzma for one, is compiled in.

smoke    DIR holds a release's Linux assets for the machine's architecture: the
         datui-v*-TRIPLE.tar.gz tarball, the .deb and the .rpm. Each distro image
         runs, in a container, the tarball's binary (`datui --version`, then a
         format spec over a CSV, which reads the file and exits), and installs the
         package its package manager takes and runs that too, so the package's
         dependencies resolve on that release. Needs docker.

Both exit non-zero on the first failure, naming it.
"""

from __future__ import annotations

import argparse
import platform
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

# The libraries a glibc system always has; the loader's name differs per arch.
SYSTEM_LIBRARIES = {
    "libc.so.6",
    "libm.so.6",
    "libpthread.so.0",
    "libdl.so.2",
    "librt.so.1",
    "libgcc_s.so.1",
    "ld-linux-x86-64.so.2",
    "ld-linux-aarch64.so.1",
}

# The oldest glibc (2.28: Debian 10, Ubuntu 20.04, RHEL 8) the release supports;
# docs/getting-started/installation.md says so.
DEFAULT_MAX_GLIBC = "2.28"

# Distros and their package managers. ubuntu:20.04 and debian:11 are at glibc
# 2.31, rockylinux:8 at 2.28 exactly, the rest newer.
DEFAULT_IMAGES = [
    "ubuntu:20.04",
    "ubuntu:22.04",
    "debian:11",
    "debian:12",
    "rockylinux:8",
    "rockylinux:9",
    "amazonlinux:2023",
]

# A delimited format spec and a file it reads: `datui formats check SPEC FILE`
# parses the spec, reads the file with Polars and prints its first rows, with no
# terminal, so it is the one command that opens data and exits.
SMOKE_SPEC = """\
name = "smoke.instrument-log"
kind = "delimited"
match = { magic = "#device_info" }
comment = "#"
skip_initial_space = true
header_rows = { name = 3, unit = 2 }
metadata_line = 1
"""
SMOKE_CSV = """\
#device_info, log_version="1.03", model="Unit 7", serial="123"
#yyyy-mm-dd, hh:mm:ss, hh:mm, degrees, volts, deg F
  Lcl Date, Lcl Time, UTCOfst,     Latitude, bus1volts, T1 Temp
2024-03-01, 10:00:00,  -05:00,    40.100000,      25.0,   180.0
2024-03-01, 10:00:01,  -05:00,    40.100010,      25.1,   180.5
"""


def glibc_versions(binary: Path) -> set[tuple[int, int]]:
    out = subprocess.run(["objdump", "-T", str(binary)], capture_output=True, text=True, check=True).stdout
    return {tuple(int(n) for n in m.groups()) for m in re.finditer(r"\bGLIBC_(\d+)\.(\d+)\b", out)}


def needed_libraries(binary: Path) -> list[str]:
    out = subprocess.run(["objdump", "-p", str(binary)], capture_output=True, text=True, check=True).stdout
    return re.findall(r"^\s*NEEDED\s+(\S+)", out, re.MULTILINE)


def parse_version(text: str) -> tuple[int, int]:
    major, minor = text.split(".")
    return int(major), int(minor)


def check_symbols(binaries: list[Path], max_glibc: str) -> int:
    limit = parse_version(max_glibc)
    failed = False
    for binary in binaries:
        versions = glibc_versions(binary)
        needed = needed_libraries(binary)
        highest = max(versions) if versions else None
        shown = f"GLIBC_{highest[0]}.{highest[1]}" if highest else "no GLIBC versions"
        print(f"{binary}: needs {shown}; NEEDED {' '.join(needed) or 'none'}")
        if highest is not None and highest > limit:
            above = sorted(v for v in versions if v > limit)
            print(f"  FAIL: above GLIBC_{max_glibc}: {', '.join(f'GLIBC_{a}.{b}' for a, b in above)}")
            failed = True
        extra = [lib for lib in needed if lib not in SYSTEM_LIBRARIES]
        if extra:
            print(f"  FAIL: needs shared libraries beyond glibc and libgcc_s: {', '.join(extra)}")
            failed = True
    return 1 if failed else 0


def find_one(directory: Path, pattern: str) -> Path | None:
    found = sorted(directory.glob(pattern))
    if len(found) > 1:
        sys.exit(f"error: more than one {pattern} in {directory}: {', '.join(p.name for p in found)}")
    return found[0] if found else None


def container_script(tarball: str, package: str | None, manager: str | None) -> str:
    """What runs inside each container: sh, since the images have no bash."""
    steps = f"""\
        set -eu
        cd /tmp
        # amazonlinux ships without tar; its users install it to use the tarball too.
        command -v tar > /dev/null || dnf install -y -q tar gzip
        mkdir -p tarball && tar xzf /release/{shlex.quote(tarball)} -C tarball
        echo "--- tarball"
        tarball/datui --version
        tarball/datui formats check /release/smoke.toml /release/smoke.csv
        """
    if manager == "apt":
        steps += f"""\
        echo "--- apt install"
        export DEBIAN_FRONTEND=noninteractive
        cp /release/{shlex.quote(package)} /tmp/
        apt-get update -qq
        apt-get install -y -qq --no-install-recommends /tmp/{shlex.quote(package)} > /dev/null
        dpkg-query -W -f='datui ${{Version}} depends on ${{Depends}}\\n' datui
        """
    elif manager == "dnf":
        steps += f"""\
        echo "--- dnf install"
        dnf install -y -q /release/{shlex.quote(package)}
        echo "datui requires:"; rpm -qR datui | grep -v '^rpmlib' | sed 's/^/  /'
        """
    if manager is not None:
        steps += """\
        /usr/bin/datui --version
        /usr/bin/datui formats check /release/smoke.toml /release/smoke.csv
        man -w datui > /dev/null 2>&1 && echo "manpage: $(man -w datui)" || echo "manpage: no man on this image"
        """
    return textwrap.dedent(steps)


def check_smoke(directory: Path, images: list[str]) -> int:
    machine = platform.machine()
    deb_arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(machine)
    if deb_arch is None:
        sys.exit(f"error: no Linux release for {machine}")
    tarball = find_one(directory, f"datui-v*-{machine}-unknown-linux-gnu.tar.gz")
    deb = find_one(directory, f"datui_*_{deb_arch}.deb")
    rpm = find_one(directory, f"datui-*.{machine}.rpm")
    for what, found in (("tarball", tarball), (".deb", deb), (".rpm", rpm)):
        if found is None:
            sys.exit(f"error: no {what} for {machine} in {directory}")
    print(f"{machine}: {tarball.name}, {deb.name}, {rpm.name}")

    with tempfile.TemporaryDirectory() as tmp:
        release = Path(tmp)
        for asset in (tarball, deb, rpm):
            shutil.copyfile(asset, release / asset.name)
        (release / "smoke.toml").write_text(SMOKE_SPEC)
        (release / "smoke.csv").write_text(SMOKE_CSV)
        for image in images:
            if image.startswith(("ubuntu:", "debian:")):
                package, manager = deb.name, "apt"
            elif image.startswith(("rockylinux:", "almalinux:", "amazonlinux:", "fedora:")):
                package, manager = rpm.name, "dnf"
            else:
                package, manager = None, None
            print(f"\n=== {image}")
            cmd = [
                "docker", "run", "--rm",
                "--volume", f"{release}:/release:ro",
                image, "sh", "-c", container_script(tarball.name, package, manager),
            ]
            if subprocess.run(cmd).returncode != 0:
                print(f"FAIL: {image}")
                return 1
    print("\nAll images ran the release.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    symbols = sub.add_parser("symbols", help="check the glibc symbol versions and shared libraries")
    symbols.add_argument("binaries", nargs="+", type=Path)
    symbols.add_argument("--max-glibc", default=DEFAULT_MAX_GLIBC, help=f"the highest allowed (default {DEFAULT_MAX_GLIBC})")
    smoke = sub.add_parser("smoke", help="run the assets on each distro image, in docker")
    smoke.add_argument("directory", type=Path)
    smoke.add_argument("--image", action="append", help=f"a distro image (default: {', '.join(DEFAULT_IMAGES)})")
    args = parser.parse_args()
    if args.command == "symbols":
        return check_symbols(args.binaries, args.max_glibc)
    return check_smoke(args.directory, args.image or DEFAULT_IMAGES)


if __name__ == "__main__":
    sys.exit(main())
