#!/usr/bin/env python3
"""Set up a datui checkout for development, on Linux, macOS or Windows.

Creates .venv (with uv when it is installed, python -m venv otherwise), installs
scripts/requirements.txt into it, installs the pre-commit hooks, and generates the
test fixtures in tests/sample-data. Safe to rerun: it reuses .venv and regenerates
the fixtures only when the generator or its pins changed.

  python3 scripts/setup_dev.py            # the above
  python3 scripts/setup_dev.py --wheel    # also maturin and pytest, for the Python package
  python3 scripts/setup_dev.py --docs     # also mdBook, and build the docs into book/
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
VENV = REPO_ROOT / ".venv"
SCRIPTS = REPO_ROOT / "scripts"
WINDOWS = sys.platform == "win32"


def venv_bin(name):
    if WINDOWS:
        return VENV / "Scripts" / f"{name}.exe"
    return VENV / "bin" / name


def run(cmd, **kwargs):
    """Run with output streaming to the terminal; exit with its status on failure."""
    print("$", " ".join(str(c) for c in cmd), flush=True)
    result = subprocess.run([str(c) for c in cmd], cwd=REPO_ROOT, **kwargs)
    if result.returncode != 0:
        sys.exit(f"setup: {Path(str(cmd[0])).name} exited with {result.returncode}")


def step(text):
    print(f"\n==> {text}", flush=True)


def make_venv(uv):
    python = venv_bin("python")
    if python.exists():
        step(f"Reusing {VENV.name}")
    elif uv:
        step(f"Creating {VENV.name} with uv")
        run([uv, "venv", VENV])
    else:
        step(f"Creating {VENV.name} with {Path(sys.executable).name} -m venv")
        run([sys.executable, "-m", "venv", VENV])
        run([python, "-m", "pip", "install", "--upgrade", "pip"])


def install(uv, requirements):
    for path in requirements:
        step(f"Installing {path.relative_to(REPO_ROOT)}")
        if uv:
            # uv installs into the environment VIRTUAL_ENV names.
            run([uv, "pip", "install", "-r", path], env={**os.environ, "VIRTUAL_ENV": str(VENV)})
        else:
            run([venv_bin("python"), "-m", "pip", "install", "-r", path])


def install_hooks():
    if not (REPO_ROOT / ".git").exists():
        print("Not a git checkout; skipping the pre-commit hooks.")
        return
    step("Installing the pre-commit hooks")
    run([venv_bin("pre-commit"), "install"])


def generate_fixtures(force):
    step("Generating test fixtures")
    cmd = [venv_bin("python"), SCRIPTS / "generate_sample_data.py"]
    if not force:
        cmd.append("--if-stale")
    run(cmd)


def pinned_version(tool):
    """A tool's version from .github/tool-versions, which CI installs from too."""
    for line in (REPO_ROOT / ".github" / "tool-versions").read_text().splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] == tool:
            return fields[1]
    sys.exit(f"setup: {tool} has no version in .github/tool-versions")


def mdbook_version():
    mdbook = shutil.which("mdbook")
    if not mdbook:
        return None
    out = subprocess.run([mdbook, "--version"], capture_output=True, text=True).stdout
    # "mdbook v0.5.2"
    return out.strip().rsplit("v", 1)[-1] or None


def build_docs():
    wanted = pinned_version("mdbook")
    if mdbook_version() != wanted:
        step(f"Installing mdBook {wanted} (a few minutes)")
        run(["cargo", "install", "mdbook", "--version", wanted, "--locked"])
    step("Building the docs into book/")
    python = venv_bin("python")
    run([python, SCRIPTS / "docs" / "build_single_version_docs.py"], stdin=subprocess.DEVNULL)
    run([python, SCRIPTS / "docs" / "rebuild_index.py"], stdin=subprocess.DEVNULL)


def main():
    lines = __doc__.strip().splitlines()
    parser = argparse.ArgumentParser(
        description=lines[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(lines[1:]),
    )
    parser.add_argument(
        "--wheel", action="store_true", help="also install the Python package's build and test tools"
    )
    parser.add_argument("--docs", action="store_true", help="also install mdBook and build the docs")
    parser.add_argument("--force", action="store_true", help="regenerate the fixtures even when current")
    parser.add_argument("--no-hooks", action="store_true", help="do not install the pre-commit hooks")
    args = parser.parse_args()

    if sys.version_info < (3, 10):
        sys.exit(f"setup: datui's scripts need Python 3.10 or newer, not {sys.version.split()[0]}")
    uv = shutil.which("uv")
    if not uv:
        print("uv not found; using pip, which is slower (https://docs.astral.sh/uv/).")

    make_venv(uv)
    requirements = [SCRIPTS / "requirements.txt"]
    if args.wheel:
        # patchelf, which the Linux and macOS wheels need, has no Windows build.
        name = "requirements-wheel-windows.txt" if WINDOWS else "requirements-wheel.txt"
        requirements.append(SCRIPTS / name)
    install(uv, requirements)
    if not args.no_hooks:
        install_hooks()
    generate_fixtures(args.force)
    if args.docs:
        build_docs()

    print("\nDone. Next:")
    print("  cargo build")
    print("  scripts/dev/test.sh full     # or: cargo test --workspace")
    if args.wheel:
        print("  scripts/dev/test.sh python   # build and test the Python package")


if __name__ == "__main__":
    main()
