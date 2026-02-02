#!/usr/bin/env python3
"""Build documentation for a single version (tag or branch).

Usage:
    python3 scripts/docs/build_single_version_docs.py [VERSION] [--worktree]
    VERSION: version tag (e.g. v1.0.0) or branch name (default: main)
    --worktree: caller is in a worktree; skip checkout/restore (used by build_all_docs_local.py and CI)

Developer usage:
    Build one tag:   python3 scripts/docs/build_single_version_docs.py v0.2.22
    Build main:      python3 scripts/docs/build_single_version_docs.py main
    Build all tags: python3 scripts/docs/build_all_docs_local.py

Prerequisites: mdbook (cargo install mdbook). For main/branch builds, python3 + scripts/requirements.txt.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


OUTPUT_DIR = "book"


def get_repo_root(cwd: Path | None = None) -> Path:
    env_root = os.environ.get("DATUI_REPO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
            cwd=cwd or Path.cwd(),
        )
        return Path(r.stdout.strip()).resolve()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return Path.cwd().resolve()


def find_mdbook() -> str:
    """Return path to mdbook or raise SystemExit."""
    mdbook = shutil.which("mdbook")
    if mdbook:
        return mdbook
    cargo_bin = Path.home() / ".cargo" / "bin" / "mdbook"
    if cargo_bin.exists():
        return str(cargo_bin)
    print("Error: mdbook not found (on PATH or in ~/.cargo/bin). Install with: cargo install mdbook", file=sys.stderr)
    sys.exit(1)


def run(cmd: list[str], cwd: Path | None = None, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, env=env or os.environ.copy(), capture_output=True, text=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build documentation for a single version (tag or branch).",
    )
    parser.add_argument(
        "version",
        nargs="?",
        default="main",
        help="Version tag (e.g. v0.2.22) or branch name (default: main)",
    )
    parser.add_argument(
        "--worktree",
        action="store_true",
        help="Caller is in a worktree; skip checkout/restore",
    )
    args = parser.parse_args()

    version_name = args.version
    use_worktree = args.worktree
    is_tag = len(version_name) > 1 and version_name.startswith("v") and version_name[1].isdigit()

    cwd = Path.cwd().resolve()
    repo_root = get_repo_root(cwd)
    if use_worktree:
        build_dir = cwd
    else:
        build_dir = repo_root

    book_dir = repo_root / OUTPUT_DIR
    book_dir.mkdir(parents=True, exist_ok=True)
    output_path = book_dir / version_name
    output_path.mkdir(parents=True, exist_ok=True)

    if not (build_dir / "docs").is_dir():
        print(f"Error: docs/ directory not found in {build_dir}", file=sys.stderr)
        return 1
    if not (build_dir / "book.toml").exists():
        print(f"Error: book.toml not found in {build_dir}", file=sys.stderr)
        return 1

    original_commit: str | None = None
    original_branch: str | None = None

    if is_tag and not use_worktree:
        try:
            original_commit = run(["git", "rev-parse", "HEAD"], cwd=repo_root).stdout.strip()
            original_branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_root).stdout.strip() or "HEAD"
        except Exception:
            pass
        print(f"Building docs for tag: {version_name}")
        ret = subprocess.run(["git", "checkout", version_name], cwd=repo_root)
        if ret.returncode != 0:
            print(f"Error: Could not checkout tag {version_name}", file=sys.stderr)
            return 1
    elif is_tag and use_worktree:
        print("Running from worktree (already checked out)")
    else:
        print(f"Building docs for branch: {version_name}")

    # Clean global demos dir to avoid conflicts
    demos_global = repo_root / OUTPUT_DIR / "demos"
    if demos_global.exists():
        shutil.rmtree(demos_global)

    with tempfile.TemporaryDirectory() as docs_temp:
        docs_temp_path = Path(docs_temp)
        shutil.copytree(build_dir / "docs", docs_temp_path / "docs")
        shutil.copy2(build_dir / "book.toml", docs_temp_path / "book.toml")

        # Generate command-line-options.md for non-tag builds
        gen_script = repo_root / "scripts" / "docs" / "generate_command_line_options.py"
        if not is_tag and gen_script.exists():
            print("Generating command line argument docs (can take some time to build)")
            out_md = docs_temp_path / "docs" / "reference" / "command-line-options.md"
            out_md.parent.mkdir(parents=True, exist_ok=True)
            proc = run(
                [sys.executable, str(gen_script), "-o", str(out_md)],
                cwd=build_dir,
            )
            if proc.returncode == 0:
                print("✓ Generated command-line-options.md (temp)")
            else:
                print("  Warning: generate_command_line_options.py failed; using existing docs if present")
        elif is_tag:
            print("Skipping command line docs for tag (using committed docs)")

        mdbook = find_mdbook()
        proc = subprocess.run(
            [mdbook, "build", str(docs_temp_path), "--dest-dir", str(output_path)],
            cwd=repo_root,
        )
        if proc.returncode != 0:
            print(f"Error: mdbook build failed for {version_name}", file=sys.stderr)
            return 1
        print(f"✓ Built docs for {version_name}")

    # Copy demos into this version's output
    demos_src = build_dir / "demos"
    if demos_src.is_dir():
        demos_dest = output_path / "demos"
        if demos_dest.exists():
            shutil.rmtree(demos_dest)
        shutil.copytree(demos_src, demos_dest)
        print(f"✓ Copied demos directory to {version_name}/demos")
    else:
        print(f"  Warning: demos directory not found for {version_name} - skipping")

    if demos_global.exists():
        shutil.rmtree(demos_global)

    # Restore original checkout if we switched for a tag
    if is_tag and not use_worktree and original_commit:
        print(f"Returning to original commit: {original_commit}")
        for ref in [original_commit, original_branch, "main"]:
            ret = subprocess.run(["git", "checkout", ref], cwd=repo_root, capture_output=True)
            if ret.returncode == 0:
                break
        else:
            print("Warning: Could not return to original commit, continuing...")
        script_path = repo_root / "scripts" / "docs" / "build_single_version_docs.py"
        if not script_path.exists():
            print("Error: Scripts missing after returning from tag checkout", file=sys.stderr)
            return 1

    print(f"✓ Single version build complete: {version_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
