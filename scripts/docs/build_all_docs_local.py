#!/usr/bin/env python3
"""Build all documentation locally (tagged releases only, matches production).

Skips building a tag if book/<tag>/.built_sha already matches the tag's current SHA.
Re-run to only rebuild changed or new tags. Uses a git worktree per tag so the
repo stays on your current branch.

Prerequisites: mdbook (cargo install mdbook), python3 + scripts/requirements.txt.

Usage:
    python3 scripts/docs/build_all_docs_local.py

To build a single version: python3 scripts/docs/build_single_version_docs.py v0.2.22
To force full rebuild:    rm -rf book && python3 scripts/docs/build_all_docs_local.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=check)


def get_repo_root() -> Path:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )
        return Path(r.stdout.strip()).resolve()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return Path.cwd().resolve()


def _version_key(tag: str) -> tuple[int, ...]:
    s = tag.lstrip("v").split("-")[0]  # drop -rc.1 etc.
    parts = s.split(".")[:3]
    nums = []
    for p in parts:
        try:
            nums.append(int(p))
        except ValueError:
            nums.append(0)
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums)


def get_version_tags(repo_root: Path) -> list[str]:
    proc = run(["git", "tag", "-l", "v*"], cwd=repo_root)
    tags = [t.strip() for t in proc.stdout.strip().splitlines() if t.strip()]
    return sorted(tags, key=_version_key)


def get_tag_sha(repo_root: Path, tag: str) -> str:
    proc = run(["git", "rev-parse", tag], cwd=repo_root)
    return proc.stdout.strip()


def main() -> int:
    repo_root = get_repo_root()
    script_dir = Path(__file__).parent.resolve()
    build_single = script_dir / "build_single_version_docs.py"
    rebuild_index = script_dir / "rebuild_index.py"
    book_dir = repo_root / "book"
    book_dir.mkdir(parents=True, exist_ok=True)

    version_tags = get_version_tags(repo_root)
    if not version_tags:
        print("No v* tags found. Create a tag (e.g. v0.2.22) to build docs.")
        return 1

    print("Building documentation locally (tagged versions only)...")
    print(f"Found {len(version_tags)} version tag(s)")
    print()

    worktree_dir = Path(tempfile.mkdtemp())
    cleanup_worktree = True
    try:
        for tag in version_tags:
            tag_sha = get_tag_sha(repo_root, tag)
            built_sha_file = book_dir / tag / ".built_sha"
            if built_sha_file.exists() and built_sha_file.read_text().strip() == tag_sha:
                print(f"  Skipping {tag} (already built for this SHA)")
                continue

            print(f"  Building {tag}...")
            # Remove worktree if it exists from a previous iteration
            worktree_git = worktree_dir / ".git"
            if worktree_dir.exists() and worktree_git.exists():
                run(["git", "worktree", "remove", "-f", str(worktree_dir)], cwd=repo_root, check=False)
                if worktree_dir.exists():
                    shutil.rmtree(worktree_dir, ignore_errors=True)
                    worktree_dir.mkdir()

            proc = run(["git", "worktree", "add", str(worktree_dir), tag], cwd=repo_root, check=False)
            if proc.returncode != 0:
                print(f"    Warning: Could not create worktree for {tag}")
                continue

            env = os.environ.copy()
            env["DATUI_REPO_ROOT"] = str(repo_root)
            proc = subprocess.run(
                [sys.executable, str(build_single), tag, "--worktree"],
                cwd=worktree_dir,
                env=env,
            )
            (book_dir / tag).mkdir(parents=True, exist_ok=True)
            (book_dir / tag / ".built_sha").write_text(tag_sha)

            run(["git", "worktree", "remove", "-f", str(worktree_dir)], cwd=repo_root, check=False)
            if worktree_dir.exists():
                shutil.rmtree(worktree_dir, ignore_errors=True)
                worktree_dir.mkdir()

        print()
        print("Rebuilding index page...")
        run([sys.executable, str(rebuild_index)], cwd=repo_root)

        # Copy newest version to latest (stable URL for "current release")
        latest_tag = version_tags[-1]
        latest_dir = book_dir / "latest"
        if latest_dir.exists():
            shutil.rmtree(latest_dir)
        shutil.copytree(book_dir / latest_tag, latest_dir)
        print(f"  Updated latest -> {latest_tag}")

        demos_global = book_dir / "demos"
        if demos_global.exists():
            shutil.rmtree(demos_global)

        for f in book_dir.rglob(".built_sha"):
            f.unlink()

        cleanup_worktree = False
    finally:
        if cleanup_worktree and worktree_dir.exists():
            print()
            print("Cleaning up worktree...")
            run(["git", "worktree", "remove", "-f", str(worktree_dir)], cwd=repo_root, check=False)
            shutil.rmtree(worktree_dir, ignore_errors=True)

    print()
    print(f"Documentation build complete: {book_dir}")
    print()
    print("View locally:")
    print("  open book/index.html")
    print("  or: python3 -m http.server 8000 --directory book   # then http://localhost:8000")
    print()

    if sys.stdin.isatty():
        try:
            reply = input("Start a local HTTP server? (y/n) ").strip().lower()
            if reply == "y":
                print("Serving at http://localhost:8000 (Ctrl+C to stop)")
                subprocess.run([sys.executable, "-m", "http.server", "8000", "--directory", str(book_dir)], cwd=repo_root)
        except (EOFError, KeyboardInterrupt):
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
