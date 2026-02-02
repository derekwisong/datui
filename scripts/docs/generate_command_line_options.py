#!/usr/bin/env python3
"""Generate command-line-options markdown from Clap definitions.

Runs the gen_docs binary (emits markdown to stdout). With -o/--output, writes
to that path; otherwise prints to the terminal.

Usage:
    python3 scripts/docs/generate_command_line_options.py [REPO_ROOT] [-o PATH]
    ./scripts/docs/generate_command_line_options.py [REPO_ROOT] [-o PATH]

If REPO_ROOT is omitted, uses the git repository root (from cwd).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate command-line-options markdown from Clap definitions."
    )
    parser.add_argument(
        "repo_root",
        nargs="?",
        default=None,
        help="Repository root (default: git rev-parse --show-toplevel)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Write output to this path; if omitted, print to terminal",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve() if args.repo_root else find_repo_root()
    repo_root = repo_root.resolve()

    # Try current layout first (gen_docs in datui-cli); fall back to old layout (gen_docs in root).
    # This lets build_all_docs_local.py work for both main and historical tags.
    for cmd in (
        ["cargo", "run", "-p", "datui-cli", "--bin", "gen_docs", "--quiet"],
        ["cargo", "run", "--bin", "gen_docs", "--quiet"],
    ):
        proc = subprocess.run(
            cmd,
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            break
    else:
        if proc.stderr:
            sys.stderr.write(proc.stderr)
        return proc.returncode

    if args.output:
        out_path = Path(args.output).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(proc.stdout, encoding="utf-8")
    else:
        print(proc.stdout, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
