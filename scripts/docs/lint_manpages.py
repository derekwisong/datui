#!/usr/bin/env python3
"""Lint the manpages in crates/datui-cli/man: zero warnings from mandoc and groff.

For each page:

  mandoc -T lint -W warning PAGE     man(7) structure, macros, escapes
  groff -ww -z -man PAGE             every troff warning, at 78 columns
  groff -ww -z -man -rLL=60n PAGE    and at MANWIDTH=60: a line that cannot fit
  lexgrog PAGE                       the NAME line whatis and apropos read

A missing tool is skipped with a note, unless --require (CI installs them all).
The pages' content (sections, order, completeness) is checked by datui-cli's
`man::tests`; that they are current, by `the_generated_docs_are_current`.

Usage:
  lint_manpages.py [--require] [--mandoc PATH] [PAGE...]
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGES = ROOT / "crates" / "datui-cli" / "man"


def run(cmd: list[str]) -> str:
    """What `cmd` says on stdout and stderr; a failing status counts as a problem."""
    proc = subprocess.run(cmd, capture_output=True, text=True)
    said = (proc.stdout + proc.stderr).strip()
    if proc.returncode != 0 and not said:
        said = f"exit {proc.returncode}"
    return said


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--require", action="store_true", help="fail when a tool is missing")
    parser.add_argument("--mandoc", help="the mandoc binary (default: on PATH)")
    parser.add_argument("pages", nargs="*", type=Path)
    args = parser.parse_args()

    pages = args.pages or sorted(p for p in PAGES.iterdir() if p.suffix[1:].isdigit())
    tools = {
        "mandoc": args.mandoc or shutil.which("mandoc"),
        "groff": shutil.which("groff"),
        "lexgrog": shutil.which("lexgrog"),
    }
    missing = [name for name, path in tools.items() if not path]
    for name in missing:
        print(f"{'error' if args.require else 'note'}: {name} not found; its check is skipped")
    if missing and args.require:
        return 1

    problems: list[str] = []
    for page in pages:
        checks: list[tuple[str, list[str]]] = []
        if tools["mandoc"]:
            checks.append(("mandoc", [tools["mandoc"], "-T", "lint", "-W", "warning", str(page)]))
        if tools["groff"]:
            checks.append(("groff", [tools["groff"], "-ww", "-z", "-man", str(page)]))
            checks.append(("groff at 60", [tools["groff"], "-ww", "-z", "-man", "-rLL=60n", str(page)]))
        for label, cmd in checks:
            said = run(cmd)
            if said:
                problems.append(f"{page.name} ({label}):\n  " + said.replace("\n", "\n  "))
        if tools["lexgrog"]:
            proc = subprocess.run([tools["lexgrog"], str(page)], capture_output=True, text=True)
            name = page.name.rsplit(".", 1)[0]
            if proc.returncode != 0 or f"{name} - " not in proc.stdout:
                problems.append(f"{page.name} (lexgrog): no NAME line whatis can read: {proc.stdout.strip()}")
    for problem in problems:
        print(problem)
    print(f"{len(pages)} pages, {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
