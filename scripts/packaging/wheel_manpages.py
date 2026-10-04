#!/usr/bin/env python3
"""Put the manpages in the Python wheel, before `maturin build`.

Copies crates/datui-cli/man/* to python/datui.data/data/share/man/manN/ and names
that directory in pyproject.toml's [tool.maturin] `data`. pip installs a wheel's
data files under the environment's prefix, so `pip install datui` puts the pages in
<prefix>/share/man, where man looks for the commands in <prefix>/bin. Run it where
the wheel bundles the CLI (release and nightly, Linux and macOS); a build without it
has no pages, and maturin needs no data directory.

Usage: wheel_manpages.py
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGES = ROOT / "crates" / "datui-cli" / "man"
PYTHON = ROOT / "python"
DATA = "datui.data"


def main() -> int:
    share = PYTHON / DATA / "data" / "share" / "man"
    if share.exists():
        shutil.rmtree(share)
    count = 0
    for page in sorted(PAGES.iterdir()):
        section = page.suffix[1:]
        if not section.isdigit():
            continue
        dest = share / f"man{section}"
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(page, dest / page.name)
        count += 1
    pyproject = PYTHON / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    if not re.search(r'^data\s*=', text, re.M):
        text = text.replace("[tool.maturin]\n", f'[tool.maturin]\ndata = "{DATA}"\n', 1)
        pyproject.write_text(text, encoding="utf-8")
    print(f"{count} pages in python/{DATA}/data/share/man")
    return 0 if count else 1


if __name__ == "__main__":
    sys.exit(main())
