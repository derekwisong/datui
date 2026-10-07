#!/usr/bin/env python3
"""Lint the book's structure (#683).

Fails on:
- a page whose H1 differs from its title in SUMMARY.md, or a page SUMMARY.md omits
- a heading in Title Case, or an empty one
- a link to an unpublished path (plans/, a file outside docs/), or to a page that
  does not exist
- a redirect in book.toml whose target page or heading does not exist
- a comment line starting a bash or TOML block, which some outlines show as a heading

Code blocks are checked by doc_examples.py; links in the built book by
check_doc_links.sh.

Usage: python3 scripts/docs/lint_docs.py
"""

from __future__ import annotations

import posixpath
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"

# Words that keep their capital inside a heading: names of things.
PROPER = {
    "Analysis", "Arrow", "AWS", "Azure", "Blob", "CAN", "CSV", "DataFlash", "Data", "Quality",
    "Describe", "Distribution", "Correlation", "ELF", "Excel", "FIX", "GCS", "GGUF", "GPS",
    "GPX", "Google", "Cloud", "Storage", "HTTP", "HTTPS", "Info", "IPC", "JSON", "MIDI",
    "NDJSON", "NMEA", "NumPy", "ORC", "Parquet", "Polars", "Python", "PyPI", "S3", "SDF",
    "SQL", "SQLite", "SafeTensors", "TOML", "TSV", "PSV", "ULog", "VCD", "WAV", "WinGet",
    "Windows", "Linux", "macOS", "Homebrew", "AUR", "Omarchy", "Arch", "Debian", "Ubuntu",
    "Fedora", "RHEL", "MinIO", "R2", "Ceph", "Hugging", "Face", "Sort", "Filter", "Pivot",
    "Melt", "Text", "Q", "UTF-8", "DBC", "QuickFIX", "Avro", "NOAA", "Overture", "GitHub",
    "Rust", "Cargo", "I", "ASCII", "Unicode", "API", "CLI", "Value", "Counts", "Kitty",
    "Alacritty", "Ghostty", "iTerm2", "tmux", "OSC", "Wayland", "X11", "Delta", "Iceberg",
    "Hudi", "XY", "KDE", "Box", "Plot", "Heatmap", "Bar", "Histogram", "Schema", "Notes",
    "Resources", "Partitions", "Model", "Audio", "SQLite's", "Enter", "Esc", "Tab", "Space", "Setup",
}

FENCE = re.compile(r"^\s*(```+|~~~+)(.*)$")
HEADING = re.compile(r"^(#{1,6})\s*(.*?)\s*#*\s*$")
LINK = re.compile(r"\]\(([^)\s]+)\)")


def slug(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text).replace("`", "")
    return "".join(c for c in text.lower().strip() if c.isalnum() or c in " -_").replace(" ", "-")


def outside_fences(text: str):
    """(line number, line) for every line outside code blocks."""
    fence = None
    for n, line in enumerate(text.splitlines(), 1):
        m = FENCE.match(line)
        if m:
            if fence is None:
                fence = m.group(1)
            elif m.group(1).startswith(fence[0]) and len(m.group(1)) >= len(fence):
                fence = None
            continue
        if fence is None:
            yield n, line


def anchors(path: Path) -> set[str]:
    ids: set[str] = set()
    seen: dict[str, int] = {}
    for _, line in outside_fences(path.read_text(encoding="utf-8")):
        m = HEADING.match(line)
        if m:
            s = slug(m.group(2))
            # mdBook numbers a repeated heading's id: name, name-1, ...
            n = seen.get(s, 0)
            ids.add(s if n == 0 else f"{s}-{n}")
            seen[s] = n + 1
        ids.update(re.findall(r'<a id="([^"]+)"', line))
    return ids


def summary_titles() -> dict[str, str]:
    titles = {}
    for line in (DOCS / "SUMMARY.md").read_text(encoding="utf-8").splitlines():
        m = re.match(r"\s*(?:- )?\[([^\]]+)\]\(([^)]+)\)", line)
        if m:
            titles[m.group(2)] = m.group(1)
    return titles


def title_case(text: str) -> bool:
    """More than one word after the first starts with a capital it does not need."""
    words = re.findall(r"[A-Za-z][\w'-]*", re.sub(r"`[^`]*`|\[[^\]]*\]\([^)]*\)|<[^>]+>", "", text))
    capped = [w for w in words[1:] if w[0].isupper() and w not in PROPER and not w.isupper()]
    return len(capped) >= 2


def lint() -> list[str]:
    problems: list[str] = []
    titles = summary_titles()
    pages = sorted(p for p in DOCS.rglob("*.md") if p.name != "SUMMARY.md")
    for page in pages:
        rel = page.relative_to(DOCS).as_posix()
        text = page.read_text(encoding="utf-8")
        if rel not in titles:
            problems.append(f"docs/{rel}: not in SUMMARY.md")
        h1 = next((m.group(2) for _, row in outside_fences(text) if (m := HEADING.match(row)) and len(m.group(1)) == 1), None)
        if rel in titles and h1 != titles[rel] and rel != "introduction.md":
            problems.append(f"docs/{rel}: H1 `{h1}` differs from SUMMARY.md's `{titles[rel]}`")
        for n, line in outside_fences(text):
            m = HEADING.match(line)
            if m:
                if not m.group(2).strip():
                    problems.append(f"docs/{rel}:{n}: an empty heading")
                elif title_case(m.group(2)):
                    problems.append(f"docs/{rel}:{n}: `{m.group(2)}` is in Title Case; use sentence case")
            for target in LINK.findall(line):
                if re.match(r"[a-z]+:", target) or target.startswith("#"):
                    if "plans/" in target:
                        problems.append(f"docs/{rel}:{n}: links to plans/, which is not published")
                    if target.startswith("#") and target[1:] not in anchors(page):
                        problems.append(f"docs/{rel}:{n}: no heading `{target}` on this page")
                    continue
                path, _, frag = target.partition("#")
                resolved = posixpath.normpath(posixpath.join(posixpath.dirname(rel), path))
                if resolved.startswith("demos/"):
                    continue  # the docs build copies demos/ into each book
                if resolved.startswith("reference/man/"):
                    # The docs build renders crates/datui-cli/man/ there.
                    page = ROOT / "crates/datui-cli/man" / posixpath.basename(resolved).removesuffix(".html")
                    if not page.exists():
                        problems.append(f"docs/{rel}:{n}: `{target}`: no manpage {page.name}")
                    continue
                if resolved == ".." and path.endswith("/"):
                    continue  # the landing page, one level above every book
                if resolved.startswith("..") or "plans/" in resolved:
                    problems.append(f"docs/{rel}:{n}: `{target}` is outside the book")
                    continue
                dest = DOCS / resolved
                if not dest.exists():
                    problems.append(f"docs/{rel}:{n}: `{target}` does not exist")
                elif frag and dest.suffix == ".md" and frag not in anchors(dest):
                    problems.append(f"docs/{rel}:{n}: `{target}`: no such heading")
        # Comment lines that open a bash or TOML block.
        fence = None
        for n, line in enumerate(text.splitlines(), 1):
            m = FENCE.match(line)
            if m:
                fence = None if fence else m.group(2).split(",")[0].strip()
                continue
            if fence in ("bash", "sh", "toml") and re.match(r"#(?!!)\s", line):
                problems.append(f"docs/{rel}:{n}: a comment line in a {fence} block; say it in the text or at the end of a line")

    book = tomllib.loads((ROOT / "book.toml").read_text(encoding="utf-8"))
    for source, target in book["output"]["html"].get("redirect", {}).items():
        page, _, frag = posixpath.normpath(posixpath.join(posixpath.dirname(source.lstrip("/")), target)).partition("#")
        md = DOCS / (page.removesuffix(".html") + ".md")
        if not md.exists():
            problems.append(f"book.toml: redirect {source} -> {target}: no such page")
        elif frag and frag not in anchors(md):
            problems.append(f"book.toml: redirect {source} -> {target}: no such heading")
    return problems


def main() -> int:
    problems = lint()
    for p in problems:
        print(p)
    if problems:
        print(f"{len(problems)} problem(s)")
        return 1
    print("ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
