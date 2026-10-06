#!/usr/bin/env python3
"""Check and run every code block users read, and every example in examples.toml.

Every fenced block in docs/, the READMEs and the next release's notes, and every
<pre data-example="LANG,ATTRS"> on the landing page (scripts/docs/index.html.j2),
is one of:

  runnable  `bash`, `sh`, `toml`, `python`, `sql`, `q`, ...: copied and pasted on a
            fresh install, it works and does what the text says. This script runs it.
  template  `,template`: a shape to fill in. Its <PLACEHOLDERS> cannot be mistaken
            for real values, and the sentence before it says what to replace.
  output    `text`, `console`, or `,output`: what a command prints, or a screen.
  file      `,file=NAME`: a file an example needs, shown under its name. The
            page's next runnable block uses it by name; it is written into that
            block's directory before the block runs. Shell blocks never make
            files with heredocs, and never generate data with `python3 -c`.

Attributes after the language, comma-separated:

  template        not run; checked: flags exist, TOML parses once filled in
  output          what something prints; not run
  network         reads public data: run with --network (the nightly job)
  interactive     its producer never ends; stopped once the first rows show
  continue        runs in the directory the page's previous block ran in
  expect=WHAT     what a datui command in it must do: rows, screen or exit
  spec            (toml) a format spec, checked with `datui formats check`
  catalog         (toml) a catalog file, checked with `datui catalog check`
  theme=NAME      (toml) a theme file, written as themes/NAME.toml and shown
                  with `datui theme show NAME`; a warning fails it
  dataset=NAME    (sql, q) the dataset it runs against, from doc_datasets.toml;
                  crates/datui-lib's doc_queries tests run it
  rows=N          (sql, q) the rows it returns
  repo            (bash) run from a checkout of the repository; its scripts
                  must exist, and it is not run here
  install         installs datui; test-install.yml installs it, not this script
  file=NAME       a file the page's next runnable block uses: not run itself

Usage:
  doc_examples.py --lint                 labels and templates; needs no binary
  doc_examples.py --bin target/debug/datui [--network] [-k TEXT]
                                         run the runnable blocks and examples
  doc_examples.py --python [--network]   run the python blocks (needs the wheel)
  doc_examples.py --fetch-datasets DIR   download the datasets queries name

A failing block names its file and line.
"""

from __future__ import annotations

import argparse
import html
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import tomllib
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SHIM = Path(__file__).resolve().parent / "datui_shim.py"
DATASETS = Path(__file__).resolve().parent / "doc_datasets.toml"

RUNNABLE = {"bash", "sh", "toml", "python", "sql", "q", "powershell"}
OUTPUT = {"text", "console"}
# Code that is shown, never run: contributor docs' excerpts.
EXCERPT = {"rust", "yaml", "json", "html", "xml", "csv", "diff", "markdown"}
ATTRS = {"template", "output", "network", "interactive", "continue", "spec", "catalog", "repo", "install"}
VALUED = {"expect", "dataset", "rows", "file", "theme"}
PLACEHOLDER = re.compile(r"<[A-Z][A-Z0-9_]*>")
FENCE = re.compile(r"^(\s*)(```+|~~~+)(.*)$")
# A file written from a shell block, or data made by a one-liner: a file block instead.
HEREDOC = re.compile(r"<<-?\s*['\"]?[A-Za-z_]\w*")
ONE_LINER = re.compile(r"\bpython3?\s+-c\b")


@dataclass
class Block:
    path: Path
    line: int
    lang: str
    attrs: set[str] = field(default_factory=set)
    values: dict[str, str] = field(default_factory=dict)
    body: str = ""
    before: str = ""  # the paragraph before the block
    files: list[Block] = field(default_factory=list)  # file blocks written before it runs

    def where(self) -> str:
        return f"{self.path.relative_to(ROOT)}:{self.line}"

    @property
    def kind(self) -> str:
        if "file" in self.values:
            return "file"
        if "template" in self.attrs:
            return "template"
        if "output" in self.attrs or self.lang in OUTPUT:
            return "output"
        if self.lang in EXCERPT:
            return "excerpt"
        return "runnable"


def user_files() -> list[Path]:
    """Every Markdown file users read."""
    files = sorted((ROOT / "docs").rglob("*.md"))
    for name in ["README.md", "python/README.md", "crates/datui-cli/README.md", "crates/datui-lib/README.md"]:
        if (ROOT / name).exists():
            files.append(ROOT / name)
    version = re.search(r'^version = "([^"]+)"', (ROOT / "Cargo.toml").read_text(), re.M)
    if version:
        notes = ROOT / "release-notes" / f"v{version.group(1).removesuffix('-dev')}.md"
        if notes.exists():
            files.append(notes)
    return files


def parse_info(info: str) -> tuple[str, set[str], dict[str, str], list[str]]:
    parts = [p.strip() for p in info.split(",") if p.strip()]
    lang = parts[0] if parts else ""
    attrs: set[str] = set()
    values: dict[str, str] = {}
    unknown: list[str] = []
    for part in parts[1:]:
        if "=" in part:
            key, _, value = part.partition("=")
            (values.__setitem__(key, value) if key in VALUED else unknown.append(part))
        elif part in ATTRS:
            attrs.add(part)
        else:
            unknown.append(part)
    return lang, attrs, values, unknown


def blocks_in(path: Path, problems: list[str]) -> list[Block]:
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[Block] = []
    paragraph: list[str] = []  # the text since the last blank line or block
    last = ""  # the paragraph before the next block
    i = 0
    while i < len(lines):
        m = FENCE.match(lines[i])
        if not m:
            if lines[i].strip():
                paragraph.append(lines[i].strip())
            elif paragraph:
                last, paragraph = " ".join(paragraph), []
            i += 1
            continue
        if paragraph:
            last, paragraph = " ".join(paragraph), []
        indent, fence, info = m.groups()
        lang, attrs, values, unknown = parse_info(info)
        start = i
        body: list[str] = []
        i += 1
        while i < len(lines) and not lines[i].strip().startswith(fence):
            body.append(lines[i][len(indent):] if lines[i].startswith(indent) else lines[i])
            i += 1
        block = Block(path, start + 1, lang, attrs, values, "\n".join(body) + "\n", last)
        if not lang:
            problems.append(f"{block.where()}: a fence with no language; label it (bash, toml, text, ...)")
        elif lang not in RUNNABLE | OUTPUT | EXCERPT:
            problems.append(f"{block.where()}: unknown language `{lang}`")
        for u in unknown:
            problems.append(f"{block.where()}: unknown fence attribute `{u}`")
        out.append(block)
        last = ""
        i += 1
    attach_files(out, problems)
    return out


def attach_files(blocks: list[Block], problems: list[str]) -> None:
    """Give each file block to the next runnable block, which must use it by name."""
    pending: list[Block] = []
    for b in blocks:
        if b.kind == "file":
            name = b.values["file"]
            if not name or "/" in name or name.startswith("."):
                problems.append(f"{b.where()}: `file={name}` is not a plain file name")
            pending.append(b)
        elif b.kind == "runnable" and pending:
            for f in pending:
                if f.values["file"] not in b.body:
                    problems.append(f"{f.where()}: {f.values['file']} is not used by the next runnable block ({b.where()})")
            b.files, pending = pending, []
    for f in pending:
        problems.append(f"{f.where()}: {f.values['file']} has no runnable block after it to use it")


LANDING = ROOT / "scripts/docs/index.html.j2"
PRE = re.compile(r"<pre\b([^>]*)>(.*?)</pre>", re.S)
EXAMPLE = re.compile(r'\bdata-example="([^"]*)"')


def blocks_in_html(path: Path, problems: list[str]) -> list[Block]:
    """The landing page's commands: each <pre> says what it is in `data-example`."""
    text = path.read_text(encoding="utf-8")
    out: list[Block] = []
    for m in PRE.finditer(text):
        line = text.count("\n", 0, m.start()) + 1
        info = EXAMPLE.search(m.group(1))
        where = f"{path.relative_to(ROOT)}:{line}"
        if not info:
            problems.append(f'{where}: a <pre> with no data-example="LANG,..."')
            continue
        lang, attrs, values, unknown = parse_info(html.unescape(info.group(1)))
        body = html.unescape(re.sub(r"<[^>]+>", "", m.group(2))).strip("\n") + "\n"
        if "{{" in body or "{%" in body:
            problems.append(f"{where}: a command holds Jinja; write it out")
        block = Block(path, line, lang, attrs, values, body)
        if lang not in RUNNABLE | OUTPUT | EXCERPT:
            problems.append(f"{where}: unknown language `{lang}`")
        for u in unknown:
            problems.append(f"{where}: unknown example attribute `{u}`")
        out.append(block)
    return out


def all_blocks(problems: list[str]) -> list[Block]:
    out: list[Block] = []
    for path in user_files():
        out.extend(blocks_in(path, problems))
    if LANDING.exists():
        out.extend(blocks_in_html(LANDING, problems))
    return out


def example_blocks() -> list[Block]:
    """examples.toml's entries, as blocks."""
    data = tomllib.loads((ROOT / "crates/datui-cli/examples.toml").read_text(encoding="utf-8"))
    out = []
    text = (ROOT / "crates/datui-cli/examples.toml").read_text(encoding="utf-8").splitlines()
    for example in data["example"]:
        line = next((n + 1 for n, l in enumerate(text) if l.startswith("command") and tomllib.loads(l)["command"] == example["command"]), 1)
        attrs = set()
        if example["test"] in ("network", "interactive"):
            attrs.add(example["test"])
        values = {"expect": example["expect"]} if "expect" in example else {}
        where = ROOT / "crates/datui-cli/examples.toml"
        # Its files, written into its directory before it runs, as a page's file blocks are.
        files = [
            Block(where, line, Path(f["name"]).suffix.lstrip(".") or "text", set(), {"file": f["name"]}, f["text"])
            for f in example.get("files", [])
        ]
        out.append(Block(where, line, "bash", attrs, values, example["command"] + "\n", example["description"], files))
    return out


def known_flags() -> set[str]:
    """Every flag of datui and its commands, from the generated reference."""
    page = (ROOT / "docs/reference/command-line-options.md").read_text(encoding="utf-8")
    flags = set(re.findall(r"`(?:-\w, )?(--[a-z][a-z0-9-]*)", page))
    flags |= set(re.findall(r"`(-[a-zA-Z])\b", page))
    flags |= {"--help", "-h", "--version", "-V", "--force", "--recents", "--list", "--dir"}
    return flags


def datui_invocations(body: str) -> list[list[str]]:
    """The datui command lines in a shell block, as words."""
    out = []
    joined = body.replace("\\\n", " ")
    for line in joined.splitlines():
        line = line.split(" #", 1)[0] if not line.lstrip().startswith("#") else ""
        for part in re.split(r"\|\||&&|;|\|", line):
            words = part.strip().split()
            while words and "=" in words[0] and not words[0].startswith("-"):
                words = words[1:]  # VAR=value prefixes
            if words and words[0] == "datui":
                out.append(words)
    return out


def lint(blocks: list[Block], problems: list[str]) -> None:
    flags = known_flags()
    for b in blocks:
        if b.kind == "template":
            if not b.before.strip():
                problems.append(f"{b.where()}: a template needs a sentence before it saying what to replace")
            if b.lang == "toml":
                filled = PLACEHOLDER.sub("x", b.body)
                try:
                    tomllib.loads(filled)
                except tomllib.TOMLDecodeError as e:
                    problems.append(f"{b.where()}: template TOML does not parse once filled in: {e}")
        if b.kind == "runnable" and b.lang in RUNNABLE and PLACEHOLDER.search(b.body):
            problems.append(
                f"{b.where()}: {PLACEHOLDER.search(b.body).group(0)} is a placeholder; mark the block `template`"
            )
        if b.lang in ("bash", "sh", "powershell") and b.kind in ("runnable", "template"):
            if m := HEREDOC.search(b.body):
                problems.append(f"{b.where()}: `{m.group(0)}` is a heredoc; show the file as a `,file=NAME` block")
            if ONE_LINER.search(b.body):
                problems.append(f"{b.where()}: a `python -c` one-liner; show the script as a `,file=NAME` block")
        if b.lang in ("bash", "sh") and b.kind in ("runnable", "template"):
            for words in datui_invocations(b.body):
                for w in words[1:]:
                    flag = w.split("=", 1)[0]
                    if flag.startswith("-") and flag != "-" and flag not in flags:
                        problems.append(f"{b.where()}: `{flag}` is not a datui flag")
        if "repo" in b.attrs:
            for script in re.findall(r"(?:\./)?(scripts/[\w./-]+)", b.body):
                if not (ROOT / script).exists():
                    problems.append(f"{b.where()}: {script} does not exist")
        if b.lang in ("sql", "q") and b.kind == "runnable" and "dataset" not in b.values:
            problems.append(f"{b.where()}: a {b.lang} block names its dataset=NAME, or is a template")
        if "dataset" in b.values:
            names = tomllib.loads(DATASETS.read_text(encoding="utf-8"))
            entry = names.get(b.values["dataset"])
            if entry is None:
                problems.append(f"{b.where()}: dataset `{b.values['dataset']}` is not in {DATASETS.name}")
            elif ("url" in entry or "open" in entry) != ("network" in b.attrs):
                want = "needs `network`: its dataset is public data" if "path" not in entry else "is local data: drop `network`"
                problems.append(f"{b.where()}: {want}")
        if b.lang == "toml" and b.kind in ("runnable", "file") and "spec" not in b.attrs:
            try:
                tomllib.loads(b.body)
            except tomllib.TOMLDecodeError as e:
                problems.append(f"{b.where()}: TOML does not parse: {e}")


def selected(b: Block, args) -> bool:
    if args.only and args.only not in b.where() and args.only not in b.body:
        return False
    if b.kind != "runnable" or b.attrs & {"repo", "install"}:
        return False
    if b.lang not in ("bash", "sh", "toml", "python"):
        return False
    if (b.lang == "python") != args.python:
        return False
    return ("network" in b.attrs) == args.network


def environment(work: Path, real: str | None) -> dict[str, str]:
    env = dict(os.environ)
    shim_dir = work / ".bin"
    shim_dir.mkdir(exist_ok=True)
    if real:
        target = shim_dir / "datui"
        if not target.exists():
            target.symlink_to(SHIM)
        env["DATUI_DOC_BIN"] = real
        env["PATH"] = f"{shim_dir}{os.pathsep}{env['PATH']}"
    env["DATUI_CONFIG_DIR"] = str(work / ".config")
    env["DATUI_CACHE_DIR"] = str(work / ".cache")
    # A block that installs for the user (`~/.local/share/man`, a completion script)
    # installs into its own directory, not the home of whoever runs this.
    env["HOME"] = str(work)
    for xdg in ("XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME"):
        env.pop(xdg, None)
    env.pop("DATUI_FORMATS_PATH", None)
    env.pop("DATUI_DOC_EXPECT", None)
    env["DATUI_DOC_STATUS"] = str(work / ".datui-status")
    return env


def datui_succeeded(work: Path) -> bool:
    """Whether datui ran in the block, through the shim, and every run exited 0."""
    try:
        said = (work / ".datui-status").read_text(encoding="utf-8").split()
    except OSError:
        return False
    return bool(said) and all(s == "0" for s in said)


# 128 + SIGPIPE: a producer still writing when the pipe it writes to closed.
SIGPIPE_EXIT = 141


def last_statement_line(body: str) -> int:
    """The 1-based line of the body's last statement's start, past comments, blanks
    and backslash continuations."""
    lines = body.splitlines()
    i = len(lines) - 1
    while i >= 0 and (not lines[i].strip() or lines[i].lstrip().startswith("#")):
        i -= 1
    while i > 0 and lines[i - 1].rstrip().endswith("\\"):
        i -= 1
    return i + 1


def failed_on_last_statement(work: Path, body: str, prelude_lines: int) -> bool:
    """Whether the ERR trap fired on the block's last statement: a SIGPIPE there
    ends a block whose every line ran."""
    try:
        line = int((work / ".doc-err-line").read_text(encoding="utf-8").split()[-1])
    except (OSError, ValueError, IndexError):
        return False
    return line - prelude_lines >= last_statement_line(body)


def run_block(b: Block, work: Path, real: str | None, timeout: float) -> str | None:
    """Run one block in `work`; the failure, or None."""
    env = environment(work, real)
    (work / ".datui-status").unlink(missing_ok=True)
    # The contributed specs a page shows, where a checkout of the repository has them.
    if (ROOT / "contrib").is_dir():
        shutil.copytree(ROOT / "contrib", work / "contrib", dirs_exist_ok=True)
    for f in b.files:
        (work / f.values["file"]).write_text(f.body, encoding="utf-8")
    if "expect" in b.values:
        env["DATUI_DOC_EXPECT"] = b.values["expect"]
    if b.lang in ("bash", "sh"):
        # An interactive block's producer is stopped by a closed pipe: its status is
        # not the block's.
        prelude = "set -eu\n" if "interactive" in b.attrs else "set -euo pipefail\n"
        # Where a failure stopped the block, for the SIGPIPE rule below.
        prelude += f"trap 'echo $LINENO > {shlex.quote(str(work / '.doc-err-line'))}' ERR\n"
        cmd = ["bash", "-c", prelude + b.body]
    elif b.lang == "toml" and "spec" in b.attrs:
        (work / "spec.toml").write_text(b.body, encoding="utf-8")
        cmd = [real, "formats", "check", "./spec.toml"]
    elif b.lang == "toml" and "catalog" in b.attrs:
        (work / "catalog.toml").write_text(b.body, encoding="utf-8")
        cmd = [real, "catalog", "check", "./catalog.toml"]
    elif b.lang == "toml" and "theme" in b.values:
        themes = work / ".config" / "themes"
        themes.mkdir(parents=True, exist_ok=True)
        (themes / f"{b.values['theme']}.toml").write_text(b.body, encoding="utf-8")
        cmd = [real, "theme", "show", b.values["theme"]]
    elif b.lang == "toml":
        config = work / ".config"
        config.mkdir(exist_ok=True)
        (config / "config.toml").write_text(b.body, encoding="utf-8")
        cmd = [real, "config", "keys"]
    elif b.lang == "python":
        script = work / "block.py"
        script.write_text(b.body, encoding="utf-8")
        env["DATUI_DOC_BIN"] = sys.executable
        cmd = [sys.executable, str(SHIM), "--command", sys.executable, str(script)]
    else:
        return None
    try:
        proc = subprocess.run(
            cmd, cwd=work, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        return f"timed out after {timeout:.0f}s"
    said = (proc.stdout + proc.stderr).strip()
    # Rows show as they arrive, so datui can be done while the producer still writes
    # (`journalctl | datui`); quit, it closes the pipe, as `less` does, and pipefail
    # reports the producer's SIGPIPE. That is the block's success when datui's is
    # and nothing after it was skipped.
    if (
        proc.returncode == SIGPIPE_EXIT
        and b.lang in ("bash", "sh")
        and datui_succeeded(work)
        and failed_on_last_statement(work, b.body, prelude.count("\n"))
    ):
        return None
    if proc.returncode != 0:
        return f"exit {proc.returncode}: {said[-2000:]}"
    if b.lang == "toml" and "warning" in proc.stderr:
        return proc.stderr.strip()
    return None


def fetch_datasets(dest: Path, blocks: list[Block]) -> int:
    names = tomllib.loads(DATASETS.read_text(encoding="utf-8"))
    wanted = {b.values["dataset"] for b in blocks if "dataset" in b.values}
    dest.mkdir(parents=True, exist_ok=True)
    for name in sorted(wanted):
        url = names[name].get("url")
        if not url:
            continue
        target = dest / f"{name}{Path(url).suffix}"
        if not target.exists():
            print(f"  downloading {url}")
            part = target.with_suffix(target.suffix + ".part")
            urllib.request.urlretrieve(url, part)
            part.rename(target)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--lint", action="store_true", help="check labels and templates only")
    parser.add_argument("--bin", help="the datui binary to run blocks with")
    parser.add_argument("--network", action="store_true", help="run the network blocks instead")
    parser.add_argument("--python", action="store_true", help="run the python blocks (needs the wheel)")
    parser.add_argument("--fetch-datasets", type=Path, metavar="DIR", help="download the datasets queries name")
    parser.add_argument("-k", dest="only", help="only blocks whose file:line or text contains this")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--list", action="store_true", help="list the blocks that would run")
    args = parser.parse_args()

    problems: list[str] = []
    blocks = all_blocks(problems)
    examples = example_blocks()
    lint(blocks + examples, problems)
    if args.fetch_datasets:
        return fetch_datasets(args.fetch_datasets, blocks)
    if args.lint and problems:
        print("\n".join(problems))
        print(f"{len(problems)} problem(s) in {len(blocks)} blocks")
        return 1
    if args.lint:
        kinds: dict[str, int] = {}
        for b in blocks:
            kinds[b.kind] = kinds.get(b.kind, 0) + 1
        print(f"ok: {len(blocks)} blocks ({', '.join(f'{n} {k}' for k, n in sorted(kinds.items()))}), {len(examples)} examples")
        return 0
    if not args.bin and not args.python:
        parser.error("--bin is required to run blocks")
    real = str(Path(args.bin).resolve()) if args.bin else None

    chosen = [b for b in blocks + examples if selected(b, args)]
    if args.list:
        for b in chosen:
            print(b.where(), b.lang, ",".join(sorted(b.attrs)))
        return 0
    failed = 0
    scratch = Path(tempfile.mkdtemp(prefix="datui-doc-examples-"))
    last_dir: dict[Path, Path] = {}
    try:
        for n, b in enumerate(chosen):
            if "continue" in b.attrs and b.path in last_dir:
                work = last_dir[b.path]
            else:
                work = scratch / f"b{n}"
                work.mkdir()
            last_dir[b.path] = work
            error = run_block(b, work, real, args.timeout)
            print(f"{'ok  ' if error is None else 'FAIL'} {b.where()} {b.lang}")
            if error:
                failed += 1
                for line in error.splitlines()[-15:]:
                    print(f"     {line}")
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    print(f"{len(chosen) - failed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
