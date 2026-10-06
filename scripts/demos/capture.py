#!/usr/bin/env python3
"""Record datui's captures with VHS: the two GIFs, the docs screenshots, the theme gallery.

Each take runs in a fixture of its own: a scratch HOME, config and cache, an empty
working directory, no shell history and no cloud credentials. datui on the take's
PATH is a wrapper that adds `-c cloud.discover=false` and the take's theme, so the
tapes type plain `datui`. Every tape gets header.tape's settings: one size, font
and theme for all of them.

Outputs land in --out (default: ~/tmp/datui-captures),
never in the docs. --publish copies reviewed outputs from there into demos/.

Usage:
    scripts/demos/capture.py --list
    scripts/demos/capture.py --bin target/release/datui            # every tape
    scripts/demos/capture.py --bin target/release/datui teaser     # one
    scripts/demos/capture.py --dry-run
    scripts/demos/capture.py --publish                              # copy into demos/
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

DEMOS_DIR = Path(__file__).resolve().parent
REPO = DEMOS_DIR.parents[1]
HEADER = DEMOS_DIR / "header.tape"
CONTRIB_THEMES = REPO / "contrib" / "themes"
PUBLISH_DIR = REPO / "demos"
# Not the system temp directory: on some machines it is RAM, and a full run is
# hundreds of MB with its fixtures.
DEFAULT_OUT = Path.home() / "tmp" / "datui-captures"

# Prefixes and names of variables that could put a login, a project, a history or
# a color override from the recording machine into a take.
SCRUBBED_PREFIXES = (
    "AWS_",
    "AZURE_",
    "GOOGLE_",
    "GCLOUD_",
    "GCP_",
    "CLOUDSDK_",
    "ARM_",
    "MC_HOST_",
    "DATUI_",
    "XDG_",
)
SCRUBBED_NAMES = {
    "ECS_CONTAINER_CREDENTIALS_FULL_URI",
    "ECS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "IDENTITY_ENDPOINT",
    "IDENTITY_HEADER",
    "MSI_ENDPOINT",
    "MSI_SECRET",
    "K_SERVICE",
    "NO_COLOR",
    "FORCE_COLOR",
    "COLORFGBG",
    "HISTFILE",
    "PROMPT_COMMAND",
    "BASH_ENV",
    "ENV",
    "TMUX",
    "TMUX_PANE",
    "OLDPWD",
    "VISUAL",
    "EDITOR",
    "PAGER",
}
# XDG_RUNTIME_DIR is a socket directory, not settings; Chromium wants it.
KEPT_XDG = {"XDG_RUNTIME_DIR"}


@dataclass(frozen=True)
class Tape:
    name: str
    kind: str  # "gif": GIF, WebM and poster; "shots": screenshots only
    used_on: str
    expected: str  # measured on a home connection; a GIF's is the clip's length


@dataclass(frozen=True)
class Look:
    """A theme for one take: datui's flags, and the terminal colors behind them."""

    name: str
    flags: tuple[str, ...]
    terminal: str = ""  # a VHS `Set Theme` value; empty keeps header.tape's
    theme_file: str = ""  # a contrib/themes file the take installs


NIGHT_MARKET = Look("night-market", ("-c", "theme.mode=dark"))

GALLERY = (
    NIGHT_MARKET,
    Look(
        "day-market",
        ("-c", "theme.mode=light"),
        terminal='{ "name": "Tokyo Night Day", "background": "#e1e2e7", "foreground": "#3760bf", '
        '"cursor": "#3760bf", "selection": "#b6bfe2", "black": "#e9e9ed", "red": "#f52a65", '
        '"green": "#587539", "yellow": "#8c6c3e", "blue": "#2e7de9", "magenta": "#9854f1", '
        '"cyan": "#007197", "white": "#6172b0", "brightBlack": "#a1a6c5", "brightRed": "#f52a65", '
        '"brightGreen": "#587539", "brightYellow": "#8c6c3e", "brightBlue": "#2e7de9", '
        '"brightMagenta": "#9854f1", "brightCyan": "#007197", "brightWhite": "#3760bf" }',
    ),
    Look(
        "gruvbox-dark",
        ("-c", "theme.mode=dark", "-c", "theme.dark=gruvbox-dark"),
        terminal='{ "name": "Gruvbox Dark", "background": "#282828", "foreground": "#ebdbb2", '
        '"cursor": "#ebdbb2", "selection": "#504945", "black": "#282828", "red": "#cc241d", '
        '"green": "#98971a", "yellow": "#d79921", "blue": "#458588", "magenta": "#b16286", '
        '"cyan": "#689d6a", "white": "#a89984", "brightBlack": "#928374", "brightRed": "#fb4934", '
        '"brightGreen": "#b8bb26", "brightYellow": "#fabd2f", "brightBlue": "#83a598", '
        '"brightMagenta": "#d3869b", "brightCyan": "#8ec07c", "brightWhite": "#ebdbb2" }',
        theme_file="gruvbox-dark.toml",
    ),
    Look(
        "high-contrast",
        ("-c", "theme.mode=dark", "-c", "theme.dark=high-contrast"),
        terminal='{ "name": "Black", "background": "#000000", "foreground": "#ffffff", '
        '"cursor": "#ffffff", "selection": "#005f87", "black": "#000000", "red": "#ff5f5f", '
        '"green": "#5fff87", "yellow": "#ffd75f", "blue": "#5fd7ff", "magenta": "#ff87ff", '
        '"cyan": "#00d7ff", "white": "#d0d0d0", "brightBlack": "#808080", "brightRed": "#ff5f5f", '
        '"brightGreen": "#5fff87", "brightYellow": "#ffd75f", "brightBlue": "#87afff", '
        '"brightMagenta": "#ff87ff", "brightCyan": "#87ffff", "brightWhite": "#ffffff" }',
        theme_file="high-contrast.toml",
    ),
)

TAPES = {
    t.name: t
    for t in (
        Tape("teaser", "gif", "README hero; landing page", "~25 s clip"),
        Tape("noaa-cloud", "gif", "Remote data guide; landing if it earns it", "~40 s clip"),
        Tape("shots-home", "shots", "Home screen; Catalogs", "~10 s"),
        Tape("shots-quick-start", "shots", "Quick start", "~20 s"),
        Tape("shots-flights-query", "shots", "Table; Query data", "~20 s"),
        Tape("shots-flights-charts", "shots", "Make a chart", "~30 s"),
        Tape("shots-football", "shots", "Query data: dates and messy text", "~15 s"),
        Tape("shots-food", "shots", "Sort, filter and arrange columns; Copy", "~30 s"),
        Tape("shots-names", "shots", "Pivot and melt; Make a chart", "~15 s"),
        Tape("shots-quakes", "shots", "Make a chart", "~10 s"),
        Tape("shots-excel", "shots", "Columnar and Excel formats", "~10 s"),
        Tape("shots-noaa-remote", "shots", "Connect to cloud storage", "~15 s"),
        Tape("shots-noaa-views", "shots", "Save and apply views", "~20 s"),
        Tape("theme-gallery", "gallery", "Configuration; landing", "~7 s per theme"),
    )
}


@dataclass
class Take:
    """One run of one tape: where its fixture lives and what it writes."""

    tape: Tape
    look: Look
    root: Path
    out: Path
    outputs: list[Path] = field(default_factory=list)

    @property
    def label(self) -> str:
        return self.tape.name if self.tape.kind != "gallery" else f"{self.tape.name}:{self.look.name}"

    @property
    def home(self) -> Path:
        return self.root / "home"

    @property
    def work(self) -> Path:
        # Under HOME, so the home screen names it `~/demo`, not the scratch path.
        return self.home / "demo"

    @property
    def bin_dir(self) -> Path:
        return self.root / "bin"

    @property
    def config_dir(self) -> Path:
        return self.root / "config" / "datui"

    @property
    def cache_dir(self) -> Path:
        return self.root / "cache" / "datui"

    @property
    def shots_dir(self) -> Path:
        if self.tape.kind == "gallery":
            return self.out / "themes"
        return self.out / "screenshots"


def take_env(base: dict[str, str], take: Take, cache_dir: Path | None = None) -> dict[str, str]:
    """The environment a take runs in: the machine's, minus anything personal."""
    env = {
        k: v
        for k, v in base.items()
        if k not in SCRUBBED_NAMES and (k in KEPT_XDG or not k.startswith(SCRUBBED_PREFIXES))
    }
    lang = env.get("LC_ALL") or env.get("LC_CTYPE") or env.get("LANG") or ""
    if "UTF-8" not in lang.upper().replace("UTF8", "UTF-8"):
        env.pop("LC_ALL", None)
        env.pop("LC_CTYPE", None)
        env["LANG"] = "C.UTF-8"
    env.update(
        {
            "HOME": str(take.home),
            "PWD": str(take.work),
            "XDG_CONFIG_HOME": str(take.root / "config"),
            "XDG_CACHE_HOME": str(take.root / "cache"),
            "XDG_DATA_HOME": str(take.root / "data"),
            "XDG_STATE_HOME": str(take.root / "state"),
            "DATUI_CONFIG_DIR": str(take.config_dir),
            "DATUI_CACHE_DIR": str(cache_dir or take.cache_dir),
            "HISTFILE": "/dev/null",
            "HISTSIZE": "0",
            "PS1": "$ ",
            "COLORTERM": "truecolor",
            "PATH": f"{take.bin_dir}{os.pathsep}{base.get('PATH', os.defpath)}",
        }
    )
    return env


def wrapper_script(binary: Path, look: Look, tmp: Path) -> str:
    """`datui` on the take's PATH: the binary under test with the take's settings.

    Its downloads go to the take's own temp directory. Only datui's: Chromium's
    socket path under a TMPDIR this deep is too long, and VHS cannot start it.
    """
    flags = ["-c", "cloud.discover=false", *look.flags]
    return f'#!/bin/sh\nTMPDIR={shlex.quote(str(tmp))} exec {shlex.quote(str(binary))} {shlex.join(flags)} "$@"\n'


SCREENSHOT = re.compile(r'^\s*Screenshot\s+"?([A-Za-z0-9_.-]+\.png)"?\s*$')
FORBIDDEN = re.compile(r"^\s*(Output|Set|Source|Require)\b")


def prepare_tape(body: str, header: str, take: Take) -> tuple[str, list[Path]]:
    """header.tape and the tape, with outputs and screenshot paths made absolute."""
    lines: list[str] = []
    shots: list[Path] = []
    for n, line in enumerate(body.splitlines(), 1):
        if FORBIDDEN.match(line):
            raise ValueError(f"{take.tape.name}.tape:{n}: settings and outputs belong to header.tape and capture.py")
        m = SCREENSHOT.match(line)
        if m:
            name = m.group(1)
            if take.tape.kind == "gallery":
                name = f"{take.look.name}.png"
            path = take.shots_dir / name
            shots.append(path)
            lines.append(f'Screenshot "{path}"')
            # VHS takes the shot from a later frame: hold still until it has.
            lines.append("Sleep 500ms")
            continue
        lines.append(line)
    if take.tape.kind == "gallery" and len(shots) != 1:
        raise ValueError(f"{take.tape.name}.tape: a gallery tape takes exactly one Screenshot")
    if take.tape.kind == "shots" and not shots:
        raise ValueError(f"{take.tape.name}.tape: takes no Screenshot")

    if take.tape.kind == "gif":
        videos = [take.out / f"{take.tape.name}.gif", take.out / f"{take.tape.name}.webm"]
    else:
        videos = [take.out / "review" / f"{take.label.replace(':', '-')}.webm"]
    head = [f'Output "{v}"' for v in videos]
    head.append(header.strip())
    if take.look.terminal:
        head.append(f"Set Theme {take.look.terminal}")
    return "\n".join([*head, *lines]) + "\n", [*videos, *shots]


def plan(names: list[str], out: Path, scratch: Path) -> list[Take]:
    takes = []
    for name in names:
        tape = TAPES[name]
        looks = GALLERY if tape.kind == "gallery" else (NIGHT_MARKET,)
        for look in looks:
            label = name if tape.kind != "gallery" else f"{name}-{look.name}"
            takes.append(Take(tape, look, scratch / label, out))
    return takes


def build_fixture(take: Take, binary: Path) -> None:
    if take.root.exists():
        shutil.rmtree(take.root)
    for d in (take.home, take.work, take.bin_dir, take.config_dir, take.cache_dir, take.root / "tmp", take.shots_dir):
        d.mkdir(parents=True, exist_ok=True)
    (take.out / "review").mkdir(parents=True, exist_ok=True)
    wrapper = take.bin_dir / "datui"
    wrapper.write_text(wrapper_script(binary, take.look, take.root / "tmp"))
    wrapper.chmod(0o755)
    if take.look.theme_file:
        themes = take.config_dir / "themes"
        themes.mkdir(parents=True, exist_ok=True)
        shutil.copy2(CONTRIB_THEMES / take.look.theme_file, themes / take.look.theme_file)


def run(cmd: list[str], **kw) -> str:
    try:
        return subprocess.run(cmd, check=True, capture_output=True, text=True, **kw).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def machine_facts(binary: Path, network_note: str) -> dict:
    """What a caption may need to say about where a take was recorded."""
    cpu = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        cpu = platform.processor()
    mem_gb = None
    try:
        kb = int(Path("/proc/meminfo").read_text().split()[1])
        mem_gb = round(kb / 1024 / 1024)
    except (OSError, ValueError, IndexError):
        pass
    iface = ""
    route = run(["ip", "route", "get", "1.1.1.1"])
    m = re.search(r"\bdev (\S+)", route)
    if m:
        iface = m.group(1)
        iface += " (wireless)" if Path(f"/sys/class/net/{m.group(1)}/wireless").exists() else " (wired)"
    commit = run(["git", "-C", str(REPO), "rev-parse", "--short", "HEAD"])
    dirty = bool(run(["git", "-C", str(REPO), "status", "--porcelain", "--untracked-files=no"]))
    return {
        "datui_version": run([str(binary), "--version"]),
        "commit": commit + ("-dirty" if dirty else ""),
        "binary": str(binary),
        "hardware": {
            "cpu": cpu,
            "cores": os.cpu_count(),
            "memory_gb": mem_gb,
            "os": platform.platform(),
        },
        "network": {"note": network_note, "interface": iface},
        "vhs": run(["vhs", "--version"]),
    }


def media_seconds(path: Path) -> float | None:
    out = run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)])
    try:
        return round(float(out), 1)
    except ValueError:
        return None


def last_frame(video: Path, png: Path) -> bool:
    png.unlink(missing_ok=True)
    run(["ffmpeg", "-loglevel", "error", "-y", "-sseof", "-0.3", "-i", str(video), "-frames:v", "1", "-update", "1", str(png)])
    return png.exists()


def compose_gallery(out: Path) -> Path | None:
    """The theme shots, two by two, in GALLERY order."""
    shots = [out / "themes" / f"{look.name}.png" for look in GALLERY]
    if not all(s.exists() for s in shots):
        return None
    target = out / "theme-gallery.png"
    cmd = ["ffmpeg", "-loglevel", "error", "-y"]
    for s in shots:
        cmd += ["-i", str(s)]
    cmd += ["-filter_complex", "xstack=inputs=4:layout=0_0|w0_0|0_h0|w0_h0", "-frames:v", "1", str(target)]
    run(cmd)
    return target if target.exists() else None


def header_value(header: str, key: str) -> str:
    m = re.search(rf"^Set {key} (.+)$", header, re.M)
    return m.group(1).strip().strip('"') if m else ""


def write_sidecars(take: Take, facts: dict, header: str, tape_text: str, seconds: float, cache: str) -> None:
    meta = {
        **facts,
        "tape": take.tape.name,
        "theme": take.look.name,
        "recorded_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "cache": cache,
        "take_seconds": round(seconds, 1),
        "terminal": {
            "font": header_value(header, "FontFamily"),
            "font_size": header_value(header, "FontSize"),
            "pixels": f"{header_value(header, 'Width')}x{header_value(header, 'Height')}",
        },
        "tape_sha256": hashlib.sha256(tape_text.encode()).hexdigest(),
    }
    stems: dict[Path, list[Path]] = {}
    for p in take.outputs:
        if p.exists():
            stems.setdefault(p.with_suffix(""), []).append(p)
    for stem, files in stems.items():
        record = dict(meta, files=[f.name for f in files])
        video = next((f for f in files if f.suffix == ".webm"), None)
        if video:
            record["duration_seconds"] = media_seconds(video)
        stem.with_suffix(".json").write_text(json.dumps(record, indent=2) + "\n")


def record(take: Take, binary: Path, facts: dict, header: str, cache_dir: Path | None, dry_run: bool) -> bool:
    body = (DEMOS_DIR / f"{take.tape.name}.tape").read_text()
    tape_text, outputs = prepare_tape(body, header, take)
    take.outputs = outputs
    env = take_env(dict(os.environ), take, cache_dir)
    if dry_run:
        print(f"\n== {take.label}")
        print(f"   fixture  {take.root}")
        print(f"   datui    {wrapper_script(binary, take.look, take.root / 'tmp').splitlines()[-1]}")
        print(f"   env      HOME={env['HOME']} DATUI_CONFIG_DIR={env['DATUI_CONFIG_DIR']} DATUI_CACHE_DIR={env['DATUI_CACHE_DIR']}")
        dropped = sorted(set(os.environ) - set(env))
        if dropped:
            print(f"   unset    {' '.join(dropped)}")
        for p in outputs:
            print(f"   writes   {p}")
        return True

    build_fixture(take, binary)
    cache = "cold"
    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache = "warm" if any(cache_dir.iterdir()) else "cold"
    prepared = take.root / f"{take.tape.name}.tape"
    prepared.write_text(tape_text)
    for p in outputs:
        p.unlink(missing_ok=True)
    print(f"-- {take.label}: recording", flush=True)
    started = time.monotonic()
    proc = subprocess.run(["vhs", str(prepared)], cwd=take.work, env=env, capture_output=True, text=True)
    seconds = time.monotonic() - started
    if proc.returncode != 0:
        sys.stderr.write(f"!! {take.label}: vhs failed\n{proc.stdout[-2000:]}{proc.stderr[-2000:]}\n")
        return False
    if take.tape.kind == "gif":
        poster = take.out / f"{take.tape.name}.png"
        if last_frame(take.out / f"{take.tape.name}.webm", poster):
            take.outputs.append(poster)
    else:
        review = outputs[0]
        final = review.with_name(review.stem + "-final.png")
        if last_frame(review, final):
            take.outputs.append(final)
    missing = [p for p in take.outputs if not p.exists()]
    write_sidecars(take, facts, header, tape_text, seconds, cache)
    if any(take.work.iterdir()):
        sys.stderr.write(f"!! {take.label}: the working directory is no longer empty\n")
    if missing:
        sys.stderr.write(f"!! {take.label}: missing {', '.join(str(p) for p in missing)}\n")
        return False
    print(f"   done in {seconds:.0f} s: {', '.join(p.name for p in take.outputs)}", flush=True)
    return True


def publish_pairs(names: list[str], out: Path) -> list[tuple[Path, Path]]:
    """(captured, published) for each output of the named tapes."""
    pairs = []
    for name in names:
        tape = TAPES[name]
        if tape.kind == "gif":
            for ext in ("gif", "webm", "png"):
                pairs.append((out / f"{name}.{ext}", PUBLISH_DIR / f"{name}.{ext}"))
        elif tape.kind == "gallery":
            for look in GALLERY:
                pairs.append((out / "themes" / f"{look.name}.png", PUBLISH_DIR / "themes" / f"{look.name}.png"))
            pairs.append((out / "theme-gallery.png", PUBLISH_DIR / "theme-gallery.png"))
        else:
            body = (DEMOS_DIR / f"{name}.tape").read_text()
            for line in body.splitlines():
                m = SCREENSHOT.match(line)
                if m:
                    pairs.append((out / "screenshots" / m.group(1), PUBLISH_DIR / "screenshots" / m.group(1)))
    return pairs


def publish(names: list[str], out: Path, dry_run: bool) -> int:
    pairs = publish_pairs(names, out)
    missing = [src for src, _ in pairs if not src.exists()]
    if missing:
        for src in missing:
            sys.stderr.write(f"missing: {src}\n")
        sys.stderr.write("Record and review these first; nothing was copied.\n")
        return 1
    for src, dst in pairs:
        print(f"{'would copy' if dry_run else 'copy'} {src} -> {dst.relative_to(REPO)}")
        if not dry_run:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
    return 0


def default_binary() -> Path:
    target = Path(os.environ.get("CARGO_TARGET_DIR", REPO / "target"))
    return target / "release" / "datui"


def font_installed(family: str) -> bool:
    families = run(["fc-list", ":", "family"])
    return any(family in line.split(",") for line in families.splitlines())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Record datui's GIFs, screenshots and theme gallery with VHS.")
    p.add_argument("tapes", nargs="*", metavar="TAPE", help="tapes to run, by name (default: all); see --list")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"output directory (default: {DEFAULT_OUT})")
    p.add_argument("--bin", type=Path, default=None, help="the datui binary (default: the release build)")
    p.add_argument("--network-note", default="", help="the connection, for captions, e.g. 'home fiber, 1 Gb/s'")
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="share one datui cache across takes (warm after the first); default: a fresh one per take",
    )
    p.add_argument("--keep-fixtures", action="store_true", help="keep each take's scratch HOME and cache")
    p.add_argument("--publish", action="store_true", help="copy recorded outputs from --out into demos/; records nothing")
    p.add_argument("--dry-run", action="store_true", help="show what would run or be copied")
    p.add_argument("--list", action="store_true", help="list the tapes and exit")
    args = p.parse_args(argv)
    unknown = [t for t in args.tapes if t not in TAPES]
    if unknown:
        p.error(f"unknown tape(s): {', '.join(unknown)}; choose from {', '.join(TAPES)}")
    args.tapes = args.tapes or list(TAPES)
    args.out = args.out.expanduser().resolve()
    if args.bin is not None:
        args.bin = args.bin.expanduser().resolve()
    if args.cache_dir is not None:
        args.cache_dir = args.cache_dir.expanduser().resolve()
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.list:
        for t in TAPES.values():
            print(f"{t.name:22} {t.kind:8} {t.expected:16} {t.used_on}")
        return 0
    if args.publish:
        return publish(args.tapes, args.out, args.dry_run)

    binary = args.bin or default_binary()
    header = HEADER.read_text()
    font = header_value(header, "FontFamily")
    if not args.dry_run:
        problems = []
        if not binary.exists():
            problems.append(f"no datui at {binary}; build it with `cargo build --release` or pass --bin")
        for tool in ("vhs", "ttyd", "ffmpeg", "ffprobe"):
            if shutil.which(tool) is None:
                problems.append(f"{tool} is not on PATH")
        if not font_installed(font):
            problems.append(f"font {font!r} is not installed (fc-list)")
        if problems:
            for msg in problems:
                sys.stderr.write(f"error: {msg}\n")
            return 2
        args.out.mkdir(parents=True, exist_ok=True)

    scratch = args.out / "fixtures"
    takes = plan(args.tapes, args.out, scratch)
    facts = machine_facts(binary, args.network_note or "unrecorded") if not args.dry_run else {}
    failed = []
    for take in takes:
        if not record(take, binary, facts, header, args.cache_dir, args.dry_run):
            failed.append(take.label)
        elif not args.dry_run and not args.keep_fixtures:
            shutil.rmtree(take.root, ignore_errors=True)
    if not args.dry_run and "theme-gallery" in args.tapes:
        gallery = compose_gallery(args.out)
        if gallery is None:
            failed.append("theme-gallery (compose)")
        else:
            print(f"-- gallery: {gallery}")
    if not args.dry_run and not args.keep_fixtures:
        shutil.rmtree(scratch, ignore_errors=True)
    if failed:
        sys.stderr.write(f"\nfailed: {', '.join(failed)}\n")
        return 1
    if not args.dry_run:
        print(f"\nOutputs in {args.out}. Review them, then: scripts/demos/capture.py --publish --out {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
