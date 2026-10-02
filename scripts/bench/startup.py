#!/usr/bin/env python3
"""Time to first rows and peak memory: datui, and VisiData and tabiew where installed.

Each run launches a viewer on a 120x30 pseudo-terminal with its config, cache and
home directories isolated, and records:

  first rows  spawn -> the first drawn frame that shows the file's rows
  peak RSS    the process's resident high-water mark (VmHWM), from spawn until
              SETTLE seconds after the first rows

"First rows" comes from datui's own hook where the build has it:
DATUI_TRACE_FIRST_ROWS=PATH makes datui write the wall-clock time to PATH right
after the first frame with rows is drawn. Other viewers, and datui builds older
than the hook, are timed by watching the terminal output for the first row's key,
which the generated files put in the first cell (FIRSTROW). Remote files have no
known cell, so they need the hook.

Cold runs drop the data file from the page cache first with
posix_fadvise(POSIX_FADV_DONTNEED), which needs no root; `fincore` confirms the
eviction when it is installed. Other platforms run warm only.

Linux only (pty, /proc). Run it with the repository's .venv, which
has Polars for the generated files.

Usage:
    scripts/bench/startup.py table --datui BIN --data DIR [--rows 1M,10M,30M]
        [--runs N] [--remote] [--compare] [--json FILE]
    scripts/bench/startup.py guard --baseline BIN --candidate BIN --data DIR
        [--rows 5M] [--runs N] [--ratio 2.0]

`table` prints the markdown table in docs/reference/performance.md. `guard`
compares two builds on the same generated files, runs interleaved in
alternating order, and exits
non-zero when the candidate's median time to first rows, or its median peak
RSS, is RATIO times the baseline's or more and past an absolute floor, so noise
on a small number cannot fail it.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import platform
import pty
import re
import select
import shutil
import signal
import statistics
import struct
import subprocess
import sys
import tempfile
import termios
import time
from dataclasses import dataclass, field
from pathlib import Path

MARKER = b"FIRSTROW"
SEED = 561
COLS, LINES = 120, 30
# Answers the keyboard-protocol query (ESC[?u ESC[c) as a terminal without the kitty
# protocol does: primary device attributes only.
DEVICE_QUERY = b"\x1b[c"
DEVICE_REPLY = b"\x1b[?1;2c"
# The cursor-position query some TUIs send at startup.
CURSOR_QUERY = b"\x1b[6n"
CURSOR_REPLY = b"\x1b[1;1R"
# datui asks before downloading an HTTP file; Yes has the focus. Matched without
# spaces, which the terminal output draws as cursor moves.
DOWNLOAD_QUESTION = b"Continuewithdownload"
CSI = re.compile(rb"\x1b\[[0-9;?]*[ -/]*[@-~]")

# Public catalog files (crates/datui-lib/src/public_datasets.toml).
REMOTE = {
    "taxi-http": (
        "NYC yellow taxis, Jan 2025 (HTTPS, 59 MB Parquet)",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
    ),
    "noaa-s3": (
        "NOAA GHCN-D, 2023 TMAX (S3, 9 Parquet files)",
        "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2023/ELEMENT=TMAX/",
    ),
}


@dataclass
class Viewer:
    name: str
    argv: list[str]
    quit_keys: bytes
    version: str
    trace: bool = False  # datui with DATUI_TRACE_FIRST_ROWS


@dataclass
class Run:
    first_rows_ms: float | None
    peak_rss_mib: float
    timed_by: str
    error: str = ""


@dataclass
class Case:
    label: str
    path: str
    cache: str  # warm, cold or remote
    runs: dict[str, list[Run]] = field(default_factory=dict)


# ---------------------------------------------------------------- fixtures


def parse_rows(text: str) -> int:
    text = text.strip().upper()
    scale = {"K": 1_000, "M": 1_000_000}.get(text[-1], 1)
    return int(float(text.rstrip("KM")) * scale)


def human_rows(n: int) -> str:
    return f"{n // 1_000_000}M" if n % 1_000_000 == 0 else f"{n:,}"


def generate(data: Path, rows: int) -> dict[str, Path]:
    """A seeded table: a string key whose first value is FIRSTROW, then ints,
    floats, a timestamp and a low-cardinality string. Written once per size, in a
    child process so this one stays small while it times the viewers."""
    out = {"parquet": data / f"bench-{rows}.parquet", "csv": data / f"bench-{rows}.csv"}
    if not all(p.exists() for p in out.values()):
        subprocess.run([sys.executable, __file__, "generate", str(data), str(rows)], check=True)
    return out


def write_files(data: Path, rows: int, out: dict[str, Path]) -> None:
    import numpy as np
    import polars as pl

    data.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)
    idx = pl.int_range(0, rows, eager=True)
    key = pl.select(
        pl.when(pl.int_range(0, rows) == 0)
        .then(pl.lit(MARKER.decode()))
        .otherwise(pl.lit("k") + (pl.int_range(0, rows) % 100_003).cast(pl.String))
        .alias("key")
    )
    df = key.with_columns(
        pl.Series("id", idx),
        pl.Series("amount", rng.normal(100.0, 25.0, rows).round(2)),
        pl.Series("count", rng.integers(0, 10_000, rows, dtype=np.int64)),
        # Seconds from 2020-01-01 over five years.
        pl.from_epoch(pl.Series("at", 1_577_836_800 + rng.integers(0, 5 * 365 * 86400, rows)), time_unit="s"),
        pl.Series("region", np.array(["north", "south", "east", "west"])[rng.integers(0, 4, rows)]),
    )
    for kind, path in out.items():
        tmp = path.with_suffix(path.suffix + ".part")
        if kind == "parquet":
            df.write_parquet(tmp)
        else:
            df.write_csv(tmp)
        with open(tmp, "rb+") as f:
            os.fsync(f.fileno())
        tmp.rename(path)


def evict(path: Path) -> str:
    """Drop a file's pages from the page cache; the residency after, if fincore can say."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)
    if shutil.which("fincore"):
        out = subprocess.run(
            ["fincore", "--bytes", "--noheadings", "--output", "RES", str(path)],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        return out
    return "?"


# ---------------------------------------------------------------- one run


def run_once(viewer: Viewer, path: str, root: Path, settle: float, timeout: float) -> Run:
    home = Path(tempfile.mkdtemp(prefix="run-", dir=root))
    for sub in ("config", "cache", "data", "home", "tmp"):
        (home / sub).mkdir()
    trace_file = home / "first-rows"
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("DATUI_", "XDG_")) and k not in ("COLUMNS", "LINES")
    }
    env.update(
        TERM="xterm-256color",
        XDG_CONFIG_HOME=str(home / "config"),
        XDG_CACHE_HOME=str(home / "cache"),
        XDG_DATA_HOME=str(home / "data"),
        DATUI_CACHE_DIR=str(home / "cache/datui"),
        HOME=str(home / "home"),
        # Downloads land here, not in a /tmp that may be memory.
        TMPDIR=str(home / "tmp"),
    )
    if viewer.trace:
        env["DATUI_TRACE_FIRST_ROWS"] = str(trace_file)
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", LINES, COLS, 0, 0))
    start_ns = time.time_ns()
    start = time.monotonic()
    proc = subprocess.Popen(
        [*viewer.argv, path],
        stdin=slave,
        stdout=slave,
        stderr=slave,
        cwd=home,
        env=env,
        start_new_session=True,
    )
    os.close(slave)
    out = b""
    first = None
    timed_by = "hook" if viewer.trace else "output"
    quit_at = None
    answered = False
    while True:
        now = time.monotonic()
        if quit_at is None and now - start > timeout:
            break
        if quit_at is not None and now >= quit_at:
            break
        if proc.poll() is not None:
            break
        if select.select([master], [], [], 0.002)[0]:
            try:
                chunk = os.read(master, 1 << 18)
            except OSError:
                break
            tail = out[-16:] + chunk
            out = out[-4096:] + chunk
            if DEVICE_QUERY in tail:
                os.write(master, DEVICE_REPLY)
            if CURSOR_QUERY in tail:
                os.write(master, CURSOR_REPLY)
            if not answered and DOWNLOAD_QUESTION in CSI.sub(b"", out).replace(b" ", b""):
                os.write(master, b"\r")
                answered = True
            if first is None and not viewer.trace and MARKER in out:
                first = time.monotonic() - start
        if first is None and viewer.trace and trace_file.exists():
            text = trace_file.read_text().strip()
            if text:
                first = (int(text) - start_ns) / 1e9
        if first is not None and quit_at is None:
            quit_at = time.monotonic() + settle
    error = ""
    if first is None:
        error = "exited" if proc.poll() is not None else "timeout"
    rss_kib = high_water_kib(proc.pid)
    stop(proc, master, viewer.quit_keys)
    os.close(master)
    shutil.rmtree(home, ignore_errors=True)
    return Run(
        first_rows_ms=None if first is None else round(first * 1000, 1),
        peak_rss_mib=round(rss_kib / 1024, 1),
        timed_by=timed_by,
        error=error,
    )


def high_water_kib(pid: int) -> int:
    """Peak resident memory of a live process. Not getrusage: Linux carries the
    parent's high-water mark across fork and exec, so a child of this script would
    report at least the script's own peak."""
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
    except (OSError, ValueError):
        pass
    return 0


def stop(proc: subprocess.Popen, master: int, keys: bytes) -> None:
    """Quit the viewer by its keys, then by signal."""
    try:
        os.write(master, keys)
    except OSError:
        pass
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and proc.poll() is None:
        # Drain the terminal so a viewer blocked on writing its last frame can exit.
        if select.select([master], [], [], 0.05)[0]:
            try:
                os.read(master, 1 << 18)
            except OSError:
                pass
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if proc.poll() is not None:
            return
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass


# ---------------------------------------------------------------- viewers


def version_of(argv: list[str]) -> str:
    try:
        out = subprocess.run([*argv, "--version"], capture_output=True, text=True, timeout=20, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    text = (out.stdout or out.stderr).strip().splitlines()
    return text[0] if text else "unknown"


def datui(binary: str, label: str = "datui") -> Viewer:
    path = str(Path(binary).resolve())
    return Viewer(label, [path], b"\x11", version_of([path]), trace=True)


def others() -> tuple[list[Viewer], list[str]]:
    """VisiData and tabiew, if installed; never installed by this script."""
    found, missing = [], []
    vd = shutil.which("vd") or shutil.which("visidata")
    if vd:
        found.append(Viewer("VisiData", [vd], b"\x11", version_of([vd])))  # Ctrl+Q: quit all
    else:
        missing.append("VisiData: not installed")
    tw = shutil.which("tw") or shutil.which("tabiew")
    if tw:
        found.append(Viewer("tabiew", [tw], b"q", version_of([tw])))
    else:
        missing.append("tabiew: not installed")
    return found, missing


# ---------------------------------------------------------------- reporting


def median(xs: list[float]) -> float | None:
    return round(statistics.median(xs), 1) if xs else None


def summary(runs: list[Run]) -> tuple[float | None, float | None, int]:
    ok = [r for r in runs if r.first_rows_ms is not None]
    return (
        median([r.first_rows_ms for r in ok]),
        median([r.peak_rss_mib for r in ok]),
        len(runs) - len(ok),
    )


def fmt_ms(ms: float | None) -> str:
    if ms is None:
        return "failed"
    return f"{ms / 1000:.2f} s" if ms >= 1000 else f"{ms:.0f} ms"


def fmt_mib(mib: float | None) -> str:
    if mib is None:
        return "—"
    return f"{mib / 1024:.2f} GiB" if mib >= 1024 else f"{mib:.0f} MiB"


def machine() -> str:
    cpu = "unknown CPU"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    mem = ""
    try:
        kib = int(Path("/proc/meminfo").read_text().split()[1])
        mem = f", {kib / 1024 / 1024:.0f} GiB RAM"
    except (OSError, ValueError, IndexError):
        pass
    return f"{cpu}, {os.cpu_count()} threads{mem}; {platform.system()} {platform.release()}"


def table(cases: list[Case], viewers: list[Viewer]) -> str:
    lines = ["| Case | Cache | " + " | ".join(f"{v.name} first rows | {v.name} peak RSS" for v in viewers) + " |"]
    lines.append("|---|---|" + "---:|---:|" * len(viewers))
    for case in cases:
        cells = []
        for v in viewers:
            ms, mib, failed = summary(case.runs.get(v.name, []))
            note = f" ({failed} failed)" if failed and ms is not None else ""
            cells += [fmt_ms(ms) + note, fmt_mib(mib)]
        lines.append(f"| {case.label} | {case.cache} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


# ---------------------------------------------------------------- commands


def cmd_table(opts) -> int:
    root = Path(opts.data).resolve()
    scratch = root / "runs"
    scratch.mkdir(parents=True, exist_ok=True)
    viewers = [datui(opts.datui)]
    missing: list[str] = []
    if opts.compare:
        found, missing = others()
        viewers += found
    cold_ok = hasattr(os, "posix_fadvise")
    caches = ["warm", "cold"] if cold_ok else ["warm"]
    cases: list[Case] = []
    for rows in [parse_rows(r) for r in opts.rows.split(",")]:
        files = generate(root, rows)
        for kind in ("parquet", "csv"):
            size = files[kind].stat().st_size / 1e6
            for cache in caches:
                label = f"{kind.upper() if kind == 'csv' else 'Parquet'}, {human_rows(rows)} rows ({size:,.0f} MB)"
                cases.append(Case(label, str(files[kind]), cache))
    if opts.remote:
        for label, url in REMOTE.values():
            cases.append(Case(label, url, "remote"))
    for case in cases:
        for _ in range(opts.runs if case.cache != "remote" else max(1, min(opts.runs, 3))):
            # Interleaved, so every viewer shares whatever else the machine is doing.
            for v in viewers:
                if case.cache == "remote" and not v.trace:
                    continue
                resident = ""
                if case.cache == "cold":
                    resident = evict(Path(case.path))
                r = run_once(v, case.path, scratch, opts.settle, opts.timeout)
                case.runs.setdefault(v.name, []).append(r)
                print(
                    f"{v.name:9} {case.label:40} {case.cache:6} "
                    f"{fmt_ms(r.first_rows_ms):>9} {fmt_mib(r.peak_rss_mib):>9}"
                    + (f"  resident before: {resident}" if resident else "")
                    + (f"  {r.error}" if r.error else ""),
                    file=sys.stderr,
                    flush=True,
                )
    print(f"Machine: {machine()}\n")
    print("Versions: " + "; ".join([f"{v.name}: {v.version}" for v in viewers] + missing) + "\n")
    print(f"Median of {opts.runs} runs (remote: up to 3); peak RSS through {opts.settle:g} s after the first rows.\n")
    print(table(cases, viewers))
    if opts.json:
        Path(opts.json).write_text(
            json.dumps(
                {
                    "machine": machine(),
                    "viewers": {v.name: v.version for v in viewers},
                    "missing": missing,
                    "cases": [
                        {"label": c.label, "cache": c.cache, "runs": {k: [r.__dict__ for r in v] for k, v in c.runs.items()}}
                        for c in cases
                    ],
                },
                indent=1,
            )
        )
    return 0


def cmd_guard(opts) -> int:
    root = Path(opts.data).resolve()
    scratch = root / "runs"
    scratch.mkdir(parents=True, exist_ok=True)
    files = generate(root, parse_rows(opts.rows))
    base = datui(opts.baseline, "baseline")
    cand = datui(opts.candidate, "candidate")
    # The same clock for both: the terminal output. The baseline may predate the hook.
    base.trace = cand.trace = False
    failures = []
    report = [
        f"Baseline: {base.version}; candidate: {cand.version}; {opts.runs} interleaved runs, warm cache.",
        "",
        "| File | Baseline first rows | Candidate first rows | Baseline peak RSS | Candidate peak RSS |",
        "|---|---:|---:|---:|---:|",
    ]
    for path in files.values():
        runs: dict[str, list[Run]] = {"baseline": [], "candidate": []}
        # Warm the page cache with the file and with each binary.
        for v in (base, cand):
            run_once(v, str(path), scratch, 0, opts.timeout)
        for i in range(opts.runs):
            # Alternating which goes first, so neither always follows the other.
            for v in (base, cand) if i % 2 == 0 else (cand, base):
                runs[v.name].append(run_once(v, str(path), scratch, opts.settle, opts.timeout))
        (b_ms, b_mib, _), (c_ms, c_mib, c_fail) = summary(runs["baseline"]), summary(runs["candidate"])
        report.append(f"| {path.name} | {fmt_ms(b_ms)} | {fmt_ms(c_ms)} | {fmt_mib(b_mib)} | {fmt_mib(c_mib)} |")
        if c_ms is None or c_fail > opts.runs // 2:
            failures.append(f"{path.name}: the candidate did not show rows in {c_fail} of {opts.runs} runs")
            continue
        if b_ms is None:
            print(f"::warning::{path.name}: the baseline never showed rows; nothing to compare", file=sys.stderr)
            continue
        if c_ms >= opts.ratio * b_ms and c_ms - b_ms >= opts.floor_ms:
            failures.append(f"{path.name}: first rows {fmt_ms(c_ms)} vs {fmt_ms(b_ms)}, {c_ms / b_ms:.1f}x")
        if c_mib >= opts.ratio * b_mib and c_mib - b_mib >= opts.floor_mib:
            failures.append(f"{path.name}: peak RSS {fmt_mib(c_mib)} vs {fmt_mib(b_mib)}, {c_mib / b_mib:.1f}x")
    # The hook itself: the candidate must report its first rows.
    hooked = datui(opts.candidate, "candidate")
    r = run_once(hooked, str(files["parquet"]), scratch, 0, opts.timeout)
    report.append("")
    report.append(f"DATUI_TRACE_FIRST_ROWS on the candidate: {fmt_ms(r.first_rows_ms)}")
    if r.first_rows_ms is None:
        failures.append("the candidate never wrote DATUI_TRACE_FIRST_ROWS")
    text = "\n".join(report)
    print(text)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a") as f:
            f.write("## Startup guard\n\n" + text + "\n")
    for failure in failures:
        print(f"::error::{failure}", file=sys.stderr)
    return 1 if failures else 0


def main() -> int:
    if not sys.platform.startswith("linux"):
        raise SystemExit("startup.py needs Linux (pty, /proc)")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("table", help="the published table")
    t.add_argument("--datui", required=True, help="a release build of datui")
    t.add_argument("--data", required=True, help="directory for generated files and run scratch")
    t.add_argument("--rows", default="1M,10M,30M", help="comma-separated sizes (default 1M,10M,30M)")
    t.add_argument("--runs", type=int, default=5, help="runs per viewer and case (default 5)")
    t.add_argument("--remote", action="store_true", help="add the public-catalog remote files")
    t.add_argument("--compare", action="store_true", help="add VisiData and tabiew if installed")
    t.add_argument("--settle", type=float, default=2.0, help="seconds kept open after the first rows (default 2)")
    t.add_argument("--timeout", type=float, default=120.0, help="seconds to wait for rows (default 120)")
    t.add_argument("--json", help="also write every run to this file")
    g = sub.add_parser("guard", help="fail on a large regression against a baseline build")
    g.add_argument("--baseline", required=True)
    g.add_argument("--candidate", required=True)
    g.add_argument("--data", required=True)
    g.add_argument("--rows", default="5M", help="generated file size (default 5M)")
    g.add_argument("--runs", type=int, default=7, help="interleaved runs per build and file (default 7)")
    g.add_argument("--ratio", type=float, default=2.0, help="fail at this multiple of the baseline (default 2)")
    g.add_argument("--floor-ms", type=float, default=100.0, help="and at least this much slower (default 100)")
    g.add_argument("--floor-mib", type=float, default=128.0, help="and at least this much larger (default 128)")
    # Long enough for the CSV row count to finish in both builds: a faster build that
    # finished it inside the window while a slower one had not would read as bigger.
    g.add_argument("--settle", type=float, default=3.0, help="seconds kept open after the first rows (default 3)")
    g.add_argument("--timeout", type=float, default=60.0)
    gen = sub.add_parser("generate", help=argparse.SUPPRESS)
    gen.add_argument("data")
    gen.add_argument("rows", type=int)
    opts = p.parse_args()
    if opts.cmd == "generate":
        data = Path(opts.data)
        write_files(data, opts.rows, {"parquet": data / f"bench-{opts.rows}.parquet", "csv": data / f"bench-{opts.rows}.csv"})
        return 0
    return cmd_table(opts) if opts.cmd == "table" else cmd_guard(opts)


if __name__ == "__main__":
    sys.exit(main())
