#!/usr/bin/env python3
"""Time datui's first frame and first rows in a pseudo-terminal; measure idle cost.

Launches a datui binary in a 120x30 PTY with its config, cache and data
directories isolated, and records, per run:

  first_frame   spawn -> the first output that draws a screen (home or loading)
  first_rows    spawn -> the first output holding the fixture's ROWMARK cell
  idle_cpu      CPU (all threads) over an idle window after the rows are drawn
  idle_wakeups  context switches (all threads) over that window, per second
  idle_bytes    terminal output during that window

The terminal answers the keyboard-protocol query (ESC[?u ESC[c) the way a
terminal without the kitty protocol does, immediately unless --reply-delay or
--silent says otherwise; a build that never asks is unaffected by either.

Linux only (pty, /proc). It is a probe, not a CI test: timings depend on the
machine, and the numbers belong in a PR description, not an assertion.

Usage:
    scripts/dev/first_frame_probe.py BINARY [BINARY...] [--runs N] [--case CASE...]
        [--silent] [--reply-delay MS] [--idle SECONDS] [--root DIR]

    BINARY       datui binaries to compare; label one with LABEL=PATH
    --case       parquet, csv, home (default: parquet csv home)
    --runs       repeats per binary and case (default 10)
    --silent     never answer the keyboard-protocol query
    --reply-delay  answer it after this many milliseconds
    --idle       idle window in seconds after the rows (default 2)
    --root       scratch directory (default: a temporary directory)
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import pty
import select
import statistics
import struct
import subprocess
import sys
import tempfile
import termios
import time
from pathlib import Path

QUERY = b"\x1b[?u\x1b[c"
# Primary device attributes only: the answer of a terminal without the kitty protocol.
REPLY = b"\x1b[?1;2c"
# Words on the first screen of each case: the loading screen, the home screen's
# control bar, the screen shown while slow settings are read, or the rows.
FRAME_MARKERS = (b"Scanning", b"Quit", b"settings", b"ROWMARK")
QUIT = b"\x11"  # Ctrl+Q


def write_fixtures(root: Path) -> dict[str, Path]:
    """A 1,000-row, three-column table whose first label cell is ROWMARK."""
    rows = ["id,label,value"] + [
        f"{i},{'ROWMARK' if i == 0 else f'row{i}'},{i * 1.5}" for i in range(1000)
    ]
    csv = root / "small.csv"
    csv.write_text("\n".join(rows) + "\n")
    parquet = root / "small.parquet"
    try:
        import polars as pl

        pl.read_csv(csv).write_parquet(parquet)
    except ImportError:
        venv = Path(__file__).resolve().parents[2] / ".venv/bin/python"
        subprocess.run(
            [str(venv), "-c", f"import polars as pl; pl.read_csv({str(csv)!r}).write_parquet({str(parquet)!r})"],
            check=True,
        )
    return {"csv": csv, "parquet": parquet}


def proc_cost(pid: int) -> tuple[int, int] | None:
    """(CPU clock ticks, context switches) summed over every thread."""
    ticks = switches = 0
    try:
        for task in Path(f"/proc/{pid}/task").iterdir():
            stat = (task / "stat").read_text()
            fields = stat[stat.rindex(")") + 2 :].split()
            ticks += int(fields[11]) + int(fields[12])
            for line in (task / "status").read_text().splitlines():
                if line.startswith(("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches")):
                    switches += int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        return None
    return ticks, switches


def run_once(binary: Path, args: list[str], root: Path, opts, wait_rows: bool) -> dict:
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))
    env = dict(os.environ)
    env.update(
        TERM="xterm-256color",
        XDG_CONFIG_HOME=str(root / "config"),
        XDG_CACHE_HOME=str(root / "cache"),
        XDG_DATA_HOME=str(root / "data"),
        DATUI_CACHE_DIR=str(root / "cache/datui"),
        HOME=str(root / "home"),
    )
    (root / "home").mkdir(exist_ok=True)
    start = time.monotonic()
    proc = subprocess.Popen(
        [str(binary), *args],
        stdin=slave,
        stdout=slave,
        stderr=slave,
        cwd=root,
        env=env,
        start_new_session=True,
    )
    os.close(slave)
    out = b""
    first_frame = first_rows = query_at = None
    replied = False
    settle_at = None
    idle_from = idle_to = None
    idle_bytes = 0
    budget = 10.0
    while proc.poll() is None:
        now = time.monotonic() - start
        if now > budget:
            break
        if query_at is not None and not replied and not opts.silent:
            if now * 1000 >= query_at * 1000 + opts.reply_delay:
                os.write(master, REPLY)
                replied = True
        if select.select([master], [], [], 0.002)[0]:
            try:
                chunk = os.read(master, 262144)
            except OSError:
                break
            now = time.monotonic() - start
            out += chunk
            if query_at is None and QUERY in out:
                query_at = now
            if first_frame is None and any(m in out for m in FRAME_MARKERS):
                first_frame = now
            if first_rows is None and b"ROWMARK" in out:
                first_rows = now
            if idle_from is not None:
                idle_bytes += len(chunk)
        ready = first_rows if wait_rows else first_frame
        if ready is not None and settle_at is None:
            # A second for counts and enrichment to finish before idle is measured.
            settle_at = time.monotonic() + 1.0
        if settle_at is not None and idle_from is None and time.monotonic() >= settle_at:
            idle_from = (time.monotonic(), proc_cost(proc.pid))
        if idle_from is not None and time.monotonic() - idle_from[0] >= opts.idle:
            idle_to = (time.monotonic(), proc_cost(proc.pid))
            break
    os.write(master, QUIT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    os.close(master)
    result = {
        "first_frame_ms": None if first_frame is None else round(first_frame * 1000, 1),
        "first_rows_ms": None if first_rows is None else round(first_rows * 1000, 1),
        "queried": query_at is not None,
        "exit": proc.returncode,
    }
    if idle_from and idle_to and idle_from[1] and idle_to[1]:
        span = idle_to[0] - idle_from[0]
        tick = os.sysconf("SC_CLK_TCK")
        result["idle_cpu_ms"] = round((idle_to[1][0] - idle_from[1][0]) / tick * 1000, 1)
        result["idle_wakeups_per_s"] = round((idle_to[1][1] - idle_from[1][1]) / span, 1)
        result["idle_bytes"] = idle_bytes
    return result


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return round(values[lo] + (values[hi] - values[lo]) * (k - lo), 1)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("binaries", nargs="+")
    p.add_argument("--case", nargs="+", default=["parquet", "csv", "home"])
    p.add_argument("--runs", type=int, default=10)
    p.add_argument("--silent", action="store_true")
    p.add_argument("--reply-delay", type=float, default=0.0)
    p.add_argument("--idle", type=float, default=2.0)
    p.add_argument("--root")
    p.add_argument("--json", action="store_true", help="print every run as JSON")
    opts = p.parse_args()

    root = Path(opts.root) if opts.root else Path(tempfile.mkdtemp(prefix="datui-first-frame-"))
    root.mkdir(parents=True, exist_ok=True)
    fixtures = write_fixtures(root)
    print(
        f"| build | case | runs | first frame p50 / p95 (ms) | first rows p50 / p95 (ms) "
        f"| idle CPU (ms/{opts.idle:g}s) | idle wakeups/s | idle bytes |"
    )
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    builds = []
    for spec in opts.binaries:
        label, _, path = spec.rpartition("=")
        binary = Path(path).resolve()
        builds.append((label or binary.name, binary))
    for case in opts.case:
        args = [] if case == "home" else [str(fixtures[case])]
        # Interleaved, so the builds share whatever else the machine is doing.
        runs = {label: [] for label, _ in builds}
        for _ in range(opts.runs):
            for label, binary in builds:
                runs[label].append(run_once(binary, args, root, opts, wait_rows=case != "home"))
        for label, _ in builds:
            if opts.json:
                for r in runs[label]:
                    print(json.dumps({"build": label, "case": case, **r}), file=sys.stderr)
            done = runs[label]
            frames = [r["first_frame_ms"] for r in done if r["first_frame_ms"] is not None]
            rows = [r["first_rows_ms"] for r in done if r["first_rows_ms"] is not None]
            cpu = [r["idle_cpu_ms"] for r in done if "idle_cpu_ms" in r]
            wakes = [r["idle_wakeups_per_s"] for r in done if "idle_wakeups_per_s" in r]
            idle_bytes = [r["idle_bytes"] for r in done if "idle_bytes" in r]
            rows_cell = "—" if case == "home" else f"{pct(rows, 0.5)} / {pct(rows, 0.95)}"
            print(
                f"| {label} | {case} | {len(done)} | {pct(frames, 0.5)} / {pct(frames, 0.95)} | {rows_cell} "
                f"| {pct(cpu, 0.5)} | {pct(wakes, 0.5)} | {pct(idle_bytes, 0.5)} |",
                flush=True,
            )

if __name__ == "__main__":
    main()
