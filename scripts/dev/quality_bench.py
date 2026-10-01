#!/usr/bin/env python3
"""Measure Data Quality runs before and after a change, on identical fixtures.

Runs `tests/quality_bench_test.rs` (ignored in CI) in this tree and in a
worktree of an earlier commit, one scenario per process so each peak memory
is that scenario's own, and prints a markdown table: wall time, requests and
bytes at the source, peak resident memory and spill, for a first run and for
role and grain edits, sampled and full, and the disk a full scan's local copy
holds.

Both builds use the same fixtures (written once under the bench directory),
the same sample size, seed, scopes and steps, and the same build profile. The
only edit to the copied test is the name of the page Run is pressed on: the
earlier commit calls Setup `Plan`. Requests and bytes exist only where the
in-process S3 stand-in counts them; local reads print `unknown`.

Usage:
    scripts/dev/quality_bench.py BEFORE_REF [--runs N] [--dir DIR] [--scenario NAME]

    BEFORE_REF  the commit to compare against, e.g. the commit before #434
    --runs      repeats per scenario; the table shows the median wall time and
                peak memory, and the request and byte counts of the first run
    --dir       bench directory: fixtures, spill, the before worktree and the
                build output (default: ~/tmp/datui-quality-bench)
"""

from __future__ import annotations

import argparse
import os
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCENARIOS = ["remote_prefix", "local_csv", "local_parquet"]
TEST = "quality_bench_test"
# What each BENCH line measures, in order. Disk is what Data Quality's local copies
# held in the cache directory during the step.
NAMES = ["wall_ms", "requests", "bytes", "peak_rss_kib", "spill_bytes", "disk_bytes"]
# The same profile for both builds: release, without the LTO and single codegen
# unit that make a release build slow to compile and change nothing compared here.
PROFILE_ENV = {
    "CARGO_PROFILE_RELEASE_LTO": "off",
    "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "16",
}


def run(cmd: list[str], cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        sys.stderr.write(result.stdout[-4000:] + result.stderr[-4000:])
        raise SystemExit(f"failed: {' '.join(cmd)} in {cwd}")
    return result.stdout


def prepare_before(ref: str, bench: Path) -> Path:
    tree = bench / "before"
    if not tree.exists():
        run(["git", "worktree", "add", "--detach", str(tree), ref], REPO, dict(os.environ))
    else:
        run(["git", "checkout", "--detach", ref], tree, dict(os.environ))
    for name in ["quality_bench_test.rs", "common/fake_s3.rs"]:
        shutil.copy(REPO / "tests" / name, tree / "tests" / name)
    test = tree / "tests" / "quality_bench_test.rs"
    test.write_text(test.read_text().replace("QualityPage::Setup", "QualityPage::Plan"))
    return tree


def measure(tree: Path, label: str, bench: Path, runs: int, scenarios: list[str]):
    env = dict(os.environ)
    env.update(PROFILE_ENV)
    env["CARGO_TARGET_DIR"] = str(bench / "target" / label)
    env["DATUI_BENCH_DIR"] = str(bench)
    run(["cargo", "test", "--release", "--test", TEST, "--no-run"], tree, env)
    rows: dict[tuple[str, str], list[dict[str, str]]] = {}
    for scenario in scenarios:
        for _ in range(runs):
            out = run(
                [
                    "cargo", "test", "--release", "--test", TEST, "--",
                    "--ignored", "--exact", scenario, "--nocapture",
                ],
                tree,
                env,
            )
            for line in out.splitlines():
                if not line.startswith("BENCH\t"):
                    continue
                _, name, step, *fields = line.split("\t")
                # The measured fields, then the outcome. An older harness has no
                # disk field: its copies, if any, were not measured.
                measured = [f for f in fields if f.split("=", 1)[0] in NAMES]
                values = dict(field.split("=", 1) for field in measured)
                values.setdefault("disk_bytes", "unknown")
                values["outcome"] = "\t".join(fields[len(measured):])
                rows.setdefault((name, step), []).append(values)
    return rows


def median(values: list[str]) -> str:
    numbers = [float(value) for value in values if value != "unknown"]
    if not numbers:
        return "unknown"
    return f"{statistics.median(numbers):.0f}"


def mib(kib: str) -> str:
    return "unknown" if kib == "unknown" else f"{float(kib) / 1024:.0f}"


def summary(rows: list[dict[str, str]], name: str) -> str:
    """The median of a measured figure over the runs; a count from the first."""
    if not rows:
        return "n/a"
    if name == "wall_ms":
        return median([row[name] for row in rows])
    if name == "peak_rss_kib":
        return mib(median([row[name] for row in rows]))
    return rows[0][name]


def table(before, after) -> str:
    lines = [
        "| Scenario | Step | Wall ms | Requests | Bytes | Peak RSS MiB | Spill bytes | Disk bytes |",
        "|---|---|---|---|---|---|---|---|",
    ]
    names = NAMES
    for key in after:
        scenario, step = key
        cells = [
            f"{summary(before.get(key, []), name)} → {summary(after[key], name)}"
            for name in names
        ]
        lines.append(f"| {scenario} | {step} | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("What each run measured (before → after):")
    lines.append("")
    for key in after:
        b, a = before.get(key, []), after[key]
        lines.append(
            f"- {key[0]} / {key[1]}: {b[0]['outcome'] if b else 'n/a'} → {a[0]['outcome']}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("before")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--dir", type=Path, default=Path.home() / "tmp" / "datui-quality-bench")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    args = parser.parse_args()
    bench = args.dir.resolve()
    bench.mkdir(parents=True, exist_ok=True)
    scenarios = args.scenario or SCENARIOS
    before_tree = prepare_before(args.before, bench)
    after = measure(REPO, "after", bench, args.runs, scenarios)
    before = measure(before_tree, "before", bench, args.runs, scenarios)
    print(table(before, after))


if __name__ == "__main__":
    main()
