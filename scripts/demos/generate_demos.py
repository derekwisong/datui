#!/usr/bin/env python3
"""
Generate demo GIFs from VHS .tape files.

Only processes tape files matching the "{number}-{name}.tape" format (e.g.
01-basic-navigation.tape). Builds the debug binary, ensures it's on PATH, then
generates GIFs using a process pool (parallel by default).

Must be run from repository root for paths to work correctly.

Usage:
    python scripts/demos/generate_demos.py              # Generate all demos
    python scripts/demos/generate_demos.py --number 2  # Generate only demo 2
    python scripts/demos/generate_demos.py -n 4        # Use 4 worker processes
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import NamedTuple

# Pattern for tape filenames: {number}-{name}.tape (e.g. 01-basic-navigation.tape)
TAPE_PATTERN = re.compile(r"^(\d+)-(.+)\.tape$")


class VHSTapeDemo:
    """
    Represents a single VHS tape demo: applies the header template and runs
    gif generation. Can be constructed from a tape number (resolved against
    tapes_dir/demos_dir) or from explicit tape and output paths.
    """

    def __init__(
        self,
        tape_number: int | None = None,
        *,
        tape_path: Path | None = None,
        output_path: Path | None = None,
        tapes_dir: Path | None = None,
        demos_dir: Path | None = None,
    ):
        if tape_number is not None:
            if tapes_dir is None or demos_dir is None:
                raise ValueError("tapes_dir and demos_dir required when using tape_number")
            self._resolve_from_number(tape_number, tapes_dir, demos_dir)
        elif tape_path is not None and output_path is not None:
            self.tape_path = Path(tape_path)
            self.output_path = Path(output_path)
        else:
            raise ValueError("Provide either tape_number (with tapes_dir/demos_dir) or tape_path and output_path")

    def _resolve_from_number(self, tape_number: int, tapes_dir: Path, demos_dir: Path) -> None:
        for f in sorted(tapes_dir.glob("*.tape")):
            m = TAPE_PATTERN.match(f.name)
            if m and int(m.group(1)) == tape_number:
                self.tape_path = f
                self.output_path = demos_dir / (f.stem + ".gif")
                return
        raise FileNotFoundError(f"No tape file found for number {tape_number} in {tapes_dir}")

    def run(self, repo_root: Path, header_file: Path, env: dict) -> tuple[Path, bool, str | None]:
        """
        Apply the header template and run VHS to generate the GIF.
        Returns (output_path, success, error_message).
        """
        try:
            prepared = _prepare_tape_file(self.tape_path, header_file, self.output_path, repo_root)
            temp_path = prepared if prepared != self.tape_path else None
            try:
                subprocess.run(
                    ["vhs", str(prepared)],
                    cwd=repo_root,
                    env=env,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                return (self.output_path, True, None)
            finally:
                if temp_path is not None and temp_path.exists():
                    temp_path.unlink(missing_ok=True)
        except subprocess.CalledProcessError as e:
            return (self.output_path, False, e.stderr or str(e))
        except Exception as e:
            return (self.output_path, False, str(e))


def _prepare_tape_file(
    tape_file: Path, header_file: Path, output_path: Path, repo_root: Path
) -> Path:
    """
    Prepare a tape file by replacing the header section with the standard header.
    Returns path to the prepared tape (temporary file or original).
    """
    with open(tape_file, encoding="utf-8") as f:
        tape_lines = f.readlines()

    marker = "# DO NOT DELETE - HEADER ABOVE"
    marker_line_idx = None
    for i, line in enumerate(tape_lines):
        if marker in line:
            marker_line_idx = i
            break

    if marker_line_idx is None:
        return tape_file

    with open(header_file, encoding="utf-8") as f:
        header_template = f.read().strip()

    # Output path relative to repo root (vhs runs from repo root)
    try:
        output_rel = output_path.relative_to(repo_root)
    except ValueError:
        output_rel = Path("demos") / output_path.name
    output_line = f"Output {output_rel}\n"

    new_lines = [output_line, header_template + "\n"]
    if marker_line_idx + 1 < len(tape_lines):
        new_lines.extend(tape_lines[marker_line_idx + 1 :])
    new_content = "".join(new_lines)

    temp_fd = tempfile.NamedTemporaryFile(mode="w", suffix=".tape", delete=False)
    temp_path = Path(temp_fd.name)
    temp_fd.write(new_content)
    temp_fd.flush()
    temp_fd.close()
    return temp_path


def _run_one_demo(args: NamedTuple) -> tuple[Path, bool, str | None]:
    """Worker entry point for process pool. Must be top-level for pickling."""
    tape_path, output_path, repo_root, header_file, env = args
    demo = VHSTapeDemo(tape_path=Path(tape_path), output_path=Path(output_path))
    return demo.run(Path(repo_root), Path(header_file), env)


class _DemoArgs(NamedTuple):
    tape_path: str
    output_path: str
    repo_root: str
    header_file: str
    env: dict


def _find_matching_tapes(tapes_dir: Path) -> list[tuple[Path, int]]:
    """Return list of (tape_path, tape_number) for files matching {number}-{name}.tape."""
    result = []
    for f in sorted(tapes_dir.glob("*.tape")):
        m = TAPE_PATTERN.match(f.name)
        if m:
            result.append((f, int(m.group(1))))
    return result


def _progress_bar(current: int, total: int, width: int = 30) -> str:
    """Return a simple percentage progress bar string."""
    if total <= 0:
        pct = 100
    else:
        pct = min(100, int(100 * current / total))
    filled = int(width * current / total) if total else width
    bar = "=" * filled + "-" * (width - filled)
    return f"\r  [{bar}] {pct}% ({current}/{total})"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate demo GIFs from VHS .tape files (format: {number}-{name}.tape)."
    )
    parser.add_argument(
        "--number",
        "-N",
        type=int,
        metavar="N",
        help="Generate only the demo with this number (e.g. 2 for 02-querying.tape).",
    )
    parser.add_argument(
        "--workers",
        "-n",
        type=int,
        default=os.cpu_count() or 1,
        metavar="N",
        help="Number of parallel workers (default: all available cores).",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).parent.resolve()
    repo_root = script_dir.parent.parent
    tapes_dir = script_dir
    demos_dir = repo_root / "demos"
    header_file = script_dir / "demo-tape-header.txt"

    if not (repo_root / "Cargo.toml").exists():
        print("Error: Could not find repository root. Please run from repository root.", file=sys.stderr)
        sys.exit(1)

    if not header_file.exists():
        print(f"Error: Header file not found: {header_file}", file=sys.stderr)
        sys.exit(1)

    # Build debug binary
    print("Building debug binary...")
    try:
        subprocess.run(
            ["cargo", "build", "--bin", "datui"],
            cwd=repo_root,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to build binary: {e}", file=sys.stderr)
        sys.exit(1)

    binary_path = repo_root / "target" / "debug" / "datui"
    if not binary_path.exists():
        print("Error: datui binary not found. Please build it first.", file=sys.stderr)
        sys.exit(1)

    binary_dir = str(binary_path.parent)
    env = os.environ.copy()
    env["PATH"] = f"{binary_dir}:{env.get('PATH', '')}"

    demos_dir.mkdir(parents=True, exist_ok=True)

    try:
        subprocess.run(["vhs", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: vhs not found. Please install VHS and ensure it's in your PATH.", file=sys.stderr)
        sys.exit(1)

    # Find matching tape files
    all_tapes = _find_matching_tapes(tapes_dir)
    if args.number is not None:
        all_tapes = [(p, n) for p, n in all_tapes if n == args.number]
        if not all_tapes:
            print(f"Error: No tape found for number {args.number}.", file=sys.stderr)
            sys.exit(1)

    if not all_tapes:
        print(f"Error: No tape files matching '{{number}}-{{name}}.tape' in {tapes_dir}.", file=sys.stderr)
        sys.exit(1)

    # Build list of (tape_path, output_path) for pool
    demo_list = [(tp, demos_dir / (tp.stem + ".gif")) for tp, _ in all_tapes]
    total = len(demo_list)
    workers = min(args.workers, total)

    print(f"\nGenerating {total} demo(s) with {workers} worker(s)...\n")

    worker_args = [
        _DemoArgs(
            tape_path=str(tape_path),
            output_path=str(output_path),
            repo_root=str(repo_root),
            header_file=str(header_file),
            env=env,
        )
        for tape_path, output_path in demo_list
    ]

    completed = 0
    failed = []

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_run_one_demo, a): a for a in worker_args}
        for future in as_completed(futures):
            out_path, success, err = future.result()
            completed += 1
            sys.stdout.write(_progress_bar(completed, total))
            sys.stdout.flush()
            if not success:
                failed.append((out_path.name, err or "unknown error"))

    print()  # newline after progress bar

    if failed:
        print("\n⚠️  Failed:", file=sys.stderr)
        for name, err in failed:
            print(f"  - {name}: {err}", file=sys.stderr)

    print(f"\n✅ Generated {total - len(failed)}/{total} demo(s) in {demos_dir}/")

    if demo_list and not failed:
        print("\nGenerated GIF(s):")
        for _, out_path in demo_list:
            if out_path.exists():
                size_kb = out_path.stat().st_size / 1024
                print(f"  - {out_path.name} ({size_kb:.1f} KB)")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
