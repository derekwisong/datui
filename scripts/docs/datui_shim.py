#!/usr/bin/env python3
"""`datui`, run headless for the doc-example runner (scripts/docs/doc_examples.py).

The runner puts a directory holding this script, as `datui`, first on PATH and sets
DATUI_DOC_BIN to the real binary. A subcommand, --help or --version runs the binary
as it is. Anything else runs it on a pseudo-terminal, as a person would, and this
script stands in for the person:

- When the table shows rows (datui writes DATUI_TRACE_FIRST_ROWS), it presses
  Ctrl+Q and exits with datui's status: the example opened what it named.
- When a question asks first (a download, a large read), it answers Enter.
- With no path and nothing piped in, datui opens the home screen: once the screen
  has gone quiet, it quits and that is a success.
- When nothing shows rows within DATUI_DOC_TIMEOUT seconds (default 60), or datui
  exits on its own without showing any, it fails, printing the screen's last text.

DATUI_DOC_EXPECT overrides what counts as success: `rows`, `screen` (anything drawn
that stays up, such as the hex view or the home screen) or `exit` (run to its end).

`datui_shim.py --command PROGRAM ARGS...` runs another program the same way: a
Python script that calls datui.view(), which must show rows.

Linux and macOS only (pty).
"""

from __future__ import annotations

import fcntl
import os
import pty
import re
import select
import signal
import stat
import struct
import subprocess
import sys
import tempfile
import termios
import time

SUBCOMMANDS = {"formats", "config", "cache", "views", "completions", "man", "help"}
CTRL_Q = b"\x11"


def passthrough(argv: list[str]) -> bool:
    """A subcommand, --help or --version: no terminal needed."""
    if any(a in ("-h", "--help", "-V", "--version") for a in argv):
        return True
    for a in argv:
        if a == "--":
            return False
        if not a.startswith("-"):
            return a in SUBCOMMANDS
    return False


def stdin_is_data() -> bool:
    try:
        mode = os.fstat(0).st_mode
    except OSError:
        return False
    return stat.S_ISFIFO(mode) or stat.S_ISREG(mode) or stat.S_ISSOCK(mode)


def has_path(argv: list[str]) -> bool:
    """Whether the command names something to open (a path, URL or `-`)."""
    takes_value = {
        "-F", "--format", "-t", "--table", "--compression", "--dict", "--tee", "--hex-width",
        "--view", "--temp-dir", "--delimiter", "--header-rows", "--footer-rows",
        "--skip-rows", "--skip-lines", "--comment", "--null", "--infer-rows",
        "--number-format", "--sample-rows", "-c", "--config", "--log-file", "--log-level",
    }
    skip = False
    for a in argv:
        if skip:
            skip = False
            continue
        if a in takes_value:
            skip = True
            continue
        if a.startswith("-") and a != "-":
            continue
        return True
    return False


def screen_text(raw: bytes) -> str:
    """The printable text of what was drawn, for a failure message."""
    text = re.sub(rb"\x1b\[[0-9;?]*[ -/]*[@-~]", b" ", raw)
    text = re.sub(rb"\x1b[\]P^_].*?(\x07|\x1b\\)", b" ", text)
    text = re.sub(rb"[\x00-\x08\x0b-\x1f\x7f]", b" ", text)
    words = text.decode("utf-8", "replace").split()
    return " ".join(words)[-1500:]


def main() -> int:
    real = os.environ.get("DATUI_DOC_BIN", "")
    if not real and sys.argv[1:2] != ["--command"]:
        print("datui_shim: DATUI_DOC_BIN is not set", file=sys.stderr)
        return 2
    argv = sys.argv[1:]
    if argv[:1] == ["--command"]:
        # Another program that opens datui, such as a Python script calling
        # datui.view(): run it on the terminal, and expect rows.
        command = argv[1:]
        piped = False
        expect = os.environ.get("DATUI_DOC_EXPECT") or "rows"
    else:
        if passthrough(argv):
            os.execv(real, [real, *argv])
        command = [real, *argv]
        piped = stdin_is_data()
        # The hex view draws bytes, not a table's rows.
        expect = os.environ.get("DATUI_DOC_EXPECT") or (
            "rows" if (has_path(argv) or piped) and "--hex" not in argv else "screen"
        )
    timeout = float(os.environ.get("DATUI_DOC_TIMEOUT", "60"))
    tee_stdout = "--tee" in argv and argv[argv.index("--tee") + 1 : argv.index("--tee") + 2] == ["-"]

    trace = tempfile.NamedTemporaryFile(prefix="datui-doc-rows-", delete=False)
    trace.close()
    os.unlink(trace.name)
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))

    def own_terminal() -> None:
        os.setsid()
        fcntl.ioctl(slave, termios.TIOCSCTTY, 0)

    env = {**os.environ, "DATUI_TRACE_FIRST_ROWS": trace.name, "TERM": "xterm-256color"}
    proc = subprocess.Popen(
        command,
        stdin=0 if piped else slave,
        stdout=1 if tee_stdout else slave,
        stderr=slave,
        env=env,
        preexec_fn=own_terminal,
        pass_fds=(slave,),
    )
    os.close(slave)

    drawn = bytearray()
    start = last_output = time.monotonic()
    answered = 0.0
    quit_sent = 0.0
    shown = False
    try:
        while proc.poll() is None:
            now = time.monotonic()
            readable, _, _ = select.select([master], [], [], 0.1)
            if readable:
                try:
                    chunk = os.read(master, 65536)
                except OSError:
                    chunk = b""
                if not chunk:
                    break
                drawn += chunk
                del drawn[:-200_000]
                last_output = now
            if not shown and os.path.exists(trace.name):
                shown = True
            quiet = now - last_output > 1.0
            # A screen that stays up for three seconds opened; the home screen's
            # spinner may never let the output go quiet.
            done = shown or (expect == "screen" and len(drawn) > 500 and now - start > 3.0)
            if done and now - quit_sent > 2.0 and expect != "exit":
                os.write(master, CTRL_Q)
                quit_sent = now
                continue
            # A question waits on an answer: a download, a large read.
            if (
                expect == "rows"
                and not shown
                and quiet
                and len(drawn) > 500
                and now - answered > 3.0
                and re.search(rb"Download|download|into memory|Continue", bytes(drawn[-20000:]))
            ):
                os.write(master, b"\r")
                answered = now
            if now - start > timeout:
                print(
                    f"datui_shim: no {'rows' if expect == 'rows' else 'end'} within {timeout:.0f}s: "
                    f"{screen_text(bytes(drawn))}",
                    file=sys.stderr,
                )
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                return 1
        status = proc.wait(timeout=30)
    finally:
        os.close(master)
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        if os.path.exists(trace.name):
            shown = True
            os.unlink(trace.name)
    if status != 0:
        print(f"datui_shim: datui exited {status}: {screen_text(bytes(drawn))}", file=sys.stderr)
        return status
    if expect == "rows" and not shown:
        print(f"datui_shim: datui showed no rows: {screen_text(bytes(drawn))}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
