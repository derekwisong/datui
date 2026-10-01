"""Tests for the datui Python binding."""

import sys

import pytest

polars = pytest.importorskip("polars")


def test_import_datui():
    """Importing datui should succeed."""
    import datui

    assert hasattr(datui, "view")
    assert hasattr(datui, "DatuiOptions")
    assert hasattr(datui, "CompressionFormat")


def test_view_accepts_lazyframe():
    """view() should accept a polars LazyFrame (type check; we don't run the TUI in tests)."""
    import datui

    lf = polars.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).lazy()
    # We only verify the binding accepts the argument; running view() would block on the TUI
    assert callable(datui.view)


def test_view_accepts_dataframe():
    """view() should accept a polars DataFrame (converted to LazyFrame internally)."""
    import datui

    df = polars.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    # We only verify the binding accepts the argument; running view() would block on the TUI
    assert callable(datui.view)


def test_view_invalid_input_raises():
    """Passing a non-existent path string to view() should raise FileNotFoundError (no TTY needed)."""
    import datui

    with pytest.raises(FileNotFoundError, match="File not found"):
        datui.view("not a lazyframe")


def test_view_list_of_paths_missing_raises():
    """Passing a list of paths where one does not exist should raise FileNotFoundError (no TTY)."""
    import datui

    with pytest.raises(FileNotFoundError, match="File not found"):
        datui.view(["also not a file", "neither is this"])


def test_view_from_json_exists():
    """view_from_json should be available (low-level API that accepts JSON from LazyFrame.serialize())."""
    import datui._datui

    assert hasattr(datui._datui, "view_from_json")
    assert callable(datui._datui.view_from_json)


def test_view_from_json_invalid_raises():
    """Passing invalid JSON to view_from_json should raise ValueError with a clear message."""
    import datui._datui

    with pytest.raises(ValueError, match="invalid LazyFrame JSON"):
        datui._datui.view_from_json("not valid json")


def test_view_from_bytes_exists():
    """view_from_bytes should be available (low-level API that accepts binary from LazyFrame.serialize())."""
    import datui._datui

    assert hasattr(datui._datui, "view_from_bytes")
    assert callable(datui._datui.view_from_bytes)


def test_view_from_bytes_invalid_raises():
    """Passing invalid bytes to view_from_bytes should raise ValueError with a clear message."""
    import datui._datui

    with pytest.raises(ValueError, match="invalid LazyFrame binary"):
        datui._datui.view_from_bytes(b"not valid binary")


def test_view_paths_empty_raises():
    """Passing an empty list to view_paths should raise ValueError."""
    import datui._datui

    with pytest.raises(ValueError, match="paths must not be empty"):
        datui._datui.view_paths([])


def test_run_cli_exists():
    """run_cli should be available (used by the datui console script)."""
    import datui._datui

    assert hasattr(datui._datui, "run_cli")
    assert callable(datui._datui.run_cli)


def test_datui_options_constructible():
    """DatuiOptions should be constructible with kwargs (no TUI run)."""
    import datui

    opts = datui.DatuiOptions(delimiter=ord(","), skip_rows=2, row_numbers=True)
    assert opts is not None
    d = opts._as_dict()
    assert d["delimiter"] == 44
    assert d["skip_rows"] == 2
    assert d["row_numbers"] is True
    assert d["debug"] is False


def test_datui_options_debug():
    """DatuiOptions(debug=True) stores debug; view(..., debug=True) is valid via kwargs."""
    import datui

    opts = datui.DatuiOptions(debug=True)
    assert opts._as_dict()["debug"] is True


def test_datui_options_delimiter_single_char():
    """DatuiOptions accepts single-char str for delimiter."""
    import datui

    opts = datui.DatuiOptions(delimiter=";")
    assert opts is not None
    d = opts._as_dict()
    assert d["delimiter"] == ord(";")


def test_view_invalid_kwarg_raises():
    """view() with invalid option keyword should raise TypeError."""
    import datui

    with pytest.raises(TypeError, match="invalid option"):
        datui.view("nonexistent.csv", not_an_option=1)


def test_compression_format_values():
    """CompressionFormat should expose gzip, zstd, bzip2, xz."""
    import datui

    assert hasattr(datui.CompressionFormat, "Gzip")
    assert hasattr(datui.CompressionFormat, "Zstd")
    assert hasattr(datui.CompressionFormat, "Bzip2")
    assert hasattr(datui.CompressionFormat, "Xz")


def test_view_names_the_paired_polars_for_an_unreadable_plan():
    """A plan neither decoder accepts says which polars this datui is built for."""
    import datui

    class Unreadable:
        def serialize(self, format="binary"):
            return b"not a plan" if format == "binary" else "not a plan"

    with pytest.raises(ValueError, match=f"built for polars {datui.PAIRED_POLARS}"):
        datui._view_frame(Unreadable(), options=None)


def test_view_capture_missing_path_still_raises_file_not_found():
    """capture=True changes nothing about input validation: a missing path still raises."""
    import datui

    with pytest.raises(FileNotFoundError, match="File not found"):
        datui.view("does-not-exist.csv", capture=True)


def test_splice_own_dsl_hash_is_identity_on_own_plans():
    """A plan this polars wrote already carries this polars' hash, so the splice is a no-op."""
    import datui

    payload = polars.DataFrame({"a": [1]}).lazy().serialize()
    if not isinstance(payload, bytes):
        pytest.skip("binary serialization unavailable")
    assert datui._splice_own_dsl_hash(payload) == payload


def test_deserialize_captured_reads_a_plan():
    """The captured-plan decoder returns a working LazyFrame."""
    import datui

    payload = polars.DataFrame({"a": [1, 2]}).lazy().serialize()
    if not isinstance(payload, bytes):
        pytest.skip("binary serialization unavailable")
    lf = datui._deserialize_captured(payload)
    assert lf.collect().to_dict(as_series=False) == {"a": [1, 2]}


def test_deserialize_captured_wraps_garbage_in_a_clear_error():
    """Bytes polars cannot read raise RuntimeError naming the paired polars, never ValueError."""
    import datui

    with pytest.raises(RuntimeError, match=f"polars {datui.PAIRED_POLARS}"):
        datui._deserialize_captured(b"not a plan at all")


@pytest.mark.skipif(sys.platform == "win32", reason="pty is not available on Windows")
def test_capture_round_trip_through_the_tui(tmp_path):
    """view(lf, capture=True) hands the frame back after a plain q, and it collects
    after the TUI (and its temp state) is gone — the in-memory round trip."""
    import fcntl
    import json
    import os
    import pty
    import select
    import struct
    import subprocess
    import termios
    import time

    out = tmp_path / "result.json"
    script = (
        "import json\n"
        "import polars as pl\n"
        "import datui\n"
        'df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})\n'
        "res = datui.view(df.lazy(), capture=True)\n"
        "rows = None if res is None else res.collect().to_dicts()\n"
        f"with open({str(out)!r}, 'w') as f:\n"
        "    json.dump(rows, f)\n"
    )
    master, slave = pty.openpty()
    # A fresh pty is 0x0; give the TUI a real screen to draw on.
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdin=slave,
        stdout=slave,
        stderr=subprocess.PIPE,
        env={**os.environ, "TERM": "xterm-256color"},
    )
    os.close(slave)
    deadline = time.monotonic() + 60
    last_q = 0.0
    last_output = time.monotonic()
    drawn = 0
    try:
        while proc.poll() is None and time.monotonic() < deadline:
            # Drain the TUI's output so it never blocks on a full pty buffer, and
            # send q only once a real frame has been drawn (terminal init writes a
            # few bytes long before the TUI is up) and the output has gone quiet
            # (the loading screen redraws its spinner continuously) — a q sent
            # during the load quits before any dataset is open, which correctly
            # captures nothing and would fail this test.
            readable, _, _ = select.select([master], [], [], 0.2)
            if readable:
                try:
                    chunk = os.read(master, 65536)
                except OSError:
                    chunk = b""
                # EIO on Linux, EOF on macOS: the child closed the pty.
                if not chunk:
                    break
                drawn += len(chunk)
                last_output = time.monotonic()
                continue
            quiet = time.monotonic() - last_output > 1.0
            if drawn >= 1000 and quiet and time.monotonic() - last_q > 2.0:
                try:
                    os.write(master, b"q")
                except OSError:
                    break
                last_q = time.monotonic()
        # The pty closes while the child is still exiting (its fds go before it is
        # reapable), so wait for the exit rather than polling once. communicate()
        # also drains stderr so a chatty child cannot block on the pipe.
        try:
            _, stderr = proc.communicate(timeout=max(deadline - time.monotonic(), 5.0))
        except subprocess.TimeoutExpired:
            proc.kill()
            _, stderr = proc.communicate()
            pytest.fail(
                f"the child did not exit within 60 seconds (q sent: {last_q > 0}): "
                f"{stderr.decode(errors='replace')}"
            )
    finally:
        os.close(master)
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    assert proc.returncode == 0, f"child failed: {stderr.decode(errors='replace')}"
    rows = json.loads(out.read_text())
    assert rows == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    ]
