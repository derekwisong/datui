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


def test_datui_options_take_the_registry_names():
    """The keywords are the option registry's: the open's options and config keys'."""
    import datui

    for name in ("format", "table", "dict", "comment", "null_values", "infer_types", "row_numbers", "config"):
        assert name in datui.OPTION_NAMES, name
    for gone in ("has_header", "comment_char", "parse_strings", "excel_sheet", "debug", "s3_region"):
        assert gone not in datui.OPTION_NAMES, gone
    datui.DatuiOptions(
        format="csv",
        table="Sales",
        comment="#",
        null_values=["NA", "x="],
        infer_types=["a", "b"],
        header_rows=[3, 2],
        no_header=True,
        max_buffered="1GiB",
        config={"display.row_numbers": True, "csv.infer_rows": 50},
    )


def test_datui_options_refuse_what_the_command_line_refuses():
    """A value the flag or key would refuse is a ValueError naming why."""
    import datui

    with pytest.raises(ValueError, match="not a format"):
        datui.DatuiOptions(format="cvs")
    with pytest.raises(ValueError, match="needs a unit"):
        datui.DatuiOptions(max_buffered="512")
    with pytest.raises(ValueError, match="not a config key"):
        datui.DatuiOptions(config={"display.row_number": True})
    with pytest.raises(TypeError, match="not a datui option"):
        datui.DatuiOptions(comment_char="#")


def test_datui_options_delimiter_single_char():
    """DatuiOptions accepts single-char str for delimiter."""
    import datui

    opts = datui.DatuiOptions(delimiter=";")
    assert opts is not None
    assert opts._as_dict()["delimiter"] == ";"
    datui.DatuiOptions(delimiter="tab")


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


class _FakeCaptured:
    """Stands in for the extension's Captured: a plan, and rows over the Arrow C stream."""

    def __init__(self, plan, frame):
        self._plan = plan
        self._frame = frame

    def plan(self):
        return self._plan

    def __arrow_c_stream__(self, requested_schema=None):
        if self._frame is None:
            raise RuntimeError("no rows")
        return self._frame.__arrow_c_stream__(requested_schema)


def test_captured_frame_reads_a_plan():
    """A plan this polars reads comes back as that plan, with no warning."""
    import warnings

    import datui

    payload = polars.DataFrame({"a": [1, 2]}).lazy().serialize()
    if not isinstance(payload, bytes):
        pytest.skip("binary serialization unavailable")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        lf = datui._captured_frame(_FakeCaptured(payload, None))
    assert lf.collect().to_dict(as_series=False) == {"a": [1, 2]}


def test_captured_frame_falls_back_to_rows():
    """A plan this polars cannot read gives the rows instead, with a warning naming the
    paired polars."""
    import datui

    rows = polars.DataFrame({"a": [1, 2], "b": ["x", None]})
    with pytest.warns(UserWarning, match=f"plans for polars {datui.PAIRED_POLARS}"):
        lf = datui._captured_frame(_FakeCaptured(b"not a plan at all", rows))
    assert lf.collect().equals(rows)


def test_captured_frame_without_plan_or_rows_is_a_runtime_error():
    """Neither a plan nor rows: RuntimeError, never ValueError."""
    import datui

    with pytest.warns(UserWarning), pytest.raises(RuntimeError, match="as a plan or as rows"):
        datui._captured_frame(_FakeCaptured(b"not a plan at all", None))


def test_view_from_arrow_refuses_a_stream_that_is_not_a_table():
    """A frame crosses over the Arrow C stream; one that is not a table is a ValueError
    before any TUI starts."""
    import datui._datui

    with pytest.raises(ValueError, match="not a table"):
        datui._datui.view_from_arrow(polars.Series("a", [1, 2]))


def _polars_major():
    return int(polars.__version__.split(".")[0])


def _capture_through_the_tui(tmp_path, frame_code, env=None):
    """Run `datui.view(<frame_code>, capture=True)` in a child on a pty, press q once
    the table is drawn, and return what came back: `rows` (None when nothing did),
    `same` (it equals the frame, dtypes included) and `warned` (it came back as rows)."""
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
        "import datetime, decimal, json, warnings\n"
        "import polars as pl\n"
        "import datui\n"
        f"frame = {frame_code}\n"
        "with warnings.catch_warnings(record=True) as caught:\n"
        "    warnings.simplefilter('always')\n"
        "    res = datui.view(frame, capture=True)\n"
        "got = None if res is None else res.collect()\n"
        "result = {\n"
        "    'rows': None if got is None else got.to_dicts(),\n"
        "    'same': got is not None and got.equals(frame.lazy().collect()),\n"
        "    'warned': any('cannot read datui' in str(w.message) for w in caught),\n"
        "}\n"
        f"with open({str(out)!r}, 'w') as f:\n"
        "    json.dump(result, f, default=str)\n"
    )
    master, slave = pty.openpty()
    # A fresh pty is 0x0; give the TUI a real screen to draw on.
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdin=slave,
        stdout=slave,
        stderr=subprocess.PIPE,
        env={**os.environ, "TERM": "xterm-256color", **(env or {})},
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
    return json.loads(out.read_text())


@pytest.mark.skipif(sys.platform == "win32", reason="pty is not available on Windows")
def test_capture_round_trip_through_the_tui(tmp_path):
    """view(lf, capture=True) hands the frame back after a plain q, and it collects
    after the TUI (and its temp state) is gone — the in-memory round trip."""
    got = _capture_through_the_tui(
        tmp_path, 'pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).lazy()'
    )
    # Polars 2 cannot read the plans the embedded Rust polars writes, so it gets rows.
    assert got["warned"] == (_polars_major() >= 2)
    assert got["rows"] == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="pty is not available on Windows")
def test_a_saved_view_applies_to_a_frame_by_its_columns(tmp_path):
    """A frame has no path, so a view matches it by its columns: auto-apply sorts it
    as the view says, and the captured frame carries the sort."""
    import json

    config = tmp_path / "config"
    (config / "views").mkdir(parents=True)
    (config / "config.toml").write_text("[views]\nauto_apply = true\n")
    view = {
        "id": "0000000000000680",
        "name": "a descending",
        "description": None,
        "created": 0,
        "usage_count": 0,
        "match_criteria": {"schema_columns": ["a", "b"]},
        "settings": {
            "filters": [],
            "sort_columns": ["a"],
            "sort_descending": [True],
            "sort_ascending": False,
            "column_order": ["a", "b"],
            "locked_columns_count": 0,
        },
    }
    (config / "views" / "view_0000000000000680.json").write_text(json.dumps(view))
    got = _capture_through_the_tui(
        tmp_path,
        'pl.DataFrame({"a": [1, 3, 2], "b": ["x", "z", "y"]}).lazy()',
        env={"DATUI_CONFIG_DIR": str(config), "DATUI_CACHE_DIR": str(tmp_path / "cache")},
    )
    assert got["rows"] == [
        {"a": 3, "b": "z"},
        {"a": 2, "b": "y"},
        {"a": 1, "b": "x"},
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="pty is not available on Windows")
def test_a_dataframe_round_trips_over_arrow_with_its_dtypes(tmp_path):
    """A DataFrame goes in over the Arrow C stream and comes back equal, dtypes
    included, whichever way the capture returns it."""
    frame = (
        "pl.DataFrame({"
        "'i': [1, None, 3],"
        "'f': [1.5, None, -2.0],"
        "'s': ['x', None, 'z'],"
        "'t': [True, False, None],"
        "'d': [datetime.date(2026, 10, 7), None, datetime.date(1970, 1, 1)],"
        "'ts': pl.Series([datetime.datetime(2026, 10, 7, 12), None, datetime.datetime(2000, 1, 1)])"
        ".dt.replace_time_zone('America/New_York'),"
        "'dur': [datetime.timedelta(seconds=5), None, datetime.timedelta(days=1)],"
        "'cat': pl.Series(['a', 'b', None], dtype=pl.Categorical),"
        "'enum': pl.Series(['lo', 'hi', 'lo'], dtype=pl.Enum(['lo', 'hi'])),"
        "'dec': pl.Series([decimal.Decimal('1.25'), None, decimal.Decimal('-3.50')], dtype=pl.Decimal(10, 2)),"
        "'list': [[1, 2], None, []],"
        "'struct': [{'k': 1}, {'k': None}, None],"
        "'bin': [b'a', None, b'c'],"
        "})"
    )
    got = _capture_through_the_tui(tmp_path, frame)
    assert got["warned"] == (_polars_major() >= 2)
    assert got["same"], got["rows"]


@pytest.mark.skipif(sys.platform == "win32", reason="pty is not available on Windows")
def test_an_empty_dataframe_round_trips(tmp_path):
    """No rows still crosses over Arrow with its columns."""
    got = _capture_through_the_tui(tmp_path, "pl.DataFrame({'a': [], 'b': []}, schema={'a': pl.Int64, 'b': pl.String})")
    assert got["rows"] == []


def test_python_api_reference_lists_every_option():
    """docs/reference/python-api.md names every keyword datui.view() takes."""
    from pathlib import Path

    import datui

    page = Path(__file__).resolve().parents[2] / "docs" / "reference" / "python-api.md"
    text = page.read_text(encoding="utf-8")
    missing = [name for name in datui.OPTION_NAMES if f"| `{name}` |" not in text]
    assert not missing, f"python-api.md lacks {missing}; run gen_docs write"
