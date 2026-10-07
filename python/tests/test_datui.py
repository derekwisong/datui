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


def test_view_refuses_object_columns_before_the_tui():
    """Python objects cannot cross over Arrow: ValueError naming the column, nested ones
    included, before any TUI starts."""
    import datui

    with pytest.raises(ValueError, match="column 'o'.*pl.Object"):
        datui.view(polars.DataFrame({"a": [1], "o": [object()]}, strict=False))
    # Polars will not build a frame with a nested Object today; check the dtypes alone.
    from types import SimpleNamespace

    for dtype in (
        polars.List(polars.Object),
        polars.Array(polars.Object, 2),
        polars.Struct({"x": polars.List(polars.Object)}),
    ):
        with pytest.raises(ValueError, match="column 's'.*pl.Object"):
            datui._refuse_unreadable_columns(SimpleNamespace(schema={"s": dtype}))


def test_view_refuses_float16_naming_the_cast():
    """Float16 has no counterpart in the embedded polars: ValueError naming the column
    and the cast that gets past it."""
    import datui

    if not hasattr(polars, "Float16"):
        pytest.skip("no Float16 in this polars")
    frame = polars.DataFrame({"h": [1.5]}).cast({"h": polars.Float16})
    with pytest.raises(ValueError, match=r"column 'h'.*cast\(pl.Float32\)"):
        datui.view(frame)


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
    """Stands in for the extension's Captured: a plan, and rows over the Arrow C stream.
    A plan or rows that are an exception raise it."""

    def __init__(self, plan, frame):
        self._plan = plan
        self._frame = frame

    def plan(self):
        if isinstance(self._plan, BaseException):
            raise self._plan
        return self._plan

    def __arrow_c_stream__(self, requested_schema=None):
        if isinstance(self._frame, BaseException):
            raise self._frame
        return self._frame.__arrow_c_stream__(requested_schema)


def _pair_with(monkeypatch, paired):
    import datui

    monkeypatch.setattr(datui, "PAIRED_POLARS", paired)


def _this_major():
    return polars.__version__.split(".")[0]


def test_captured_frame_reads_a_plan(monkeypatch):
    """A plan this polars reads comes back as that plan, with no warning."""
    import warnings

    import datui

    _pair_with(monkeypatch, polars.__version__)
    payload = polars.DataFrame({"a": [1, 2]}).lazy().serialize()
    if not isinstance(payload, bytes):
        pytest.skip("binary serialization unavailable")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        lf = datui._captured_frame(_FakeCaptured(payload, AssertionError("rows taken")))
    assert lf.collect().to_dict(as_series=False) == {"a": [1, 2]}


def test_captured_frame_takes_rows_when_this_minor_cannot_read_the_plan(monkeypatch):
    """Another minor release that cannot read the plan gets the rows, with a warning
    carrying polars' own reason."""
    import datui

    _pair_with(monkeypatch, f"{_this_major()}.999")
    rows = polars.DataFrame({"a": [1, 2], "b": ["x", None]})
    reason = rf"written for polars {_this_major()}\.999\): .+; the captured view is its rows"
    with pytest.warns(UserWarning, match=reason):
        lf = datui._captured_frame(_FakeCaptured(b"not a plan at all", rows))
    assert lf.collect().equals(rows)


def test_captured_frame_on_the_paired_release_raises_an_unreadable_plan(monkeypatch):
    """On the paired release an unreadable plan is a bug, not a version gap: raise."""
    import datui

    _pair_with(monkeypatch, polars.__version__)
    rows = polars.DataFrame({"a": [1]})
    with pytest.raises(RuntimeError, match="cannot read the plan datui wrote for it"):
        datui._captured_frame(_FakeCaptured(b"not a plan at all", rows))


def test_captured_frame_skips_the_plan_across_a_major_release(monkeypatch):
    """A major release apart never reads the plan, so it is not even written."""
    import datui

    _pair_with(monkeypatch, "0.1")
    rows = polars.DataFrame({"a": [1, 2]})
    with pytest.warns(UserWarning, match="cannot read the plans datui writes"):
        lf = datui._captured_frame(_FakeCaptured(AssertionError("plan written"), rows))
    assert lf.collect().equals(rows)


def test_captured_frame_takes_rows_when_datui_cannot_write_the_plan(monkeypatch):
    """A view over a source with no plan form (SQLite, text files, follow) still comes
    back, as rows, with a warning saying why."""
    import datui

    _pair_with(monkeypatch, polars.__version__)
    rows = polars.DataFrame({"a": [1, 2]})
    failure = RuntimeError(
        "datui could not serialize the captured view: serialization failed\n\n"
        "error: the enum variant FileScanDsl::Anonymous cannot be serialized"
    )
    with pytest.warns(UserWarning, match="has no plan form .*FileScanDsl::Anonymous"):
        lf = datui._captured_frame(_FakeCaptured(failure, rows))
    assert lf.collect().equals(rows)


def test_captured_frame_without_plan_or_rows_is_a_runtime_error(monkeypatch):
    """Neither a plan nor rows: RuntimeError, never ValueError, a panic included."""
    import datui

    _pair_with(monkeypatch, "0.1")

    class Panic(BaseException):
        pass

    for failure in (RuntimeError("no rows"), Panic("rust panicked")):
        with pytest.warns(UserWarning), pytest.raises(RuntimeError, match="as rows either"):
            datui._captured_frame(_FakeCaptured(None, failure))


def test_view_from_arrow_refuses_a_stream_that_is_not_a_table():
    """A frame crosses over the Arrow C stream; one that is not a table is a ValueError
    before any TUI starts."""
    import datui._datui

    with pytest.raises(ValueError, match="not a table"):
        datui._datui.view_from_arrow(polars.Series("a", [1, 2]))


def _polars_major():
    return int(polars.__version__.split(".")[0])


def _capture_through_the_tui(
    tmp_path, frame_code, env=None, setup="", call="datui.view(frame, capture=True)"
):
    """Run `setup`, `frame = <frame_code>` and `call` in a child on a pty, press q once
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
        f"{setup}\n"
        f"frame = {frame_code}\n"
        "with warnings.catch_warnings(record=True) as caught:\n"
        "    warnings.simplefilter('always')\n"
        f"    res = {call}\n"
        "got = None if res is None else res.collect()\n"
        "result = {\n"
        "    'rows': None if got is None else got.to_dicts(),\n"
        "    'same': got is not None and got.equals(frame.lazy().collect()),\n"
        "    'warned': any('the captured view is its rows' in str(w.message) for w in caught),\n"
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


@pytest.mark.skipif(sys.platform == "win32", reason="pty is not available on Windows")
def test_a_stream_of_several_batches_reads_as_one_frame(tmp_path):
    """Python polars sends one batch; other producers send several, each with its own
    dictionary. They stack into one frame."""
    pytest.importorskip("pyarrow")
    setup = (
        "import pyarrow as pa\n"
        "batches = [pa.record_batch({'n': pa.array([i, i + 1]),"
        " 'c': pa.array(['x', 'y']).dictionary_encode() if i == 0 else"
        " pa.array(['z', 'x']).dictionary_encode()}) for i in (0, 2)]\n"
        "tbl = pa.Table.from_batches(batches)\n"
    )
    got = _capture_through_the_tui(
        tmp_path,
        "pl.from_arrow(tbl)",
        setup=setup,
        call="datui._captured_frame(datui._datui.view_from_arrow(tbl, capture=True))",
    )
    assert got["rows"] == [
        {"n": 0, "c": "x"},
        {"n": 1, "c": "y"},
        {"n": 2, "c": "z"},
        {"n": 3, "c": "x"},
    ]


def test_python_api_reference_lists_every_option():
    """docs/reference/python-api.md names every keyword datui.view() takes."""
    from pathlib import Path

    import datui

    page = Path(__file__).resolve().parents[2] / "docs" / "reference" / "python-api.md"
    text = page.read_text(encoding="utf-8")
    missing = [name for name in datui.OPTION_NAMES if f"| `{name}` |" not in text]
    assert not missing, f"python-api.md lacks {missing}; run gen_docs write"
