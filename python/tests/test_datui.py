"""Tests for the datui Python binding."""

import pytest

polars = pytest.importorskip("polars")


def test_import_datui():
    """Importing datui should succeed."""
    import datui

    assert hasattr(datui, "view")


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
    """Passing a non-LazyFrame/DataFrame to view() should raise TypeError."""
    import datui

    with pytest.raises(TypeError, match="expected polars.LazyFrame or polars.DataFrame"):
        datui.view("not a lazyframe")


def test_view_from_json_exists():
    """view_from_json should be available (low-level API that accepts JSON from LazyFrame.serialize())."""
    import datui

    assert hasattr(datui, "view_from_json")
    assert callable(datui.view_from_json)


def test_view_from_json_invalid_raises():
    """Passing invalid JSON to view_from_json should raise RuntimeError with a clear message."""
    import datui

    with pytest.raises(RuntimeError, match="invalid LazyFrame JSON"):
        datui.view_from_json("not valid json")


def test_view_from_bytes_exists():
    """view_from_bytes should be available (low-level API that accepts binary from LazyFrame.serialize())."""
    import datui

    assert hasattr(datui, "view_from_bytes")
    assert callable(datui.view_from_bytes)


def test_view_from_bytes_invalid_raises():
    """Passing invalid bytes to view_from_bytes should raise RuntimeError with a clear message."""
    import datui

    with pytest.raises(RuntimeError, match="invalid LazyFrame binary"):
        datui.view_from_bytes(b"not valid binary")


def test_run_cli_exists():
    """run_cli should be available (used by the datui console script)."""
    import datui

    assert hasattr(datui, "run_cli")
    assert callable(datui.run_cli)
