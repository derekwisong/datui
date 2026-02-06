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
        datui._datui.view_paths([], debug=False)


def test_run_cli_exists():
    """run_cli should be available (used by the datui console script)."""
    import datui._datui

    assert hasattr(datui._datui, "run_cli")
    assert callable(datui._datui.run_cli)
