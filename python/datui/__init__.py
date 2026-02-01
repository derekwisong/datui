"""
datui: View Polars LazyFrames and DataFrames in the terminal TUI.

Use view(lf) or view(df) to open the datui TUI. Data is passed via
LazyFrame.serialize(format="json") for compatibility across Polars versions.
DataFrames are converted with .lazy() first.
"""

from __future__ import annotations

import polars as pl

from datui._datui import run_cli, view_from_bytes, view_from_json  # noqa: F401


def view(
    data: pl.LazyFrame | pl.DataFrame,
    *,
    debug: bool = False,
    row_numbers: bool = False,
) -> None:
    """
    Open the datui TUI with a Polars LazyFrame or DataFrame.

    DataFrames are converted to a LazyFrame via .lazy(); the LazyFrame
    is serialized as JSON and passed to the Rust extension.

    Args:
        data: A Polars LazyFrame or DataFrame (e.g. pl.scan_csv(...) or pl.DataFrame(...)).
        debug: If True, enable debug overlay (default False).
        row_numbers: If True, show row numbers (default False).

    Raises:
        TypeError: If data is not a LazyFrame or DataFrame.
        RuntimeError: If serialization fails or the TUI fails.
    """
    # Normalize to a LazyFrame only (never serialize a DataFrame).
    if hasattr(data, "lazy") and callable(getattr(data, "lazy", None)):
        lf = data.lazy()
    elif hasattr(data, "serialize") and callable(getattr(data, "serialize", None)):
        lf = data
    else:
        raise TypeError("expected polars.LazyFrame or polars.DataFrame")

    try:
        # Prefer JSON: stable across Polars versions; binary format is not.
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*json.*deprecated", category=UserWarning)
            serialized = lf.serialize(format="json")
    except TypeError:
        # Older Polars: serialize() has no format=, may return JSON string or bytes.
        serialized = lf.serialize()
    except AttributeError as e:
        raise TypeError("expected polars.LazyFrame or polars.DataFrame") from e

    if isinstance(serialized, str):
        return view_from_json(serialized, debug=debug, row_numbers=row_numbers)
    if isinstance(serialized, bytes):
        try:
            return view_from_bytes(serialized, debug=debug, row_numbers=row_numbers)
        except Exception as e:
            raise RuntimeError(
                "Binary LazyFrame format is not compatible with this extension. "
                "Use LazyFrame.serialize(format='json') if your Polars supports it, "
                "or pass the result to view_from_json()."
            ) from e
    raise RuntimeError("LazyFrame.serialize() returned an unsupported type")


__all__ = ["view", "view_from_bytes", "view_from_json", "run_cli"]
