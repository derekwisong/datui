"""
View Polars LazyFrames and DataFrames in the terminal.

Use view(lf) or view(df) to open Datui.
"""

from __future__ import annotations

import polars as pl

import datui._datui  # noqa: F401  # pyright: ignore[reportMissingImports]


def view(
    data: pl.LazyFrame | pl.DataFrame,
    *,
    debug: bool = False,
) -> None:
    """
    View a Polars LazyFrame or DataFrame in the terminal.

    Args:
        data: A Polars LazyFrame or DataFrame (e.g. pl.scan_csv(...) or pl.DataFrame(...)).
        debug: If True, enable debug overlay (default False).

    Raises:
        TypeError: If data is not a LazyFrame or DataFrame.
        RuntimeError: The application crashed.
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
        return datui._datui.view_from_json(serialized, debug=debug)
    if isinstance(serialized, bytes):
        try:
            return datui._datui.view_from_bytes(serialized, debug=debug)
        except Exception as e:
            raise RuntimeError(
                "Unable to communicate serialized LazyFrame plan to Datui. "
                "Possible incompatibility between the polars version and the Datui extension."
            ) from e
    raise RuntimeError("LazyFrame.serialize() returned an unsupported type")


__all__ = ["view"]
