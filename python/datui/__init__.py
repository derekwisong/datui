"""View Polars data or open files/URLs by path in the terminal."""

from __future__ import annotations

import os
import warnings
from pathlib import Path

import polars as pl

import datui._datui  # noqa: F401  # pyright: ignore[reportMissingImports]

PathLike = str | Path


def _to_path_strings(data: str | Path | list[PathLike] | tuple[PathLike, ...]) -> list[str]:
    """Return a non-empty list of path strings. Raises ValueError if data is an empty sequence."""
    if isinstance(data, (str, Path)):
        return [os.fspath(data)]
    paths = [os.fspath(p) for p in data]
    if not paths:
        raise ValueError("paths must not be empty")
    return paths


def _view_frame(lf: pl.LazyFrame, *, debug: bool) -> None:
    """Serialize LazyFrame Plan and launch TUI. Prefers JSON for cross-version stability."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*json.*deprecated", category=UserWarning)
        try:
            payload = lf.serialize(format="json")
        except TypeError:
            payload = lf.serialize()
    if isinstance(payload, str):
        datui._datui.view_from_json(payload, debug=debug)
    elif isinstance(payload, bytes):
        try:
            datui._datui.view_from_bytes(payload, debug=debug)
        except Exception as e:
            raise RuntimeError(
                "LazyFrame could not be sent to Datui; Polars version may be incompatible."
            ) from e
    else:
        raise RuntimeError("LazyFrame.serialize() returned an unsupported type")


def view(
    data: pl.LazyFrame | pl.DataFrame | PathLike | list[PathLike] | tuple[PathLike, ...],
    *,
    debug: bool = False,
) -> None:
    """
    View data in the terminal.

    Accepts path(s), a LazyFrame, or a DataFrame. Paths may be local or remote
    (s3://, gs://, http(s)://). Remote non-Parquet files are downloaded to a temp
    file. With multiple paths, at most one may be remote.

    Raises:
        TypeError: Unsupported type for data.
        ValueError: Empty path list or invalid LazyFrame serialization.
        FileNotFoundError: A given path does not exist.
        PermissionError: Read access denied for a path.
        RuntimeError: Error serializing LazyFrame plan or launching the TUI (last resort).
    """
    if isinstance(data, str) or isinstance(data, Path) or isinstance(data, (list, tuple)):
        datui._datui.view_paths(_to_path_strings(data), debug=debug)
        return

    if hasattr(data, "lazy") and callable(getattr(data, "lazy", None)):
        lf = data.lazy()
    elif hasattr(data, "serialize") and callable(getattr(data, "serialize", None)):
        lf = data
    else:
        raise TypeError(
            "data must be path(s) (str or Path), a URL (s3://, gs://, http(s)://), "
            "or a polars.LazyFrame or polars.DataFrame"
        )

    try:
        _view_frame(lf, debug=debug)
    except AttributeError as e:
        raise TypeError("data must be a LazyFrame or DataFrame") from e
