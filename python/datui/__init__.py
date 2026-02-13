"""View Polars data or open files/URLs by path in the terminal."""

from __future__ import annotations

import os
import warnings
from pathlib import Path

import polars as pl

import datui._datui  # noqa: F401  # pyright: ignore[reportMissingImports]

PathLike = str | Path

# Re-export options types so users can do datui.DatuiOptions(...), datui.CompressionFormat.Gzip
DatuiOptions = datui._datui.DatuiOptions
CompressionFormat = datui._datui.CompressionFormat

# Keyword arguments accepted by view(..., **kwargs) and DatuiOptions; unknown kwargs raise TypeError.
_DATUI_OPTIONS_KEYS = frozenset({
    "delimiter",
    "has_header",
    "skip_lines",
    "skip_rows",
    "skip_tail_rows",
    "compression",
    "pages_lookahead",
    "pages_lookback",
    "max_buffered_rows",
    "max_buffered_mb",
    "row_numbers",
    "row_start_index",
    "hive",
    "single_spine_schema",
    "parse_dates",
    "parse_strings",
    "parse_strings_sample_rows",
    "decompress_in_memory",
    "temp_dir",
    "excel_sheet",
    "s3_endpoint_url",
    "s3_access_key_id",
    "s3_secret_access_key",
    "s3_region",
    "polars_streaming",
    "workaround_pivot_date_index",
    "null_values",
    "debug",
})


def _normalize_delimiter(value: int | str) -> int:
    """Convert delimiter to int 0-255 for Rust. Accepts single-char str or int."""
    if isinstance(value, str):
        if len(value) != 1:
            raise ValueError("delimiter as str must be a single character")
        return ord(value)
    return int(value)


def _merge_options(options: DatuiOptions | None, kwargs: dict) -> DatuiOptions | None:
    """Build options from options and/or kwargs. Kwargs override options. Returns None if both empty."""
    if not kwargs and options is None:
        return None
    if options is not None and not kwargs:
        return options
    bad = set(kwargs) - _DATUI_OPTIONS_KEYS
    if bad:
        raise TypeError(f"invalid option(s) for view: {sorted(bad)}; valid: {sorted(_DATUI_OPTIONS_KEYS)}")
    if options is not None:
        base = dict(options._as_dict())
        merged = {**base, **kwargs}
    else:
        merged = dict(kwargs)
    if "delimiter" in merged:
        merged["delimiter"] = _normalize_delimiter(merged["delimiter"])
    return datui._datui.DatuiOptions(**merged)


def _to_path_strings(data: str | Path | list[PathLike] | tuple[PathLike, ...]) -> list[str]:
    """Return a non-empty list of path strings. Raises ValueError if data is an empty sequence."""
    if isinstance(data, (str, Path)):
        return [os.fspath(data)]
    paths = [os.fspath(p) for p in data]
    if not paths:
        raise ValueError("paths must not be empty")
    return paths


def _view_frame(lf: pl.LazyFrame, *, options: DatuiOptions | None) -> None:
    """Serialize LazyFrame plan and launch TUI. Tries binary first, falls back to JSON."""
    payload = lf.serialize()
    if isinstance(payload, bytes):
        try:
            datui._datui.view_from_bytes(payload, options=options)
            return
        except (ValueError, RuntimeError):
            pass
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*json.*deprecated", category=UserWarning)
            try:
                json_payload = lf.serialize(format="json")
            except TypeError:
                raise RuntimeError(
                    "LazyFrame could not be sent to Datui; binary format was rejected and "
                    "this Polars version does not support format='json'."
                ) from None
            if isinstance(json_payload, str):
                datui._datui.view_from_json(json_payload, options=options)
                return
        raise RuntimeError(
            "LazyFrame could not be sent to Datui; Polars version may be incompatible."
        )
    if isinstance(payload, str):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*json.*deprecated", category=UserWarning)
            datui._datui.view_from_json(payload, options=options)
        return
    raise RuntimeError("LazyFrame.serialize() returned an unsupported type")


def view(
    data: pl.LazyFrame | pl.DataFrame | PathLike | list[PathLike] | tuple[PathLike, ...],
    *,
    options: DatuiOptions | None = None,
    **kwargs: object,
) -> None:
    """
    View data in the terminal.

    Accepts path(s), a LazyFrame, or a DataFrame. Paths may be local or remote
    (s3://, gs://, http(s)://). Remote non-Parquet files are downloaded to a temp
    file. With multiple paths, at most one may be remote.

    Options (path-based viewing): delimiter, has_header, skip_lines, skip_rows, skip_tail_rows,
    compression, null_values, parse_strings (default: all CSV string columns; use False to
    disable or a list of column names to limit), parse_strings_sample_rows, hive, debug,
    etc. (see DatuiOptions). For frame-based viewing only display/buffer options apply.
    Pass options as a DatuiOptions instance or as keyword arguments.

    Args:
        data: Path(s), LazyFrame, or DataFrame.
        options: Optional DatuiOptions; use default options when None.
        **kwargs: Optional DatuiOptions fields (override options when both given).

    Raises:
        TypeError: Unsupported type for data or invalid option keyword.
        ValueError: Empty path list or invalid LazyFrame serialization.
        FileNotFoundError: A given path does not exist.
        PermissionError: Read access denied for a path.
        RuntimeError: Error serializing LazyFrame plan or launching the TUI (last resort).
    """
    opts = _merge_options(options, kwargs)
    if isinstance(data, str) or isinstance(data, Path) or isinstance(data, (list, tuple)):
        if not hasattr(datui._datui, "view_paths"):
            _ext = getattr(datui._datui, "__file__", "unknown")
            raise ImportError(
                "datui native extension is outdated or wrong ABI (missing view_paths). "
                f"Extension loaded from: {_ext}. "
                "If you switched Python/ABI: remove that file so the venv install is used, "
                "or run: cd python && maturin develop"
            )
        datui._datui.view_paths(_to_path_strings(data), options=opts)
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
        _view_frame(lf, options=opts)
    except AttributeError as e:
        raise TypeError("data must be a LazyFrame or DataFrame") from e
