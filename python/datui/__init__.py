"""View Polars data or open files/URLs by path in the terminal."""

from __future__ import annotations

import io
import os
import warnings
from pathlib import Path

import polars as pl

import datui._datui  # noqa: F401  # pyright: ignore[reportMissingImports]

PathLike = str | Path

# Re-export options types so users can do datui.DatuiOptions(...), datui.CompressionFormat.Gzip
DatuiOptions = datui._datui.DatuiOptions
CompressionFormat = datui._datui.CompressionFormat

# Keyword arguments view(..., **kwargs) and DatuiOptions take, from datui's option
# registry: the open's own options (format, table, delimiter, ...), the config keys'
# keywords (comment, row_numbers, infer_types, ...) and config, a dict of any config
# key to its value. Unknown keywords raise TypeError.
OPTION_NAMES = tuple(datui._datui.OPTION_NAMES)
_DATUI_OPTIONS_KEYS = frozenset(OPTION_NAMES)


def _merge_options(options: DatuiOptions | None, kwargs: dict) -> DatuiOptions | None:
    """Build options from options and/or kwargs. Kwargs override options. Returns None if both empty."""
    if not kwargs and options is None:
        return None
    if options is not None and not kwargs:
        return options
    bad = set(kwargs) - _DATUI_OPTIONS_KEYS
    if bad:
        raise TypeError(f"invalid option(s) for view: {sorted(bad)}; valid: {sorted(_DATUI_OPTIONS_KEYS)}")
    merged = dict(options._as_dict()) if options is not None else {}
    merged.update(kwargs)
    return datui._datui.DatuiOptions(**merged)


def _to_path_strings(data: str | Path | list[PathLike] | tuple[PathLike, ...]) -> list[str]:
    """Return a non-empty list of path strings. Raises ValueError if data is an empty sequence."""
    if isinstance(data, (str, Path)):
        return [os.fspath(data)]
    paths = [os.fspath(p) for p in data]
    if not paths:
        raise ValueError("paths must not be empty")
    return paths


# The Python polars release paired with the Rust polars this wheel embeds. Plans it writes
# are the ones the wheel is tested against; other releases may write or read plans that
# differ (2.0 cannot read 0.55's). Move it with the Rust polars bump.
PAIRED_POLARS = "1.43"

# Where the DSL schema hash sits in a versioned plan: after the DSL_VERSION magic bytes
# and the u16 major and minor version. Mirrors the same constants in the Rust binding.
_DSL_HASH_OFFSET = len(b"DSL_VERSION") + 4
_DSL_HASH_LEN = 64


def _splice_own_dsl_hash(payload: bytes) -> bytes:
    """Replace a captured plan's DSL schema hash with this polars' own.

    The hash is the digest of a file in the polars repository at the commit each
    release was cut from, so a plan from the wheel's embedded Rust polars never
    matches even within one release train. The Rust binding does the same splice on
    the way in; this is the way out. The plan body stays MessagePack with field
    names, so a genuinely incompatible plan still fails on a missing field.
    """
    own = pl.DataFrame().lazy().serialize()
    end = _DSL_HASH_OFFSET + _DSL_HASH_LEN
    if not isinstance(own, bytes) or len(own) < end or len(payload) < end:
        return payload
    return payload[:_DSL_HASH_OFFSET] + own[_DSL_HASH_OFFSET:end] + payload[end:]


def _captured_frame(captured) -> pl.LazyFrame:
    """Turn the view the TUI captured into a LazyFrame.

    `captured` has `plan()`, the view's plan bytes, and `__arrow_c_stream__`, its rows.
    The plan is read when this polars can; a polars whose plans differ from the
    paired release's (2.0 does) gets the rows instead, collected now, with a warning.
    """
    payload = captured.plan()
    try:
        return pl.LazyFrame.deserialize(io.BytesIO(_splice_own_dsl_hash(payload)))
    except Exception as e:
        unreadable = e
    version = getattr(pl, "__version__", "unknown")
    warnings.warn(
        f"polars {version} cannot read datui's plan (datui writes plans for polars "
        f"{PAIRED_POLARS}); the captured view is its rows, collected now, not a plan.",
        UserWarning,
        stacklevel=3,
    )
    try:
        return pl.DataFrame(captured).lazy()
    except Exception as e:
        raise RuntimeError(
            f"polars {version} cannot read the view datui returned, as a plan or as rows: {e}. "
            "Export from inside datui (press e) instead."
        ) from unreadable


def _view_frame(
    lf: pl.LazyFrame, *, options: DatuiOptions | None, capture: bool = False
) -> object | None:
    """Serialize the LazyFrame plan and launch the TUI.

    Returns the captured view when capture is requested and a dataset was open at
    quit, else None.

    The binary plan is tried first; the deprecated JSON plan only if binary is refused.
    Only a refused plan (ValueError) moves on: a RuntimeError is the TUI itself failing
    (or the capture failing on the way out), and must not launch it a second time.
    """
    payload = lf.serialize()
    if isinstance(payload, str):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*json.*deprecated", category=UserWarning)
            return datui._datui.view_from_json(payload, options=options, capture=capture)
    if not isinstance(payload, bytes):
        raise RuntimeError("LazyFrame.serialize() returned an unsupported type")
    try:
        return datui._datui.view_from_bytes(payload, options=options, capture=capture)
    except ValueError as refused:
        binary_error = refused
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*json.*deprecated", category=UserWarning)
        try:
            json_payload = lf.serialize(format="json")
        except TypeError:
            json_payload = None
        if isinstance(json_payload, str):
            try:
                return datui._datui.view_from_json(json_payload, options=options, capture=capture)
            except ValueError:
                pass
    version = getattr(pl, "__version__", "unknown")
    raise ValueError(
        f"datui cannot read this LazyFrame: it was serialized by polars {version}, and this "
        f"datui is built for polars {PAIRED_POLARS}. Pass the collected frame "
        f"(datui.view(lf.collect())) or a file path instead, or install polars {PAIRED_POLARS}."
    ) from binary_error


def view(
    data: pl.LazyFrame | pl.DataFrame | PathLike | list[PathLike] | tuple[PathLike, ...],
    *,
    capture: bool = False,
    options: DatuiOptions | None = None,
    **kwargs: object,
) -> pl.LazyFrame | None:
    """
    View data in the terminal.

    Accepts path(s), a LazyFrame, or a DataFrame. Paths may be local or remote
    (s3://, gs://, abfss://, az://, http(s)://). An az:// URL names no storage
    account: it comes from the Azure environment variables or the one Azure
    connection in the config, and without one the open is refused. Remote
    non-Parquet files are downloaded to a temp file. With multiple paths, at
    most one may be remote.

    With capture=True, returns the final table's logical view on normal quit as a
    LazyFrame — the applied query, filters, sort, drill-down, reshape and column
    order, over all matching rows — or None when no dataset was open. The result is
    a plan, not a snapshot: collecting it executes the plan again, so file-backed
    sources are reread and must remain available, and a plan over an in-memory
    frame can carry (and copy) the frame's data even when the final result would be
    small. A polars that cannot read the plans this datui writes (PAIRED_POLARS is
    the release it pairs with; 2.0 differs) gets the view's rows instead, collected
    at quit, with a UserWarning; that holds every row of the view in memory. Views
    over files datui downloaded or decompressed into temporary files are refused
    (RuntimeError); export from inside datui (press e) instead — that also remains
    the way to write rows out without capture.

    Options are datui.OPTION_NAMES: the open's own (format, table, delimiter,
    no_header, header_rows, skip_rows, ...) and config keys' (comment, null_values,
    infer_types, row_numbers, ...), each meaning what its flag or key means on the
    command line; config={"display.row_numbers": True} sets any config key, as -c
    does. Pass them as keywords or as a DatuiOptions instance. For a frame, only
    display options apply.

    Args:
        data: Path(s), LazyFrame, or DataFrame.
        capture: Return the final view as a LazyFrame on quit (None if no dataset).
        options: Optional DatuiOptions; use default options when None.
        **kwargs: Optional DatuiOptions fields (override options when both given).

    Returns:
        The final view as a LazyFrame when capture=True and a dataset was open;
        None otherwise.

    Raises:
        TypeError: Unsupported type for data or invalid option keyword.
        ValueError: Empty path list or invalid LazyFrame serialization.
        FileNotFoundError: A given path does not exist.
        PermissionError: Read access denied for a path.
        RuntimeError: No interactive terminal (e.g. Jupyter or piped output), error
            serializing the LazyFrame plan, launching the TUI, or returning a
            captured view.
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
        captured = datui._datui.view_paths(_to_path_strings(data), options=opts, capture=capture)
        return _captured_frame(captured) if captured is not None else None

    # A DataFrame crosses over the Arrow C stream, which no polars release changes.
    if isinstance(data, pl.DataFrame):
        captured = datui._datui.view_from_arrow(data, options=opts, capture=capture)
        return _captured_frame(captured) if captured is not None else None

    if hasattr(data, "lazy") and callable(getattr(data, "lazy", None)):
        lf = data.lazy()
    elif hasattr(data, "serialize") and callable(getattr(data, "serialize", None)):
        lf = data
    else:
        raise TypeError(
            "data must be path(s) (str or Path), a URL (s3://, gs://, abfss://, http(s)://), "
            "or a polars.LazyFrame or polars.DataFrame"
        )

    try:
        captured = _view_frame(lf, options=opts, capture=capture)
    except AttributeError as e:
        raise TypeError("data must be a LazyFrame or DataFrame") from e
    return _captured_frame(captured) if captured is not None else None
