# datui Python package

View Polars LazyFrames or DataFrames in the terminal with the datui TUI. The package provides `datui.view(lf)` / `datui.view(df)` and the `datui` CLI command (when the bundled binary is present or `datui` is on PATH). Data is passed via `LazyFrame.serialize(format="json")` for compatibility across Polars versions. Binary serialization is only used as a fallback when JSON is not available.

## Install

**From PyPI** (stable):

```bash
pip install datui "polars>=0.20"
```

**From the repo** (development):

From the `python/` directory, with a venv activated:

```bash
# Requires maturin: pip install maturin
cd python && maturin develop
pip install "polars>=0.20"
```

## Usage

**View a LazyFrame or DataFrame in the TUI:**

```python
import polars as pl
import datui

# From a LazyFrame (e.g. scan)
lf = pl.scan_csv("data.csv")
datui.view(lf)

# From a DataFrame (converted to LazyFrame internally)
df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
datui.view(df)
```

Press `q` to exit the TUI and return to Python.

**Run the CLI from Python** (e.g. `datui file.csv`):

If you installed via pip, the `datui` command is available and runs the bundled CLI. From a development install, it uses the `datui` binary on your PATH (e.g. from `cargo build` at the repo root).

## Options (for `datui.view`)

- `debug=False`: Enable debug overlay.
- `row_numbers=False`: Show row numbers in the table.

Configuration (theme, etc.) uses the same config as the CLI: `~/.config/datui/config.toml`.

## Low-level API

- `datui.view_from_json(json_str, *, debug=False, row_numbers=False)` accepts a JSON string from `LazyFrame.serialize(format="json")`. This is the path used by `view()` and works across Polars versions.
- `datui.view_from_bytes(data, *, debug=False, row_numbers=False)` accepts bytes from `LazyFrame.serialize()` (binary). Use only when you already have binary data; format compatibility with the extension is not guaranteed across Polars versions.

## Run tests

From the repo root (with the package installed, e.g. after `cd python && maturin develop`):

```bash
pytest python/tests/ -v
```
