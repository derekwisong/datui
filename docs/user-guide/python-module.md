# Python Module

```bash
pip install datui
```

The package on [PyPI](https://pypi.org/project/datui/) installs the `datui`
command and a module that opens the same UI on a Polars frame. Python 3.10 or
later.

## View a frame

```python
import polars as pl
import datui

lf = pl.scan_parquet("events.parquet").filter(pl.col("region") == "EU")
datui.view(lf)          # a LazyFrame: the plan is passed, not the data
datui.view(lf.collect())  # a DataFrame works too
```

<kbd>q</kbd> closes datui and returns to Python. A LazyFrame stays lazy, so
datui can request rows without collecting the entire frame first. Sorting,
aggregation and other operations may still scan the full input.

## View a path

`view` also accepts a path, a URL, or a list of paths, with the same options as
the command line:

```python
datui.view("data.csv", delimiter=";", has_header=False)
datui.view("s3://bucket/events/", hive=True)
datui.view(["jan.parquet", "feb.parquet"])
```

Options can be passed as keywords or as a `datui.DatuiOptions` instance.
Common options include `delimiter`, `has_header`, `null_values`, `hive`,
`excel_sheet` and `row_numbers`. Use `help(datui.DatuiOptions)` for the full
Python option list. For a frame, only display options apply.

## Return the current view

```python
result = datui.view(lf, capture=True)
if result is not None:
    result.collect()  # or keep transforming, or sink_parquet(...)
```

`capture=True` returns the final table's logical view on a normal quit: the
applied query, filters, sort, drill-down, reshape and column order, over all
matching rows. It is a LazyFrame even for DataFrame input, so Python decides
when to collect or write. `None` when no dataset was open at quit; text still
sitting in an editor and unfinished background work are not part of it.

**Collecting runs the returned plan again.** Python rebuilds it with its own
Polars resources; the rows you saw in datui are not cached for Python.

| Source | What collecting the result does |
|---|---|
| Scanned files (`pl.scan_*`, paths) | Executes the plan again and rereads the files. They must still exist; datui's preview is not a result cache |
| In-memory frame (`df.lazy()`) | The plan can embed the DataFrame, so it may be copied on the way in and again on the way out, even when the final result is a few rows |
| Materialized intermediates | A LazyFrame built from an already computed intermediate may serialize that intermediate's data too |
| Downloaded or decompressed files | Refused with `RuntimeError`: the temp file is removed when datui exits. [Export](exporting-data.md) with <kbd>e</kbd> instead |

Without `capture`, exporting with <kbd>e</kbd> inside the TUI remains the way
to get data out.

## Compatibility

A frame is handed over as a serialized Polars plan, which the wheel reads with
its own embedded Polars (0.55). The two need to agree on the plan format:

| Python `polars` | Frames |
|---|---|
| 1.43 | The release Polars pairs with Rust 0.55; fully tested |
| 1.38 to 1.42 | Read in testing (scan, filter, group by, join, cast, sort, unique) |
| 1.44 | Most plans read; 1.44 writes joins 0.55 cannot read |
| 1.37 and earlier | Refused: older path format |

The wheel declares `polars>=1.38` and never downgrades the `polars` you have. A
plan the wheel cannot read raises `ValueError` before the TUI opens, naming the
release it is built for. Paths do not go through the plan and work with any
`polars` version — though a view captured with `capture=True` always comes back
as a plan, and one your `polars` cannot read raises `RuntimeError` after the
TUI closes.

## Building from source

See [Python Bindings](../for-developers/python-bindings.md).
