# Use datui from Python

`datui.view()` opens the terminal UI on a Polars frame, a file or a URL, and
can hand the final view back as a LazyFrame.

```bash,install
pip install datui
```

The package on [PyPI](https://pypi.org/project/datui/) installs the `datui`
command and the `datui` module. It needs Python 3.10 or later.
[Python API](../reference/python-api.md) lists every option, the return value
and the errors.

## View a frame

```python,network
import polars as pl
import datui

url = "https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv"
penguins = pl.scan_csv(url)
datui.view(penguins)
datui.view(penguins.collect())
```

A LazyFrame passes its plan, not its data, and stays lazy: datui reads the
rows it needs to show. Sorting, aggregation and other operations may still scan the
whole input. A DataFrame works too. <kbd>q</kbd> closes datui and returns to
Python.

## View a path

`view` also takes a path, a URL or a list of paths, with the command line's
options as keywords:

```python
import polars as pl
import datui

pl.DataFrame({"month": [1], "sales": [10.5]}).write_parquet("jan.parquet")
pl.DataFrame({"month": [2], "sales": [12.0]}).write_parquet("feb.parquet")
datui.view(["jan.parquet", "feb.parquet"])

with open("data.csv", "w") as f:
    f.write("1;north\n2;south\n")
datui.view("data.csv", delimiter=";", no_header=True)
```

Remote paths read as they do on the command line:

```python,network
import datui

datui.view("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/")
datui.view("https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv")
```

The keywords are named after the flags and config keys: `delimiter` for
`--delimiter`, `row_numbers` for `display.row_numbers`, and `config` for any
key, the way `-c` sets it. Pass them as keywords or as a `datui.DatuiOptions`.
`datui.OPTION_NAMES` lists them, and [Python API](../reference/python-api.md#options)
gives each one's type and flag. For a frame, only display options apply.

## Return the current view

```python,network
import polars as pl
import datui

url = "https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv"
result = datui.view(pl.scan_csv(url), capture=True)
if result is not None:
    print(result.collect())
```

Run the species summary from the
[quick start](../getting-started/quick-start.md#2-group-by-species) and press
<kbd>q</kbd>. `result` collects to the three rows, with Gentoo first at
5076.01626 and 124 penguins. Collecting reads the CSV from the web again.

With `capture=True`, a normal quit returns the final view as a LazyFrame, even
for DataFrame input: the applied query, filters, sort, drill-down, reshape and
column order, over all matching rows. It returns `None` if no dataset was open
at quit.

**Collecting runs the returned plan again**, with Python's own Polars; the
rows datui showed are not cached. If your `polars` cannot read the plan, you
get rows instead; see [Compatibility](#compatibility).

| Source | What collecting the result does |
|---|---|
| Scanned files (`pl.scan_*`, paths) | Executes the plan again and rereads the files, which must still exist |
| In-memory frame (`df.lazy()`) | The plan can embed the DataFrame, so it may be copied on the way in and again on the way out, even when the result is a few rows |
| Materialized intermediates | A LazyFrame built from an already computed intermediate may carry that intermediate's data too |
| Downloaded or decompressed files | Refused with `RuntimeError`: the temp file is removed when datui exits. [Export](exporting-data.md) with <kbd>e</kbd> instead |

Without `capture`, exporting with <kbd>e</kbd> is the way to get data out.

## Compatibility

A DataFrame crosses into datui as Arrow data, which works with any `polars` the
wheel accepts. A LazyFrame crosses as a serialized Polars plan, read by the
wheel's embedded Polars (0.55), and a captured view comes back as a plan the
same way. Both sides must agree on the plan format:

| Python `polars` | LazyFrames | Captured views |
|---|---|---|
| 2.0 | Most plans read; some, such as joins and `rank`, are refused | Rows, with a warning |
| 1.44 | Most plans read; some, such as joins, are refused | Plans |
| 1.43 | The release Polars pairs with Rust 0.55; fully tested | Plans |
| 1.38 to 1.42 | Read in testing (scan, filter, group by, join, cast, sort, unique) | Plans |

The wheel declares `polars>=1.38` and never downgrades the `polars` you have. A
plan the wheel cannot read raises `ValueError` before the TUI opens, naming the
Polars release the wheel is built for. Pass `lf.collect()` or a path instead. Paths do not go
through a plan and work with any `polars` version. A DataFrame column of Python
objects (`pl.Object`) or `Float16` raises `ValueError` naming the column. Cast a
`Float16` column to `Float32` first.

When your `polars` cannot read the captured plan, or the view has no plan form
(a SQLite table, a text or log file), `view()` returns the view's
rows instead, with a `UserWarning` saying why. datui collects the rows at quit
and holds them all in memory, so collecting the result reads nothing again.

## Build from source

See [Build Python bindings](../for-developers/python-bindings.md).
