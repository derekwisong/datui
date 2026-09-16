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
scanning a large file and viewing it costs no more than it does in the CLI.

## View a path

`view` also accepts a path, a URL, or a list of paths, with the same options as
the command line:

```python
datui.view("data.csv", delimiter=";", has_header=False)
datui.view("s3://bucket/events/", hive=True)
datui.view(["jan.parquet", "feb.parquet"])
```

Options can be passed as keywords or as a `datui.DatuiOptions` instance.
They mirror the [command-line flags](../reference/command-line-options.md):
`delimiter`, `has_header`, `skip_lines`, `skip_rows`, `skip_tail_rows`,
`null_values`, `compression`, `parse_dates`, `parse_strings`, `hive`,
`single_spine_schema`, `excel_sheet`, `temp_dir`, `decompress_in_memory`,
`row_numbers`, `row_start_index`, `pages_lookahead`, `pages_lookback`,
`s3_endpoint_url`, `s3_access_key_id`, `s3_secret_access_key`, `s3_region`,
`polars_streaming` and `debug`. For a frame, only the display options apply.

## Compatibility

A frame is handed over as a serialized Polars plan, which the wheel reads with
its own embedded Polars (0.55). The two need to agree on the plan format:

| Python `polars` | Frames |
|---|---|
| 1.43 | The release Polars pairs with Rust 0.55; fully tested |
| 1.38 to 1.42 | Read in testing (scan, filter, group by, join, cast, sort, unique) |
| 1.44 and later | Refused: joins use a newer plan node |
| 1.37 and earlier | Refused: older path format |

The wheel declares `polars>=1.38,<1.44`; an incompatible plan raises
`ValueError` before the TUI opens. Paths do not go through the plan and work
with any `polars` version.

## Building from source

See [Python Bindings](../for-developers/python-bindings.md).
