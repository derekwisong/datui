# Python API

`datui.view()` opens a file, a URL or a Polars frame in the terminal, and can
hand the final view back. [Use datui from Python](../user-guide/python-module.md)
is the guide.

```python
import polars as pl
import datui

df = pl.DataFrame({"city": ["Oslo", "Lima", "Pune"], "temp_c": [4.5, 19.0, 27.5]})
result = datui.view(df, capture=True, row_numbers=True)
if result is not None:
    print(result.collect())
```

## datui.view

The signature; `data` is the one argument it needs:

```python,template
datui.view(data, *, capture=False, options=None, **kwargs) -> polars.LazyFrame | None
```

| Parameter | Takes |
|---|---|
| `data` | A `polars.LazyFrame` or `polars.DataFrame`; a path or URL as `str` or `pathlib.Path`; or a list or tuple of them, read as one table as on the command line |
| `capture` | `True` returns the final view on a normal quit |
| `options` | A `datui.DatuiOptions` |
| `**kwargs` | Any [option](#options) by name. With `options` too, a keyword wins over the same option there |

A path or URL is read as the command line reads it (`s3://`, `gs://`,
`abfss://`, `http(s)://`, globs), and the open's options apply. A frame is
handed over as its serialized plan, and only display options apply to it.
An `az://container/path` URL takes its account from the Azure environment
variables or from the config's one Azure connection.

### Return value

`None`, unless `capture=True` and a dataset was open at quit. Then a
`polars.LazyFrame`: the applied query, filters, sort, drill-down, reshape and
column order, over every matching row. It is a plan, not the rows datui
showed: collecting it runs the plan again with Python's Polars, rereading
files that must still exist.

### Errors

| Raised | When |
|---|---|
| `TypeError` | `data` is not a frame, path or list of paths; a keyword is not an option |
| `ValueError` | An empty list of paths; an option value the flag or key would refuse (`format="cvs"`, `max_buffered="512"`, an unknown `config` key); a frame plan this wheel's Polars cannot read, naming the Polars release it is built for |
| `FileNotFoundError` | A path that does not exist. A glob is not checked |
| `PermissionError` | A path that cannot be read |
| `RuntimeError` | No terminal (a notebook, piped output); the terminal UI failing; a captured view over a file datui downloaded or decompressed into a temporary file, which is removed at quit; a captured view your Polars cannot read |

## datui.DatuiOptions

The same options as keywords, made once and passed as `options=`. A value is
checked when it is made, with the errors above.

```python
import datui

with open("readings.csv", "w") as f:
    f.write("# exported 2024-03-01\nsensor;value\na;1.5\nb;2.0\n")
opts = datui.DatuiOptions(delimiter=";", comment="#")
datui.view("readings.csv", options=opts)
```

| Name | What it is |
|---|---|
| `datui.OPTION_NAMES` | The option names, as a tuple: the table below |
| `datui.PAIRED_POLARS` | The Python Polars release the wheel's plans are written for, such as `"1.43"` |
| `datui.CompressionFormat` | `Gzip`, `Zstd`, `Bzip2`, `Xz`. The `compression` option takes their names as strings |

A value is written as the flag or key takes it: `True` or `False`, a number, a
string, a list of strings, or a `pathlib.Path`. `delimiter` also takes the
character's code (`ord(";")`).

## Options

<!-- generated: options -->
| Keyword | Takes | Command line | What it does |
|---|---|---|---|
| `format` | string | `--format` | File format, when the extension does not say: parquet, csv, tsv, psv, json, jsonl, arrow, avro, orc, excel, safetensors, gguf, nmea, gpx, audio, midi, sqlite, vcd, fix, sdf, numpy, elf, ulog, dataflash, candump, text, journal; or a format spec: its name (`datui formats` lists them), its file (a path with a / or ending .toml), or its http(s), s3, gs or az URL (at most 1 MiB) |
| `table` | string | `--table` | Table to open from a file that holds several. Excel: a worksheet by name, or by 0-based index when no worksheet is so named. NMEA: fixes (default), GGA, RMC, VTG, GSA, GSV, GLL, ZDA or sentences. SQLite: a table or view by name. NumPy: an array of an archive (.npz) by name. ELF: symbols (default) or sections. ULog: a topic. DataFlash: a message type. candump: frames (default), signals, or a message a dictionary names. Hugging Face cache and DatasetDict directories: a split (default train) |
| `hive` | bool | `--hive` | Read a glob as one partitioned table, or force partition columns on a directory whose layout does not say so. Ignored for a single file |
| `compression` | gzip \| zstd \| bzip2 \| xz | `--compression` | Compression, when the extension does not say: gzip, zstd, bzip2 or xz |
| `dict` | list | `--dict` | A dictionary to decode with, over those on the format search path: QuickFIX XML (.xml) for FIX logs, DBC (.dbc) for CAN logs, or TOML with kind = "fix" or "dbc". Repeatable |
| `view` | string | `--view` | Apply a saved view by name once the data is on screen |
| `delimiter` | string | `--delimiter` | Column separator: one character, tab, \t or a code such as 0x1f (default: , for .csv, tab for .tsv, \| for .psv) |
| `no_header` | bool | `--no-header` | Read the first row as data; columns are named column_1, column_2, ... |
| `header_rows` | list | `--header-rows` | The line, or comma-separated lines, holding the header, counted from 1 before anything is skipped. Several are joined per column ([csv] header_join); the data starts after the last |
| `footer_rows` | integer | `--footer-rows` | Skip this many rows at the end, such as a footer. Reads the whole file to count rows |
| `skip_rows` | integer | `--skip-rows` | Skip this many rows at the start; the header is read after them. Quote-aware, unlike --skip-lines |
| `skip_lines` | integer | `--skip-lines` | Skip this many raw lines at the start, split on newlines alone: a newline inside quotes counts |
| `infer_types` | bool \| list of columns | `--infer-types` | Read string columns as dates, times, durations or numbers where every value parses, after trimming: true for all, false for none, or a list of columns. CSV, and dates in JSON. |
| `parquet_schema` | union \| first | `-c read.parquet_schema=...` | A partitioned Parquet dataset's schema: union is every column any file has, from their footers; first lets Polars take one file's. |
| `decompress_in_memory` | bool | `-c read.decompress_in_memory=...` | Decompress a compressed CSV, TSV or PSV into memory instead of to a temp file. |
| `temp_dir` | path | `--temp-dir` | Directory for decompression temp files. Unset: the system's. |
| `audio_float` | bool | `-c read.audio_float=...` | Show integer audio samples as float in [-1, 1]. |
| `comment` | string | `--comment` | Lines starting with this are comments, before the header and among the data. |
| `header_join` | string | `-c csv.header_join=...` | Joins a column's names when --header-rows names several lines. |
| `skip_initial_space` | bool | `--skip-initial-space` | Ignore the spaces after a delimiter, so padded numbers are numbers and a cell of spaces is null. |
| `null_values` | list | `--null` | Values read as null: VAL in every column, COL=VAL in column COL only. --null is repeatable and replaces this list. |
| `infer_rows` | integer | `--infer-rows` | Rows read to infer column types. |
| `ignore_errors` | bool | `--ignore-errors` | Skip rows that do not parse instead of failing. |
| `row_numbers` | bool | `--row-numbers` | Show row numbers on the left (# toggles). |
| `row_numbers_start` | integer | `-c display.row_numbers_start=...` | The first row's number. |
| `column_colors` | bool | `-c display.column_colors=...` | Color cells by column type. |
| `right_align_numbers` | bool | `-c display.right_align_numbers=...` | Right-align numeric columns and their headers. |
| `number_format` | preset \| table | `--number-format` | Digit grouping: none, thousands, european, si, swiss, indian, underscore or system, or a [display.number_format] table (, toggles). |
| `pages_ahead` | integer | `-c performance.pages_ahead=...` | Pages of rows buffered ahead of the screen. |
| `pages_behind` | integer | `-c performance.pages_behind=...` | Pages of rows buffered behind the screen. |
| `max_buffered_rows` | integer | `-c performance.max_buffered_rows=...` | Most rows the table buffers between reads; 0 for no limit. |
| `max_buffered` | size | `-c performance.max_buffered=...` | Most memory the buffered rows may take, estimated from the schema; 0 for no limit. Rounded up to whole MiB. |
| `streaming` | bool | `-c performance.streaming=...` | Use the Polars streaming engine where it applies. |
| `sample_rows` | integer | `--sample-rows` | Rows an analysis samples from a larger table, spread across all of it; 0 reads every row. |
| `config` | dict | `-c KEY=VALUE` | Any config key to its value, as `-c` sets it: `config={"display.row_numbers": True}` |
<!-- end generated: options -->
