# Python API

`datui.view()` opens a file, a URL or a Polars frame in the terminal.

## Options

<!-- generated: options -->
| Keyword | Takes | Command line | What it does |
|---|---|---|---|
| `format` | string | `--format` | File format, when the extension does not say: parquet, csv, tsv, psv, json, jsonl, arrow, avro, orc, excel, safetensors, gguf, nmea, gpx, audio, midi, sqlite, vcd, fix, sdf, numpy, elf, ulog, dataflash, candump, text, journal; or a format spec, by name (acme.l2feed), file (./acme.toml) or http(s), s3, gs or az URL (at most 1 MiB) |
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
