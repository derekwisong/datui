# Command-line options

<!-- Generated from crates/datui-cli by `gen_docs`. Do not edit. -->

`datui --help` prints these; `datui COMMAND --help` a command's own.

```text
Usage: datui [OPTIONS] [PATH]... [COMMAND]
```

## Arguments

| Option | Description |
|---|---|
| `[<PATH>]...` | Files, directories, globs or URLs to open; files of the same shape open as one table. - reads standard input, as does no PATH when data is piped in. With no PATH and nothing piped in, datui opens the home screen |

## Open

| Option | Description |
|---|---|
| `-F, --format <FMT>` | File format, when the extension does not say: parquet, csv, tsv, psv, json, jsonl, arrow, avro, orc, excel, safetensors, gguf, nmea, gpx, audio, midi, sqlite, vcd, fix, sdf, numpy, elf, ulog, dataflash, candump, text, journal. Or a format spec: its name (`datui formats` lists them), its file (a path with a / or ending .toml), or its http(s), s3, gs or az URL (at most 1 MiB) |
| `-t, --table <NAME>` | Table to open from a file that holds several. Excel: a worksheet by name, or by 0-based index when no worksheet has that name. NMEA: fixes (default), GGA, RMC, VTG, GSA, GSV, GLL, ZDA or sentences. SQLite: a table or view by name. NumPy: an array of an archive (.npz) by name. ELF: symbols (default) or sections. ULog: a topic. DataFlash: a message type. candump: frames (default), signals, or a message a dictionary names. Hugging Face cache and DatasetDict directories: a split (default train) |
| `--hive` | Read a glob as one partitioned table, or force partition columns on a directory whose layout does not say so. Ignored for a single file |
| `--compression <C>` | Compression, when the extension does not say: gzip, zstd, bzip2 or xz |
| `--dict <FILE>` | A dictionary to decode with, ahead of those on the format search path: QuickFIX XML (.xml) for FIX logs, DBC (.dbc) for CAN logs, or TOML with kind = "fix" or "dbc". Repeatable |
| `-f, --follow` | Follow the file as it grows, as tail -f does: a local CSV, TSV, PSV or NDJSON file or Arrow IPC stream, or standard input (-). t pauses and resumes; Esc stops |
| `--tee <FILE>` | Record standard input to FILE while viewing it. With -, pass it through to standard output, as tee does, while drawing on the terminal |
| `--tee-raw` | With --tee: write FILE exactly as the bytes arrived. Without it, sizes a stream left blank in its header are filled in when it ends |
| `--force` | With --tee: replace FILE if it exists |
| `--hex` | Open in the hex view, whatever the file holds |
| `--hex-width <N>` | Bytes per row in the hex view, so records line up (default: 8, 16, 32 or 64, as many as fit) |
| `--view <NAME>` | Apply a saved view by name once the data is on screen |
| `--temp-dir <DIR>` | Directory for decompression temp files. Unset, the system's temp directory. [config: read.temp_dir] |

## Delimited text

| Option | Description |
|---|---|
| `--delimiter <C>` | Column separator: one character, tab, \t or a code such as 0x1f (default: , for .csv, tab for .tsv, \| for .psv) |
| `--no-header` | Read the first row as data; columns are named column_1, column_2, ... |
| `--header-rows <N[,M...]>` | The line holding the header, or several lines separated by commas, counted from 1 before anything is skipped. Several lines are joined per column ([csv] header_join), and the data starts after the last |
| `--footer-rows <N>` | Skip this many rows at the end, such as a footer. Reads the whole file to count rows |
| `--skip-rows <N>` | Skip this many rows at the start; the header is read after them. Quote-aware, unlike --skip-lines |
| `--skip-lines <N>` | Skip this many raw lines at the start. Every newline ends a line, even one inside quotes |
| `--comment <PREFIX>` | Lines starting with this are comments, before the header and among the data. [config: csv.comment] |
| `--skip-initial-space[=<BOOL>]` | Ignore the spaces after a delimiter, so padded numbers are numbers and a cell of spaces is null. [config: csv.skip_initial_space] |
| `--null <VAL>` | Values read as null: VAL in every column, COL=VAL in column COL only. --null is repeatable and replaces this list. [config: csv.null_values] |
| `--infer-types[=<COLS\|off>]` | Read string columns as dates, times, durations or numbers where every value parses, after trimming: true for all, false for none, or a list of columns. CSV, and dates in JSON. A column with a leading zero (02134) stays text; a later value that does not parse is null, and the Notes tab counts them. [config: read.infer_types] |
| `--infer-rows <N>` | Rows read to infer column types. [config: csv.infer_rows] |
| `--ignore-errors[=<BOOL>]` | Skip rows that do not parse instead of failing. [config: csv.ignore_errors] |

## Display

| Option | Description |
|---|---|
| `--row-numbers[=<BOOL>]` | Number rows on the left by their place in the source; a row keeps its number through a sort or filter (# toggles). auto numbers text and logs; true or false turns them on or off for every table. [config: display.row_numbers] |
| `--number-format <F>` | Digit grouping: none, thousands, european, si, swiss, indian, underscore or system, or a [display.number_format] table (, toggles). [config: display.number_format] |
| `--mouse[=<BOOL>]` | Use the mouse: the wheel scrolls and a click selects. false leaves the mouse to the terminal. [config: display.mouse] |
| `--sample-rows <N>` | Rows an analysis samples from a larger table, spread across the whole table; 0 reads every row. [config: analysis.sample_rows] |

## Config

| Option | Description |
|---|---|
| `-c, --config <KEY=VALUE>` | Set a config key for this run, written as in the file: -c display.row_numbers=true. Repeatable; the key's own flag, where it has one, still wins. `datui config keys` lists the keys |

## Logging

| Option | Description |
|---|---|
| `--log-file <PATH>` | Where the log is written. Unset, datui.log in the cache directory. [config: log.file] |
| `--log-level <LEVEL>` | How much the log records (default warn). DATUI_LOG overrides the config file; -c and --log-level override DATUI_LOG. [config: log.level] |

`-h`, `--help` prints help; `-V`, `--version` the version.

## Commands

| Command | Does |
|---|---|
| `datui formats` | List the format specs and dictionaries (FIX, DBC) on the search path: each one's name, what it matches, its file, and the copies it overrides |
| `datui formats check SPEC [FILE]` | Check a format spec or a dictionary (QuickFIX XML, DBC or TOML), by name or by file. With FILE, print the first rows it decodes, or what the dictionary names in that log. Exits non-zero on an error |
| `datui config` | Write the default config file, list the files read, or list every key |
| `datui config init` | Write the default config file, every key commented out at its default |
| `datui config path` | Print the config files read, lowest precedence first |
| `datui config keys` | List every key: its type, default, the value in effect and what set it |
| `datui catalog` | Show the catalogs of named datasets on the home screen, or check a catalog file |
| `datui catalog show [NAME]` | Print catalog NAME's file (examples is the one datui ships). Without NAME, list the catalogs: id, label, datasets and file |
| `datui catalog check FILE` | Check a catalog file and list its datasets. A mistake is reported with its line and the fix, and the command exits non-zero |
| `datui theme` | List the themes, built in and in the config directory's themes/, or print one as a file to start from |
| `datui theme list` | List the themes: each one's name, mode, source and description |
| `datui theme show NAME` | Print a theme as a file with every slot, to save into themes/ and edit |
| `datui cache` | Clear the cache: recents, history, schemas and copies |
| `datui cache clear` | Delete the cache directory's contents; with --recents, only the recent datasets |
| `datui views` | List or remove saved views |
| `datui views list` | List the saved views: name, what files they match, when last used |
| `datui views rm NAME` | Remove one saved view by name |
| `datui views clear` | Remove every saved view |
| `datui completions` | Print the shell completion script for SHELL |
| `datui man` | Show a manual page, list them, or write them all under a directory |

## Examples

| Command | Does |
|---|---|
| `datui` | Open the home screen, where Example datasets lists the catalog that comes with datui |
| `datui https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv` | Palmer penguins from the web |
| `datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/` | NOAA daily highs for 2024, one table from public S3 |
| `datui --hive 's3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=T*/*.parquet'` | A glob, read as one partitioned table: every 2024 element starting with T |
| `datui abfss://release@overturemapswestus2.dfs.core.windows.net/` | Overture Maps releases in public Azure storage, listed on the home screen |
| `datui https://huggingface.co/openai-community/gpt2/resolve/main/model.safetensors` | A model's tensors, read from its header without downloading the weights |
| `printf 'id,amount\n1,9.50\n2,3.25\n' \| datui` | Data piped in; the format is read from its first bytes |
| `journalctl -o json -n 1000 \| datui` | The last 1,000 systemd journal entries, a column per field |
| `(echo time,value; while sleep 0.2; do echo "$(date +%s),$RANDOM"; done) \| datui -f -` | Rows as they arrive; t pauses, Esc stops following |
| `datui --hex /bin/sh` | A file's bytes in the hex view |
| `datui -c display.row_numbers=true https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv` | Any config key, for this run |
| `datui config init` | Write the config file, every key commented out at its default |
| `datui formats` | List the format specs and dictionaries datui finds |
| `datui man keys` | The keys of every screen, as a manual page |
| `datui catalog show examples` | Print the catalog datui ships, a worked example of the catalog format |
