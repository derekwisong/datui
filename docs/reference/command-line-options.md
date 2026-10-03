# Command Line Options

## Usage

```
Usage: datui [OPTIONS] [PATH]... [COMMAND]
```

## Options

| Option | Description |
|--------|-------------|
| `[<PATH>]` | Files, directories, globs or URLs to open; files of one shape are one table. - reads standard input, as does no PATH when data is piped in. No PATH opens the home screen |
| `-F, --format <FMT>` | File format, when the extension does not say: parquet, csv, tsv, psv, json, jsonl, arrow, avro, orc, excel, safetensors, gguf, nmea, gpx, audio, midi, sqlite, vcd, fix, sdf, numpy, elf, ulog, dataflash, candump, text, journal; or a format spec, by name (acme.l2feed), file (./acme.toml) or http(s), s3, gs or az URL (at most 1 MiB) |
| `-t, --table <NAME>` | Table to open from a file that holds several. Excel: a worksheet by 0-based index or name. NMEA: fixes (default), GGA, RMC, VTG, GSA, GSV, GLL, ZDA or sentences. SQLite: a table or view by name. NumPy: an array of an archive (.npz) by name. ELF: symbols (default) or sections. ULog: a topic. DataFlash: a message type. candump: frames (default), signals, or a message a dictionary names. Hugging Face cache and DatasetDict directories: a split (default train) |
| `--hive` | Read a glob as one partitioned table, or force partition columns on a directory whose layout does not say so. Ignored for a single file |
| `--compression <C>` | Compression, when the extension does not say: gzip, zstd, bzip2 or xz |
| `--dict <FILE>` | A dictionary to decode with, over those on the format search path: QuickFIX XML (.xml) for FIX logs, DBC (.dbc) for CAN logs, or TOML with kind = "fix" or "dbc". Repeatable |
| `-f, --follow` | Follow the file as it grows, as tail -f does: a local CSV, TSV, PSV or NDJSON file or Arrow IPC stream, or standard input (-). t pauses and resumes; Esc stops |
| `--tee <FILE>` | Record standard input to FILE while viewing it, byte for byte. A WAV file's sizes are filled in when the stream ends. With -, pass it on to standard output, as tee does, and draw on the terminal |
| `--tee-raw` | With --tee: leave FILE exactly as the bytes came, a WAV header's sizes included |
| `--force` | With --tee: replace FILE if it is there |
| `--hex` | Open in the hex view, whatever the file holds |
| `--hex-width <N>` | Bytes a row of the hex view holds, so records line up (default: 8, 16, 32 or 64, as many as fit) |
| `--view <NAME>` | Apply a saved view by name once the data is on screen |
| `--temp-dir <DIR>` | Directory for decompression temp files. Unset: the system's. [config: read.temp_dir] |
| `--delimiter <C>` | Column separator: one character, tab, \t or a code such as 0x1f (default: , for .csv, tab for .tsv, \| for .psv) |
| `--no-header` | Read the first row as data; columns are named column_1, column_2, ... |
| `--header-rows <N[,M...]>` | The line, or comma-separated lines, holding the header, counted from 1 before anything is skipped. Several are joined per column ([csv] header_join); the data starts after the last |
| `--footer-rows <N>` | Skip this many rows at the end, such as a footer. Reads the whole file to count rows |
| `--skip-rows <N>` | Skip this many rows at the start; the header is read after them. Quote-aware, unlike --skip-lines |
| `--skip-lines <N>` | Skip this many raw lines at the start, split on newlines alone: a newline inside quotes counts |
| `--comment <PREFIX>` | Lines starting with this are comments, before the header and among the data. [config: csv.comment] |
| `--skip-initial-space[=<BOOL>]` | Ignore the spaces after a delimiter, so padded numbers are numbers and a cell of spaces is null. [config: csv.skip_initial_space] |
| `--null <VAL>` | Values read as null: VAL in every column, COL=VAL in column COL only. --null is repeatable and replaces this list. [config: csv.null_values] |
| `--infer-types[=<COLS|off>]` | Read string columns as dates, times, durations or numbers where every value parses, after trimming: true for all, false for none, or a list of columns. CSV, and dates in JSON. [config: read.infer_types] |
| `--infer-rows <N>` | Rows read to infer column types. [config: csv.infer_rows] |
| `--ignore-errors[=<BOOL>]` | Skip rows that do not parse instead of failing. [config: csv.ignore_errors] |
| `--row-numbers[=<BOOL>]` | Show row numbers on the left (# toggles). [config: display.row_numbers] |
| `--number-format <F>` | Digit grouping: none, thousands, european, si, swiss, indian, underscore or system, or a [display.number_format] table (, toggles). [config: display.number_format] |
| `--mouse[=<BOOL>]` | Take the mouse: the wheel scrolls, a click selects. false leaves it to the terminal. [config: display.mouse] |
| `--sample-rows <N>` | Rows an analysis samples from a larger table, spread across all of it; 0 reads every row. [config: analysis.sample_rows] |
| `-c, --config <KEY=VALUE>` | Set a config key for this run, as in the file: -c display.row_numbers=true. Repeatable; a flag of the key's own still wins. `datui config keys` lists them |
| `--log-file <PATH>` | Where the log goes. Unset: datui.log in the cache directory. [config: log.file] |
| `--log-level <LEVEL>` | How much the log says (default warn). DATUI_LOG beats a config file's; -c and --log-level beat DATUI_LOG. [config: log.level] |

## Commands

| Command | Does |
|---------|------|
| `datui formats` | List the format specs and dictionaries (FIX, DBC) on the search path: each one's name, what it matches, its file, and the copies it overrides |
| `datui formats check SPEC [FILE]` | Check a format spec or a QuickFIX dictionary, by name or by file; with FILE, print its first decoded rows. Exits non-zero on an error |
| `datui config` | Write the default config file, list the files read, or list every key |
| `datui config init ` | Write the default config file, every key commented out at its default |
| `datui config path ` | Print the config files read, lowest precedence first |
| `datui config keys ` | List every key: its type, default, the value in effect and what set it |
| `datui cache` | Clear the cache: recents, history, schemas and copies |
| `datui cache clear ` | Delete the cache directory's contents, or with --recents only the recent datasets |
| `datui views` | List or remove saved views |
| `datui views list ` | List the saved views: name, what files they match, when last used |
| `datui views rm NAME` | Remove one saved view by name |
| `datui views clear ` | Remove every saved view |
| `datui completions` | Print the shell completion script for SHELL |

## Examples

| Command | Does |
|---------|------|
| `datui` | Open the home screen. Public datasets lists the built-in catalog |
| `datui https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv` | Palmer penguins from the web; datui asks before it downloads |
| `datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/` | NOAA daily highs for 2024, one table from public S3 |
| `datui abfss://release@overturemapswestus2.dfs.core.windows.net/` | Overture Maps releases in public Azure storage, listed on the home screen |
| `datui jan.csv feb.csv mar.csv` | Files of the same shape, as one table |
| `curl -s https://example.com/data.csv.gz \| datui` | Data piped in; the format is read from its first bytes |
| `serial-logger \| datui -f -` | Rows as they arrive; t pauses, Esc stops following |
| `datui --hive "/data/events/**/*.parquet"` | A glob, read as one partitioned table |
| `datui --format csv --no-header raw.txt` | Headerless text, whatever the extension |
| `datui --format acme.l2feed capture.bin` | A binary file, read through a format spec (or --format ./acme.toml) |
| `datui formats check acme.l2feed capture.bin` | Check a format spec and print the first rows it reads |
| `datui -c display.row_numbers=true data.csv` | Any config key, for this run |
| `datui config init` | Write ~/.config/datui/config.toml |
