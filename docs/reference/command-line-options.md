# Command Line Options

## Usage

```
Usage: datui [OPTIONS] [PATH]... [COMMAND]
```

## Options

| Option | Description |
|--------|-------------|
| `[<PATH>]` | Path(s) to the data file(s) to open. Multiple files of the same format are concatenated into one table. `-` reads data piped to standard input, as does no PATH when something is piped in. With no PATH and nothing piped in, datui opens its home screen so you can pick a dataset |
| `-f, --follow` | Follow a file as it grows, as tail -f does: rows appended to a local CSV, TSV, PSV or NDJSON file or Arrow IPC stream, or still arriving on standard input (-), show as they land. t pauses and resumes; Esc stops |
| `--tee <FILE>` | Record standard input to FILE while viewing it: the bytes exactly as they arrive, in any format. Never replaces FILE without --force. A WAV file's sizes are filled in when the stream ends. With -, pass it on to standard output, as tee does, and draw on the terminal |
| `--tee-raw` | With --tee: leave FILE exactly as the bytes came, a WAV header's sizes included |
| `--skip-lines <N>` | Skip this many raw lines at the start of the file, split on newlines alone. Not quote-aware: a newline inside a quoted field counts. Compare --skip-rows |
| `--skip-rows <N>` | Skip this many CSV rows at the start of the file; the header is read after them. Quote-aware: a row with embedded newlines counts once. Compare --skip-lines |
| `--skip-tail-rows <N>` | Skip this many rows at the end of the file, such as a vendor footer or trailing garbage. Needs the row count first, which reads the whole file; on a directory in a bucket, every file |
| `--comment-char <C>` | Lines that start with this are comments and are skipped, before the header and among the data; the header is the first line that is not one. Frictionless `commentChar` |
| `--header-rows <N[,M...]>` | The line, or comma-separated lines, that hold the header, counted from 1 at the top of the file before anything is skipped. Several are joined per column with [file_loading] header_join (default a space); the data starts after the last. Frictionless `headerRows` |
| `--skip-initial-space[=<BOOL>]` | Ignore the spaces after a delimiter, so padded numbers read as numbers and a cell of spaces is null. Frictionless `skipInitialSpace` |
| `--no-header[=<BOOL>]` | Read the first row as data, not column names; columns are named column_1, column_2, … |
| `--delimiter <CODE>` | Column separator for a delimited text file, as an ASCII code (9 for tab). Default: `,` for .csv, tab for .tsv, `\|` for .psv |
| `--infer-schema-length <N>` | Number of rows to use when inferring CSV schema (default: 1000). Larger values reduce the risk of a wrong type (e.g. int then N/A) |
| `--ignore-errors[=<BOOL>]` | When reading CSV, ignore parse errors and continue with the next batch (default: false) |
| `--null-value <VAL>` | Treat these values as null when reading CSV. Use once per value; no "=" means all columns, COL=VAL means column COL only (first "=" separates column from value). Example: --null-value NA --null-value amount= |
| `--compression <COMPRESSION>` | Compression format, when the extension does not say (default: auto-detected from the extension) |
| `--format <FORMAT>` | File format, for a URL or a path whose extension does not say (default: auto-detected from the extension): parquet, csv, tsv, psv, json, jsonl, arrow, avro, orc, excel, safetensors, gguf, nmea, gpx, audio, midi, sqlite, vcd, fix, sdf, numpy, elf, ulog, dataflash, candump, or the name of a binary format spec such as acme.l2feed |
| `--spec <FILE>` | Read the file (or directory of column files) through this binary format spec, whatever else matches it. FILE may be an http(s), s3, gs or az URL; a spec is at most 1 MiB |
| `--dbc <FILE>` | Decode a candump log's frames with this DBC file too, over those on the format search path: a .dbc file, or TOML with kind = "dbc" |
| `--fix-dict <FILE>` | Read a FIX log with this dictionary too, over the built-in one and those on the format search path: a QuickFIX XML data dictionary, or TOML with kind = "fix" |
| `--variant <NAME>` | Read one variant of a binary format spec's records alone, as its own table: only its records, and only its columns |
| `--hex` | Show the file's bytes in the hex view, whatever it holds. A local file no reader and no spec takes opens there anyway |
| `--record-size <N>` | Bytes a row of the hex view holds, so that records line up (default: 8, 16, 32 or 64, as many as fit) |
| `--debug` | Enable debug mode to show operational information |
| `--log-file <PATH>` | Write the log here (default: [debug] log_file, or datui.log in the cache directory). DATUI_LOG sets the level: error, warn (default), info, debug or off |
| `--hive` | Read this as one partitioned table. Not needed for a directory, which datui reads the way Enter reads its row; use it for a glob, or to force partition columns on a layout that does not say so itself. Ignored for a single file |
| `--single-spine-schema[=<BOOL>]` | Combine Parquet file schemas from their footers (default: true). Set to false to use Polars' single-file schema inference |
| `--parse-dates[=<BOOL>]` | Parse CSV and JSON string columns that look like dates or ISO 8601 timestamps (e.g. 2024-01-31, 2024-01-31T10:00:00Z) as Date or Datetime (default: true) |
| `--parse-strings[=<COL>]` | Trim whitespace and parse CSV string columns as date, datetime, time, duration, int, or float (default: all string columns). --parse-strings=COL (repeatable) limits it to named columns; --no-parse-strings disables it |
| `--no-parse-strings` | Do not trim or type-infer CSV string columns. Overrides config and --parse-strings |
| `--decompress-in-memory[=<BOOL>]` | Decompress into memory (default: decompress to a temp file and scan lazily) |
| `--temp-dir <DIR>` | Directory for decompression temp files (default: system temp, e.g. TMPDIR) |
| `--sheet <SHEET>` | Excel sheet to load: 0-based index (e.g. 0) or sheet name (e.g. "Sales") |
| `--table <TABLE>` | Table to open from a file that holds several. NMEA: fixes (default), GGA, RMC, VTG, GSA, GSV, GLL, ZDA or sentences. SQLite: a table or view by name. NumPy: an array of an archive (.npz) by name. ELF: symbols (default) or sections. ULog: a topic. DataFlash: a message type. candump: frames (default), signals, or a message a DBC file names. Hugging Face cache and DatasetDict directories: a split (default train) |
| `--normalize` | Show integer audio samples as float in [-1, 1] (default: the integers as stored) |
| `--clear-recents` | Forget every recently opened dataset and exit; other caches are kept |
| `--clear-cache` | Clear all cache data and exit |
| `--template <NAME>` | Apply a saved view by name when starting the application |
| `--remove-templates` | Remove all saved views and exit |
| `--sample-rows <N>` | Rows an analysis samples from a larger table, spread across all of it (default: [performance] analysis_sample_rows, 100000). 0 reads every row |
| `--polars-streaming[=<BOOL>]` | Use the Polars streaming engine where available (default: true) |
| `--pages-lookahead <N>` | Pages to buffer ahead of the visible area (default: 3). More is smoother scrolling, more memory |
| `--pages-lookback <N>` | Pages to buffer behind the visible area (default: 3). More is smoother scrolling, more memory |
| `--row-numbers` | Show row numbers on the left side of the table. Press N to toggle while running |
| `--row-start-index <N>` | Starting index for row numbers (default: 1) |
| `--column-colors[=<BOOL>]` | Color table cells by column type (default: true) |
| `--number-format <FORMAT>` | Digit grouping for numbers in the table (default: none). "system" reads LC_ALL/LC_NUMERIC/LANG. Press , to toggle while running |
| `--align-numeric-right[=<BOOL>]` | Right-align numeric columns and their headers (default: true) |
| `--mouse[=<BOOL>]` | Take the mouse: wheel scrolls, click selects (default: true). --mouse=false leaves it to the terminal |
| `--generate-config` | Write the default configuration to ~/.config/datui/config.toml and exit |
| `--force` | Overwrite an existing file: the config file with --generate-config, or FILE with --tee |
| `--s3-endpoint-url <URL>` | S3-compatible endpoint URL (overrides config and AWS_ENDPOINT_URL). Example: http://localhost:9000 |
| `--s3-access-key-id <KEY>` | S3 access key (overrides config and AWS_ACCESS_KEY_ID) |
| `--s3-secret-access-key <SECRET>` | S3 secret key (overrides config and AWS_SECRET_ACCESS_KEY) |
| `--s3-region <REGION>` | S3 region (overrides config and AWS_REGION). Example: us-east-1 |
| `--cloud-discover <WHICH>` | Which cloud logins found on this machine appear on the home screen: all, none, or kinds separated by commas (s3, gcs, azure). Overrides [cloud] discover. Entries in [[cloud.connections]] always appear |

## Commands

| Command | Does |
|---------|------|
| `datui formats` | List the binary format specs and FIX dictionaries on the search path: each one's name, what it matches, the file it came from, and the copies it overrides |
| `datui formats check SPEC [FILE]` | Check a spec or FIX dictionary, by name or by file; with FILE, print its first decoded rows. Exits non-zero on an error |

## Examples

| Command | Does |
|---------|------|
| `datui` | Open the home screen. Public datasets lists the built-in catalog |
| `datui https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv` | Palmer penguins from the web; datui asks before it downloads |
| `datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/` | NOAA daily highs for 2024, one table from public S3 |
| `datui abfss://release@overturemapswestus2.dfs.core.windows.net/` | Browse Overture Maps releases in public Azure storage |
| `datui jan.csv feb.csv mar.csv` | Files of the same shape, as one table |
| `curl -s https://example.com/data.csv.gz \| datui` | Data piped in; the format is read from its first bytes |
| `serial-logger \| datui -f -` | Rows as they arrive; t pauses, Esc stops following |
| `datui --hive "/data/events/**/*.parquet"` | A glob, read as one partitioned table |
| `datui --format csv --no-header raw.txt` | Headerless text, whatever the extension |
| `datui --format acme.l2feed capture.bin` | A binary file, read through the format spec of that name |
| `datui formats check acme.l2feed capture.bin` | Check a format spec and print the first rows it reads |
| `datui --generate-config` | Write ~/.config/datui/config.toml |
