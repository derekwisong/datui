# Command Line Options

## Usage

```
Usage: datui [OPTIONS] [PATH]...
```

## Options

| Option | Description |
|--------|-------------|
| `[<PATH>]` | Path(s) to the data file(s) to open. Multiple files of the same format are concatenated into one table. With no PATH, datui opens its home screen so you can pick a dataset |
| `--skip-lines <N>` | Skip this many raw lines at the start of the file, split on newlines alone. Not quote-aware: a newline inside a quoted field counts. Compare --skip-rows |
| `--skip-rows <N>` | Skip this many CSV rows at the start of the file; the header is read after them. Quote-aware: a row with embedded newlines counts once. Compare --skip-lines |
| `--skip-tail-rows <N>` | Skip this many rows at the end of the file, such as a vendor footer or trailing garbage. Needs the row count first, which reads the whole file; on a directory in a bucket, every file |
| `--no-header[=<BOOL>]` | Read the first row as data, not column names; columns are named column_1, column_2, … |
| `--delimiter <CODE>` | Column separator for a delimited text file, as an ASCII code (9 for tab). Default: `,` for .csv, tab for .tsv, `\|` for .psv |
| `--infer-schema-length <N>` | Number of rows to use when inferring CSV schema (default: 1000). Larger values reduce the risk of a wrong type (e.g. int then N/A) |
| `--ignore-errors[=<BOOL>]` | When reading CSV, ignore parse errors and continue with the next batch (default: false) |
| `--null-value <VAL>` | Treat these values as null when reading CSV. Use once per value; no "=" means all columns, COL=VAL means column COL only (first "=" separates column from value). Example: --null-value NA --null-value amount= |
| `--compression <COMPRESSION>` | Compression format, when the extension does not say (default: auto-detected from the extension) |
| `--format <FORMAT>` | File format, for a URL or a path whose extension does not say (default: auto-detected from the extension) |
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
| `--number-format <FORMAT>` | Digit grouping for numbers in the table (default: none). "system" reads LC_ALL/LC_NUMERIC/LANG. Press F to toggle while running |
| `--align-numeric-right[=<BOOL>]` | Right-align numeric columns and their headers (default: true) |
| `--generate-config` | Write the default configuration to ~/.config/datui/config.toml and exit |
| `--force` | Overwrite an existing config file (with --generate-config) |
| `--s3-endpoint-url <URL>` | S3-compatible endpoint URL (overrides config and AWS_ENDPOINT_URL). Example: http://localhost:9000 |
| `--s3-access-key-id <KEY>` | S3 access key (overrides config and AWS_ACCESS_KEY_ID) |
| `--s3-secret-access-key <SECRET>` | S3 secret key (overrides config and AWS_SECRET_ACCESS_KEY) |
| `--s3-region <REGION>` | S3 region (overrides config and AWS_REGION). Example: us-east-1 |
| `--cloud-discover <WHICH>` | Which cloud logins found on this machine appear on the home screen: all, none, or kinds separated by commas (s3, gcs, azure). Overrides [cloud] discover. Entries in [[cloud.connections]] always appear |

## Examples

| Command | Does |
|---------|------|
| `datui` | Open the home screen. Public datasets lists the built-in catalog |
| `datui https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv` | Palmer penguins from the web; datui asks before it downloads |
| `datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/` | NOAA daily highs for 2024, one table from public S3 |
| `datui abfss://release@overturemapswestus2.dfs.core.windows.net/` | Browse Overture Maps releases in public Azure storage |
| `datui jan.csv feb.csv mar.csv` | Files of the same shape, as one table |
| `datui --hive "/data/events/**/*.parquet"` | A glob, read as one partitioned table |
| `datui --format csv --no-header raw.txt` | Headerless text, whatever the extension |
| `datui --generate-config` | Write ~/.config/datui/config.toml |
