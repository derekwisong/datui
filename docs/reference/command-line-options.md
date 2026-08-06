# Command Line Options

## Usage

```
Usage: datui [OPTIONS] [PATH]...
```

## Options

| Option | Description |
|--------|-------------|
| `[<PATH>]` | Path(s) to the data file(s) to open. Multiple files of the same format are concatenated into one table (not required with --generate-config, --clear-cache, or --remove-templates) |
| `--skip-lines <SKIP_LINES>` | Skip this many lines when reading a file |
| `--skip-rows <SKIP_ROWS>` | Skip this many rows when reading a file |
| `--skip-tail-rows <N>` | Skip this many rows at the end of the file (e.g. to ignore vendor footer or trailing garbage) |
| `--no-header <NO_HEADER>` | Specify that the file has no header |
| `--delimiter <DELIMITER>` | Specify the delimiter to use when reading a delimited text file |
| `--infer-schema-length <N>` | Number of rows to use when inferring CSV schema (default: 1000). Larger values reduce risk of wrong type (e.g. int then N/A) |
| `--ignore-errors <BOOL>` | When reading CSV, ignore parse errors and continue with the next batch (default: false) |
| `--null-value <VAL>` | Treat these values as null when reading CSV. Use once per value; no "=" means all columns, COL=VAL means column COL only (first "=" separates column from value). Example: --null-value NA --null-value amount= |
| `--compression <COMPRESSION>` | Specify the compression format explicitly (gzip, zstd, bzip2, xz) If not specified, compression is auto-detected from file extension |
| `--format <FORMAT>` | Force file format (parquet, csv, tsv, psv, json, jsonl, arrow, avro, orc, excel). By default format is auto-detected from the file extension. Use this for URLs or paths without an extension |
| `--debug` | Enable debug mode to show operational information |
| `--hive` | Enable Hive-style partitioning for directory or glob paths; ignored for a single file |
| `--single-spine-schema <BOOL>` | Infer Hive/partitioned Parquet schema from one file for faster load (default: true). Set to false to use full schema scan |
| `--parse-dates <BOOL>` | Try to parse CSV string columns as dates (e.g. YYYY-MM-DD, ISO datetime). Default: true |
| `--parse-strings <COL>` | Trim whitespace and parse CSV string columns as date, datetime, time, duration, int, or float. Default: applied to all string columns. Use --parse-strings=COL (repeatable) to limit to specific columns, or --no-parse-strings to disable |
| `--no-parse-strings` | Disable parse-strings for CSV (trim and type inference). Overrides config and default |
| `--decompress-in-memory <DECOMPRESS_IN_MEMORY>` | Decompress into memory. Default: decompress to temp file and use lazy scan |
| `--temp-dir <DIR>` | Directory for decompression temp files (default: system temp, e.g. TMPDIR) |
| `--sheet <SHEET>` | Excel sheet to load: 0-based index (e.g. 0) or sheet name (e.g. "Sales") |
| `--clear-cache` | Clear all cache data and exit |
| `--template <TEMPLATE>` | Apply a template by name when starting the application |
| `--remove-templates` | Remove all templates and exit |
| `--sampling-threshold <N>` | When set, datasets with this many or more rows are sampled for analysis (faster, less memory). Overrides config [performance] sampling_threshold. Use 0 to disable sampling (full dataset) for this run. When omitted, config or full-dataset mode is used |
| `--polars-streaming <BOOL>` | Use Polars streaming engine for LazyFrame collect when available (default: true). Set to false to disable |
| `--workaround-pivot-date-index <BOOL>` | Apply workaround for Polars 0.52 pivot with Date/Datetime index (default: true). Set to false to test without it |
| `--pages-lookahead <PAGES_LOOKAHEAD>` | Number of pages to buffer ahead of the visible area (default: 3) Larger values provide smoother scrolling but use more memory |
| `--pages-lookback <PAGES_LOOKBACK>` | Number of pages to buffer behind the visible area (default: 3) Larger values provide smoother scrolling but use more memory |
| `--row-numbers` | Display row numbers on the left side of the table |
| `--row-start-index <ROW_START_INDEX>` | Starting index for row numbers (default: 1) |
| `--column-colors <BOOL>` | Colorize main table cells by column type (default: true). Set to false to disable |
| `--number-format <FORMAT>` | Digit grouping for numbers in the data table (default: none). Press F to toggle while running. "system" reads LC_ALL/LC_NUMERIC/LANG and picks a matching style |
| `--align-numeric-right <BOOL>` | Right-align numeric columns and their headers (default: true). Set to false to left-align |
| `--generate-config` | Generate default configuration file at ~/.config/datui/config.toml |
| `--force` | Force overwrite existing config file when using --generate-config |
| `--s3-endpoint-url <URL>` | S3-compatible endpoint URL (overrides config and AWS_ENDPOINT_URL). Example: http://localhost:9000 |
| `--s3-access-key-id <KEY>` | S3 access key (overrides config and AWS_ACCESS_KEY_ID) |
| `--s3-secret-access-key <SECRET>` | S3 secret key (overrides config and AWS_SECRET_ACCESS_KEY) |
| `--s3-region <REGION>` | S3 region (overrides config and AWS_REGION). Example: us-east-1 |
