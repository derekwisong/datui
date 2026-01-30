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
| `--no-header <NO_HEADER>` | Specify that the file has no header |
| `--delimiter <DELIMITER>` | Specify the delimiter to use when reading a file |
| `--compression <COMPRESSION>` | Specify the compression format explicitly (gzip, zstd, bzip2, xz) If not specified, compression is auto-detected from file extension. Supported formats: gzip (.gz), zstd (.zst), bzip2 (.bz2), xz (.xz) |
| `--debug` | Enable debug mode to show operational information |
| `--hive` | Enable Hive-style partitioning for directory or glob paths; ignored for a single file |
| `--clear-cache` | Clear all cache data and exit |
| `--template <TEMPLATE>` | Apply a template by name when starting the application |
| `--remove-templates` | Remove all templates and exit |
| `--pages-lookahead <PAGES_LOOKAHEAD>` | Number of pages to buffer ahead of the visible area (default: 3) Larger values provide smoother scrolling but use more memory |
| `--pages-lookback <PAGES_LOOKBACK>` | Number of pages to buffer behind the visible area (default: 3) Larger values provide smoother scrolling but use more memory |
| `--row-numbers` | Display row numbers on the left side of the table |
| `--row-start-index <ROW_START_INDEX>` | Starting index for row numbers (default: 1) |
| `--generate-config` | Generate default configuration file at ~/.config/datui/config.toml |
| `--force` | Force overwrite existing config file when using --generate-config |
