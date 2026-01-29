# Loading Data

Load data with Datui by passing it [command line options](../reference/command-line-options.md)
and a path to open.

## Supported Formats

- Parquet
- CSV (or other delimited text formats)
- NDJSON (`.jsonl` files)
- JSON

## Compression

Compressed files will be identified by extension and decompressed before loading.

> Command line options may be used to set the compression used when there is no extension
> or it is incorrect

### Supported Compression Formats

- gz
- bzip2
- xz

## Hive-partitioned data

You can load a Hive-style partitioned dataset (e.g. a directory tree with `key=value` segment names such as `year=2024/month=01/`) by using the `--hive` flag and passing a **directory** or a **glob pattern** instead of a single file.

- **Directory**: point at the partition root, e.g. `datui --hive /path/to/data`
- **Glob**: use a pattern that matches the partition layout, e.g. `datui --hive /path/to/data/**/*.parquet`  
  You may need to quote the glob so your shell does not expand it (e.g. `datui --hive "/path/to/data/**/*.parquet"`).

Only Parquet is supported for hive-partitioned loading. If you pass a single file with `--hive`, it is loaded as usual and the flag is ignored.

Partition columns (the keys from the path, e.g. `year`, `month`) are shown first in the table and listed in the Info panel under the **Partitioned data** tab.
