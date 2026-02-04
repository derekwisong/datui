# Loading Data

Load data with Datui by passing it [command line options](../reference/command-line-options.md)
and a path to open.

## Supported Formats

| Format | Extensions | Eager load only | Hive partitioning |
|--------|------------|-----------------|-------------------|
| Parquet | `.parquet` | No | Yes |
| CSV (or other-delimited) | `.csv`, `.tsv`, `.psv`, etc. | No | No |
| NDJSON | `.jsonl` | No | No |
| JSON | `.json` | Yes | No |
| Arrow IPC / Feather v2 | `.arrow`, `.ipc`, `.feather` | No | No |
| Avro | `.avro` | Yes | No |
| Excel | `.xls`, `.xlsx`, `.xlsm`, `.xlsb` | Yes | No |
| ORC | `.orc` | Yes | No |

**Eager load only** — The file is read fully into memory before use; no lazy streaming. **Hive partitioning** — Use the `--hive` flag with a directory or glob; see [Hive-partitioned data](#hive-partitioned-data) below.
**Excel** — Use the `--sheet` flag to specify which sheet to open.

**CSV date inference** — By default, CSV string columns that look like dates (e.g. `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`) are parsed as Polars Date/Datetime. Use `--parse-dates false` or set `parse_dates = false` in [configuration](configuration.md) to disable.

## Compression

Compressed files are identified by extension and decompressed before loading. Use the `--compression` option to specify the format when the file has no extension or the extension is wrong.

### Supported Compression Formats

- gz
- zstd
- bzip2
- xz

## Hive-partitioned data

You can load a Hive-style partitioned dataset (e.g. a directory tree with `key=value` segment names such as `year=2024/month=01/`) by using the `--hive` flag and passing a **directory** or a **glob pattern** instead of a single file.

- **Directory**: point at the partition root, e.g. `datui --hive /path/to/data`
- **Glob**: use a pattern that matches the partition layout, e.g. `datui --hive /path/to/data/**/*.parquet`  
  You may need to quote the glob so your shell does not expand it (e.g. `datui --hive "/path/to/data/**/*.parquet"`).

Only Parquet is supported for hive-partitioned loading. If you pass a single file with `--hive`, it is loaded as usual and the flag is ignored.

Partition columns (the keys from the path, e.g. `year`, `month`) are shown first in the table and listed in the Info panel under the **Partitioned data** tab.
