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
