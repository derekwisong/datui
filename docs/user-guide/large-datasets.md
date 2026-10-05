# Large datasets

datui reads only the rows on screen to show a table, but a query, sort,
aggregation or analysis may read the whole input.

```bash,network
datui --sample-rows 50000 https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

<a id="sample-deliberately"></a>
<a id="know-what-gets-read"></a>

| To | Do |
|---|---|
| Page quickly through a large dataset | Prefer Parquet: its footers hold the types and row counts, and it is read a row group at a time |
| Open local partitions | Pass the directory (`datui events/`), so datui combines the footers and counts the rows |
| Analyze many rows | Analyses read a [sample](analysis-features.md#sampling), 100,000 rows by default; `--sample-rows N` changes it, `0` reads every row |
| Work on part of a large table | <kbd>S</kbd> draws a [sample](sampling.md) into memory: the query, Analysis, charts and export run on it, and export saves it |
| Chart many rows | Charts read `[analysis] chart_rows` rows, 10,000 by default, spread across the table; aggregate a long time series first to chart every step |
| Pivot a large table | Filter first: a pivot reads every row it covers to find its columns |
| Open compressed CSV, TSV or PSV | Put `--temp-dir` on a disk with room for the uncompressed file |
| See what a format reads | The [formats table](../formats/index.md#how-each-format-is-read): lazy scan, decompressed copy, converted to Arrow, or in memory. Past `[read] memory_warning` (1 GiB by default), datui asks before reading a file into memory |
| See what was read | <kbd>i</kbd> → **Resources** for the buffer and the loading measurements; **Notes** for row groups and small files |

`[performance] streaming` (on by default) runs what Polars can in batches; it
is not a memory limit on every query. [Performance](../reference/performance.md)
has measured times to first rows and memory.

## How large datasets open

A directory of more than 64 Parquet files, local or in the cloud, opens on the
first and last files by name and reads the other footers in the background;
the footer counts them on a line of its own. Until they are in:

- The total row count is estimated from a random sample of 2,000 footers, read
  first: `~4.12B (est.)` in the footer and on the Info panel.
- Every empty cell shows as `∅`.
- New columns join the end of the table as they are found; Notes says how
  much was read.
- A query, pivot or drill-down leaves the new columns out until you return to
  the data as opened.

Above 20,000 files the background pass reads a sample of the footers, and the
row count reads the rest: the footer shows `files 18,402 / 842,225` and
<kbd>Esc</kbd> stops it, leaving the estimate. Counts read 256 footers at once.

| Files | Row count |
|---|---|
| Up to 64 | Exact at once, from every footer |
| Up to 20,000 | Estimated, then exact when the background pass is done |
| Up to `read.exact_count_files` (50,000) | Estimated, then counted in the background |
| More | Estimated; <kbd>c</kbd> on the Info panel counts exactly, and so does <kbd>End</kbd> |

A count keeps each file's footer in the cache by its path, size, time and
etag. Counting the dataset again, after a stop, or after files were added,
reads only the footers it does not have.

[Value counts](value-counts.md) of a dataset of files say in the footer how
many of the files the read has reached.

While a directory or prefix is listed, the loading screen counts the files:
`Listing files: 412,000`; <kbd>Ctrl</kbd>+<kbd>O</kbd> stops it. A large S3 or
Google Cloud prefix is listed in parallel key ranges. Partition columns come
from the listing: the newest file's path names them, and the first and newest
files' values set their types.

The Notes tab flags layouts that make an open slow:

| Finding | Why it matters |
|---|---|
| Median row group above 64 MiB | A page may need a large row group read |
| More than 10,000 files, median below 1 MiB | Many footer reads before the first rows |
| Different partition keys, such as `date` and `dt` | The partition columns differ across the dataset; key order alone is fine |

`-c read.parquet_schema=first` skips datui's union of the footers and lets
Polars take one file's schema; the partition-key check is skipped too.

## Opening it again

The schema of a remote dataset, or of a local directory of more than 64
files, is cached by its URL or path. Opening it again lists the files; when no
name, size, time or etag changed, no footer is read. The home screen uses the
same cache to show a local directory's full row count without reading a
footer. The cache keeps 128 MiB
of schemas and drops the dataset opened longest ago first.
`datui cache clear` clears it with the rest of the
[cache](home-screen.md#what-datui-remembers).
