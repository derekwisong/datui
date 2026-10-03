# Large datasets

Opening a file and calculating over it have different costs. Datui keeps a
buffer of visible rows, but a query, sort, aggregation or analysis may scan
the full input. Lazy loading does not make every operation fit in memory.
[Performance](../reference/performance.md) has measured startup times and memory.

| Task | What helps |
|---|---|
| Page through a large dataset | Prefer Parquet: types and row counts are stored in footers, and data is read in row groups |
| Open local partitions | Pass the directory, such as `datui ./events/`, to use datui's schema union and file metadata |
| Pivot a large table | Filter first; pivot reads all affected rows to discover the output columns |
| Analyze many rows | Use the sample-size setting for Describe, Distribution and Correlation |
| Chart many rows | Charts sample 10,000 rows across the table; raise **Sample size** for more |
| Open compressed CSV, TSV or PSV | Put `--temp-dir` on a disk with room for the uncompressed file |

## Sample deliberately

Describe, Distribution and Correlation use up to 100,000 rows by default.
For a larger table, the sample is spread across the dataset. An unfiltered
Parquet or IPC file uses selected ranges; other views stream the data while
keeping a bounded sample. See [sampling](../user-guide/analysis-features.md#sampling).

```bash
datui --sample-rows 50000 https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

Use `--sample-rows 0` to analyze every row, or press <kbd>a</kbd> on a sampled
result and confirm. <kbd>Esc</kbd> cancels a run.
[Data Quality](../user-guide/data-quality.md) reads the same sample.

Charts read the same kind of sample, 10,000 rows by default (**Sample size**
in the chart view). A sampled line chart of a long time series is sparse;
aggregate the period first to chart every step of it.

## Know what gets read

- Parquet, CSV and Arrow IPC use scans. JSON arrays, NDJSON, Avro, Excel and
  ORC are loaded in full; a bucket prefix of NDJSON, and a followed NDJSON file,
  are scanned. Past `memory_warning` in `[read]` (`"1GiB"` by
  default), datui asks before loading a file in full. NMEA and GPX logs, VCD dumps, FIX logs and SDF files are
  read once into a temporary Arrow file, then scanned. SQLite tables are read in
  place, a page at a time, with the sidebar's sort and filters run in SQLite; an
  index on the column makes them fast. See
  [formats](../formats/index.md#how-each-format-is-read).
- A Parquet directory reads file footers to combine schemas and count rows.
  Beyond 64 files it opens before the background footer pass finishes, and
  beyond 20,000 the pass uses sampled footers. See
  [multi-file loading](large-datasets.md#how-large-datasets-open).
- Parquet in object storage uses range reads. Supported remote CSV and JSONL
  directories scan in place; HTTP and other download routes fetch the file
  first. See [remote data](../user-guide/remote-data.md).

Leave `[performance] streaming` on unless you are diagnosing a problem. It lets
supported operations process data in batches; it is not a memory bound on
all queries.

Use <kbd>i</kbd> → **Resources** to inspect the buffer and loading measurements,
and **Notes** for layouts with large row groups or many small files.

## How large datasets open

Directories with more than 64 Parquet files, local or in the cloud, open using
the first and last files by name, then read the remaining footers in the
background. New columns join the end of the table, and the total row count
arrives, when they land. Until then:

- The total row count is unavailable and empty cells display as `∅`.
- Notes state the partial metadata scope.
- A query, pivot or drill-down defers the new columns until you return to the original data.

Above 20,000 files the background pass reads a sample, and the row count reads
the footers the sample skipped. Once every footer is read, the dataset is cached
and a reopen reads none. The control bar reports footer-reading progress.

While a directory or prefix is listed, the loading screen counts the files
found: `Listing files: 412,000`. <kbd>Ctrl</kbd>+<kbd>O</kbd> stops the listing.
A large S3 or Google Cloud prefix is listed in parallel key ranges.

Partition columns come from the listing, the same way for a local directory and a
cloud prefix. The newest file's path names the columns. The first and newest
files' values set each column's type.

The Notes tab also flags storage layouts that may explain a slow open:

| Finding | Why it matters |
|---|---|
| Median row-group size above 64 MiB | Reading a page may require a large row group |
| More than 10,000 files, median size below 1 MiB | Many metadata reads before data can be displayed |
| Different partition keys, such as `date` and `dt` | Partition columns vary across the dataset; key order alone is fine |

`-c read.parquet_schema=false` skips datui's footer union and uses
Polars' single-file schema inference. That route also omits the partition-key check.

## Opening it again

Schema metadata of a remote dataset, or of a local directory of more than 64
files, is cached by URL or path. Datui still lists the files to check for
changes to names, sizes, timestamps or etags. An unchanged listing opens with
no footer reads; a changed one triggers fresh metadata reads. The cache keeps
128 MiB of this metadata and drops the least recently opened dataset first.

`datui cache clear` clears this metadata along with other cached state, including
query history. See [cache contents](home-screen.md#what-datui-remembers).
