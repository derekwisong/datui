# Performance tips

Opening a file and calculating over it have different costs. Datui keeps a
buffer of visible rows, but a query, sort, aggregation or analysis may scan
the full input. Lazy loading does not make every operation fit in memory.
[Performance](../reference/performance.md) has measured startup times and memory.

| Task | What helps |
|---|---|
| Browse a large dataset | Prefer Parquet: types and row counts are stored in footers, and data is read in row groups |
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
  are scanned. Past `memory_warning_mb` in `[file_loading]` (1024 MB by
  default), datui asks before loading a file in full. NMEA and GPX logs, VCD dumps, FIX logs and SDF files are
  read once into a temporary Arrow file, then scanned. SQLite tables are read in
  place, a page at a time, with the sidebar's sort and filters run in SQLite; an
  index on the column makes them fast. See
  [formats](../user-guide/loading-data.md#formats).
- A Parquet directory reads file footers to combine schemas and count rows.
  Beyond 64 files it opens before the background footer pass finishes, and
  beyond 20,000 the pass uses sampled footers. See
  [multi-file loading](../user-guide/loading-data.md#how-large-datasets-open).
- Parquet in object storage uses range reads. Supported remote CSV and JSONL
  directories scan in place; HTTP and other download routes fetch the file
  first. See [remote data](../user-guide/remote-data.md).

Leave `[performance] polars_streaming` on unless you are diagnosing a problem. It lets
supported operations process data in batches; it is not a memory bound on
all queries.

Use <kbd>i</kbd> → **Resources** to inspect the buffer and loading measurements,
and **Notes** for layouts with large row groups or many small files.
