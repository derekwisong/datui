# Performance tips

Opening a file and calculating over it have different costs. Datui keeps a
buffer of visible rows, but a query, sort, aggregation or analysis may scan
the full input. Lazy loading does not make every operation fit in memory.

| Task | What helps |
|---|---|
| Browse a large dataset | Prefer Parquet: types and row counts are stored in footers, and data is read in row groups |
| Open local partitions | Pass the directory, such as `datui ./events/`, to use datui's schema union and file metadata |
| Pivot a large table | Filter first; pivot reads all affected rows to discover the output columns |
| Analyze many rows | Use the sample-size setting for Describe, Distribution and Correlation |
| Chart many rows | Filter or aggregate first, then check **Limit Rows**; the default is 10,000 |
| Open compressed CSV | Put `--temp-dir` on a disk with room for the uncompressed file |

## Sample deliberately

Describe, Distribution and Correlation use up to 100,000 rows by default.
For a larger table, the sample is spread across the dataset. An unfiltered
Parquet or IPC file uses selected ranges; other views stream the data while
keeping a bounded sample. See [sampling](../user-guide/analysis-features.md#sampling).

```bash
datui --sample-rows 50000 events.parquet
```

Use `--sample-rows 0` to analyze every row, or press <kbd>a</kbd> on a sampled
result and confirm. <kbd>Esc</kbd> cancels a run.
[Data Quality](../user-guide/data-quality.md) reads the same sample.

Chart **Limit Rows** caps input rows; it does not produce a representative
random sample. For a time series, aggregate the whole period before charting
if you want the whole period represented.

## Know what gets read

- Parquet, CSV, JSONL and Arrow IPC use scans. JSON arrays, Avro, Excel and ORC
  are loaded in full. See [formats](../user-guide/loading-data.md#formats).
- A local Parquet directory normally reads file footers to combine schemas.
  Beyond 20,000 files the schema uses sampled footers. Large cloud directories
  can open before the background footer pass finishes. See
  [multi-file loading](../user-guide/loading-data.md#how-large-remote-datasets-open).
- Parquet in object storage uses range reads. Supported remote CSV and JSONL
  directories scan in place; HTTP and other download routes fetch the file
  first. See [remote data](../user-guide/remote-data.md).

Leave `--polars-streaming` enabled unless you are diagnosing a problem. It lets
supported operations process data in batches; it is not a memory bound on
all queries.

Use <kbd>i</kbd> → **Resources** to inspect the buffer and loading measurements,
and **Notes** for layouts with large row groups or many small files.
