# Performance Tips

Datui works on a lazy Polars plan and reads only the rows on screen, so most
datasets are fast without any of this. When one is not:

**Prefer Parquet.** It is scanned lazily, row groups are skipped, and the
schema and row count come from the footer. JSON, Avro, Excel and ORC are read
whole before the table appears; see [Formats](../user-guide/loading-data.md#formats).

**Open a hive directory, not a glob.** `datui --hive /data/events/` is faster
than `datui --hive "/data/events/**/*.parquet"`, and the schema is read from one
file rather than all of them.

**Query before you pivot.** Pivot has to read every affected row to discover
the new column names. Filter or query down first.

**Sample the analysis.** Describe, distribution fitting and the correlation
matrix read every row by default. On a very large table:

```toml
[performance]
sampling_threshold = 1000000
```

or `--sampling-threshold 1000000` for one run. See
[Analysis](../user-guide/analysis-features.md#sampling).

**Cap chart rows.** Charts use at most `row_limit` rows (default 10,000), set
in [`[chart]`](../user-guide/configuration.md#charts) or with **Limit Rows** in
the chart view.

**Leave streaming on.** `--polars-streaming` (default `true`) lets Polars
process a collect in batches. Turn it off only to test whether it is the cause
of a problem.

**Compressed CSV.** A `.csv.gz` is decompressed to a temporary file so it can
still be scanned lazily. Point `--temp-dir` at a fast disk with room for the
uncompressed file.

**Cloud data.** Parquet in S3 or GCS is read in place, and a partitioned prefix
opens like a local hive directory. Other formats are downloaded whole first.
The [home screen](../user-guide/home-screen.md#cloud-storage) lists bucket
contents without reading any object.
