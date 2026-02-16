# Performance Tips

## Sampling in Analysis Mode

When you use Datui's [Analysis Mode](../user-guide/analysis-features.md), you can optionally have the application
sample from your data rather than analyzing every row. Sampling improves responsiveness and keeps memory usage
low when working with very large datasets.

**By default, sampling is off:** analysis uses the full dataset. To enable sampling for large tables, set a
threshold in configuration or on the command line. When enabled, datasets with at least that many rows are
analyzed using a representative sample; the **r** key resamples and the tool shows "(sampled)".

- **Configuration:** In `[performance]`, set `sampling_threshold = N` (e.g. `10000`). Omit the setting or leave it unset to keep full-dataset analysis (default).
- **CLI:** Use `--sampling-threshold N` to enable sampling for that run; this overrides the config file. Use `--sampling-threshold 0` to force full-dataset analysis for that run even if config sets a threshold.

See the [Configuration Guide: Performance Settings](../user-guide/configuration.md#performance-settings) for details.

## Pivot materializes the current view

Pivot needs the full result set in memory to compute new column names and perform the reshape. The entire current view—whatever is on screen after your filters, query, and source (e.g. a single CSV, local Parquet, or partitioned Parquet on S3)—is collected before pivoting. Large views mean high RAM use and possibly slow runs.

**Tip:** Filter or narrow the data (fewer rows/columns) before pivoting so the materialized set stays manageable.

## Prefer Directories with `--hive`

Using a directory with `--hive` is faster than a glob.

e.g. `/path/to/partitioned/` would be faster than `/path/to/partitioned/**/*.parquet`.
