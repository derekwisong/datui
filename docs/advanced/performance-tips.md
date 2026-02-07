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

## Pivot is Eager

In order to determine all column names, pivot operations materialize all affected data in memory, which may increase
RAM usage significantly for large tables.

Do as much filtering on the data as possible before pivoting to keep things manageable.

## Prefer Directories with `--hive`

Using a directory with `--hive` is faster than a glob.

e.g. `/path/to/partitioned/` would be faster than `/path/to/partitioned/**/*.parquet`.
