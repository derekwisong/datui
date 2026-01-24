# Performance Tips

## Sampling in Analysis Mode

When you use Datui's [Analysis Mode](../user-guide/analysis-features.md), the application may automatically
sample from your data rather than analyzing every row. Sampling is used to improve responsiveness and keep
memory usage low when working with very large datasets.

By default, if your table contains more rows than a set threshold, Datui will analyze a representative sample
instead of the full dataset. This threshold can be adjusted in the configuration file. To learn how to change the
sampling limit, see the
[Configuration Guide: Analysis & Performance Settings](../user-guide/configuration.md#performance-settings).

## Pivot is Eager

In order to determine all column names, pivot operations materialize all affected data in memory, which may increase
RAM usage significantly for large tables.

Do as much filtering on the data as possible before pivoting to keep things manageable.
