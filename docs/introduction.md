# Datui

Datui is a terminal UI for looking at tabular data: Parquet, CSV, JSON, Arrow
and more, on disk or in S3, GCS and HTTP, from a few rows to a few billion.

![Overview Demo](demos/11-overview.gif)

```bash
datui data.parquet                    # open a file
datui --hive s3://bucket/warehouse/   # or a partitioned dataset in the cloud
datui                                 # or pick one from the home screen
```

Press <kbd>?</kbd> for the keys. <kbd>/</kbd> queries, <kbd>s</kbd> sorts and
filters, <kbd>c</kbd> charts, <kbd>a</kbd> analyzes, <kbd>q</kbd> quits.

## Where to go

| I want to... | Read |
|---|---|
| Install it | [Installation](getting-started/installation.md) |
| Learn the basics in five minutes | [Quick Start](getting-started/quick-start.md) |
| See every key | [Keyboard Shortcuts](reference/keyboard-shortcuts.md) |
| Write a query | [Querying Data](user-guide/querying-data.md), [Query Syntax](reference/query-syntax.md) |
| Open something in S3, GCS or over HTTP | [Loading Data](user-guide/loading-data.md#remote-data) |
| View a Polars frame from Python | [Python Module](user-guide/python-module.md) |
| Change colors or defaults | [Configuration](user-guide/configuration.md) |
| Watch it work | [Demos](demos.md) |
| Build or contribute | [For Developers](for-developers.md) |

## What it does

- **Opens the file where it is.** Local paths, `s3://` and `gs://` URLs, and
  hive-partitioned directories; HTTP URLs are fetched first. Parquet is read
  lazily through [Polars](https://pola.rs), one row group at a time, so a
  dataset larger than memory scrolls like a small one.
- **Shows you what is around you.** The [home screen](user-guide/home-screen.md)
  lists recent datasets, the current directory, your data directories and the
  buckets your credentials reach, with rows, columns and size before you open
  anything.
- **Answers questions.** SQL, a short query language
  (`select a, b where c > 10 by region`), and fuzzy text search across every
  column. Sort, filter, freeze and hide columns from a sidebar.
- **Summarizes.** Describe, distribution fitting with Q-Q plots, and a
  correlation matrix, computed on the data as filtered.
- **Draws.** Line, scatter, bar, histogram, box, KDE and heatmap charts in the
  terminal, exportable as PNG or EPS.
- **Reshapes and saves.** Pivot and melt, export to CSV, Parquet, JSON, NDJSON,
  Arrow or Avro, and templates that replay a query, filters and sort on the
  next dataset with the same shape.
- **Configurable.** Light and dark palettes with every color a config key,
  defaults for every option, and arrow keys or `h` `j` `k` `l`.

Datui is open source under the MIT license. Source, issues and releases are on
[GitHub](https://github.com/derekwisong/datui).

These pages describe one release; [other versions](../) are also published.
