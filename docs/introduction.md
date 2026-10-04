# datui documentation

Open, query, chart and export tabular data in your terminal. datui reads local
files and cloud storage, and works with Polars frames in Python.

<!-- generated: format-count -->
datui reads 27 formats: Parquet, CSV, TSV, PSV, JSON, NDJSON, Arrow IPC, Avro, ORC, Excel, SafeTensors, GGUF, NMEA, GPX, WAV/AIFF audio, MIDI, SQLite, VCD, FIX, SDF, NumPy, ELF, ULog, DataFlash, candump, plain text, systemd journal, and binary formats you describe in a format spec.
<!-- end generated: format-count -->

**Start here:** [Install datui](getting-started/installation.md), then follow the
[quick start](getting-started/quick-start.md) with a small public dataset.

## Find a task

| I want to… | Go to |
|---|---|
| Open a file, a directory, a glob or a compressed export | [Open files and directories](user-guide/open-files.md) |
| Read a pipe, or follow a file as it grows | [Pipes and growing files](user-guide/pipes-and-follow.md) |
| Know how a format is read, or read my own binary format | [Formats](formats/index.md) · [Format specs](formats/format-specs.md) |
| Connect to S3, GCS, Azure or an HTTP URL | [Connect to cloud storage](user-guide/remote-data.md) |
| Find recent files or browse buckets | [Home screen](user-guide/home-screen.md) · [Cloud sources](user-guide/home-screen.md#cloud-sources) |
| Filter rows or write SQL | [Query data](user-guide/querying-data.md) · [Sort and filter](user-guide/filtering-sorting.md) |
| Make a chart or reshape a table | [Make a chart](user-guide/charting.md) · [Pivot and melt](user-guide/reshaping.md) |
| Check missing values or schema changes | [Check data quality](user-guide/data-quality.md) · [Info panel](user-guide/dataset-info.md) |
| Copy, export, or reuse a result | [Copy](user-guide/copying.md) · [Export](user-guide/exporting-data.md) · [Views](user-guide/views.md) |
| Explore a DataFrame in Python | [Use datui from Python](user-guide/python-module.md) · [Python API](reference/python-api.md) |
| Change defaults or colors | [Configure datui](user-guide/configuration.md) · [Settings](reference/settings.md) |
| Look up a key, expression, flag or variable | [Keys](reference/keyboard-shortcuts.md) · [Query syntax](reference/query-syntax.md) · [Options](reference/command-line-options.md) · [Environment](reference/environment.md) |
| Fix a problem | [Troubleshooting](reference/troubleshooting.md) |
| Build or contribute | [Development overview](for-developers.md) |

## Help while you work

Press <kbd>?</kbd> for the current screen's keys. The footer says what is in
effect, and the keys of whatever mode is active. <kbd>Esc</kbd> backs out; <kbd>Ctrl</kbd>+<kbd>Q</kbd> quits.
The search button above searches this manual.

[Choose another version](https://derekwisong.github.io/datui/#versions), [watch the demos](demos.md), or
[report a problem](https://github.com/derekwisong/datui/issues).
