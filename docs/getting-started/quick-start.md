# Quick Start

Five minutes from install to your first chart. Not installed yet? See
[Installation](installation.md).

## Open something

```bash
datui data.parquet                          # one file
datui part-1.csv part-2.csv                 # several files of the same shape, as one table
datui /data/events/                         # a folder: read as one table, or browsed into
datui s3://bucket/events/                   # the same, in S3 (also gs:// and https://)
datui                                       # no path: the home screen
```

With no path, the [home screen](../user-guide/home-screen.md) lists recent
datasets, the current directory, your configured data directories and the
buckets your credentials reach. Type to filter, <kbd>Enter</kbd> to open.
<kbd>Ctrl</kbd>+<kbd>O</kbd> brings you back to it from anywhere.

From Python:

```python
import polars as pl
import datui

datui.view(pl.scan_parquet("data.parquet"))
```

## Move around

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>j</kbd> <kbd>k</kbd> <kbd>l</kbd> | Move |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A page at a time |
| <kbd>Home</kbd> <kbd>End</kbd> | First and last row |
| <kbd>:</kbd> | Go to a row number |
| <kbd>?</kbd> | Help for the screen you are on |
| <kbd>Esc</kbd> | Close whatever is open |
| <kbd>q</kbd> | Quit |

The bottom bar always shows the keys that matter on the current screen.

## Ask a question

Press <kbd>/</kbd> and type a query:

```
select name, city, salary where salary > 100000 by department
```

<kbd>Enter</kbd> runs it. A `by` clause groups; press <kbd>Enter</kbd> on a
group to drill into it and <kbd>Esc</kbd> to come back. The same prompt has a
**SQL** tab (`SELECT * FROM df WHERE ...`) and a **Fuzzy** tab that matches text
in any column. See [Querying Data](../user-guide/querying-data.md).

## Sort, filter, chart, analyze

| Key | Opens |
|---|---|
| <kbd>s</kbd> | Sort and filter: order, freeze and hide columns, add row filters |
| <kbd>c</kbd> | Charts: line, scatter, bar, histogram, box, KDE, heatmap |
| <kbd>a</kbd> | Analysis: describe, distribution fitting, correlation matrix |
| <kbd>p</kbd> | Pivot and melt |
| <kbd>e</kbd> | Export the current view to a file |
| <kbd>i</kbd> | Schema and file details |
| <kbd>R</kbd> | Reset: clear the query, filters and sort |

Everything works on the data as you currently see it. Filter first, then chart,
analyze or export the result.

## Next

- [Keyboard Shortcuts](../reference/keyboard-shortcuts.md) lists every key on every screen.
- [Loading Data](../user-guide/loading-data.md) covers formats, compression, CSV options and cloud credentials.
- [Configuration](../user-guide/configuration.md) sets defaults and colors. Start with `datui --generate-config`.
