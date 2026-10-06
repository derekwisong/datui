# Sample a table

Draw a sample of a large table into memory and work on it: the table, the query,
Analysis, charts and export all read the sample.

<kbd>S</kbd> at the table opens the **Sample** form; <kbd>Enter</kbd> draws it.

```bash,network
datui -c analysis.sample_memory_limit=8GiB https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

| Key | Does |
|---|---|
| <kbd>S</kbd> | The Sample form, on the view's sample or a new one |
| <kbd>Enter</kbd> | Draw the sample; again past a memory warning, to draw anyway |
| <kbd>Esc</kbd> in the form | Close it; the view's sample stays as it was |
| <kbd>Esc</kbd> while it is drawn | Stop; the rows so far stay. Before the first rows come, the view stays as it was |
| **Method** → **No sample**, or <kbd>R</kbd> | Take the sample away |

The form's settings (which rows, the method, the size, the seed) are the
[analysis sample's](analysis-features.md#sampling). **Every row** reads
**No sample** here.

## A step of the view

The sample sits between the source and the query: source, then sample, then
the query, filters and sort, which run over the sample's rows. The footer says
it in that order:

```text
yellow_tripdata_2025-01.parquet › sample 100,000 of 3.48M › query   1 / 41,208
```

| Footer | Means |
|---|---|
| `sample 100,000 of 3.48M` | 100,000 rows of the 3.48M the scope holds |
| `sample about 100,000 of 3.48M` | Kept row by row by chance: about the size asked for |
| `sample 1,234+` | Still being drawn |
| `sample 58,700 of 3.48M, stopped` | Stopped before its end: by <kbd>Esc</kbd>, or by memory |
| `query › sample 2,000 of 41,208` | Drawn from the query's rows: <kbd>S</kbd> on a queried view |

To sample part of a table, choose it under **Rows from** (a row range,
partitions, files, a time range), or query first and press <kbd>S</kbd> on the
queried view. Taking the sample away keeps the query, filters and sort laid on
it, over the source again; a sample drawn from a query's rows returns to that
query.

A pivot is read whole, so no sample is drawn under one: sample the pivoted view
(**Rows from** **All rows**) instead. A sample with a pivot laid on it is taken
away with <kbd>R</kbd>, which takes the pivot too.

The same seed keeps the same rows. A random sample of a stream is kept row by
row when the total is known and in a reservoir when it is not; drawn again, in
the session or from a [view](views.md), it is drawn the way it was first.

## Rows as they arrive

The table shows the rows while the sample is drawn, the view staying where it
is, as a [pipe](pipes-and-follow.md) does. Until the first rows come, the view
it replaces stays; a draw that fails or stops before then leaves it as it was. Rows show in the order they arrive,
then in the order the source holds them once the sample ends.

| Read | Rows show |
|---|---|
| One Parquet or IPC file | Each of the 50 seeded runs as it lands |
| First rows, every row | Each batch as it is read |
| Random over a stream, the total known | Each row kept with chance size ÷ total, as it is read: about the size asked for |
| Random over a stream, the total unknown | All at the end; the footer counts the rows read meanwhile |
| Equal per value | All at the end |

While it is drawn, moving, [find](finding.md), the inspector and the Info panel
act at once. Anything that needs every row (a sort, a query, Analysis, a chart,
an export) waits until it is drawn.

## What reads it

| Where | Reads |
|---|---|
| [Analysis](analysis-features.md) | The sample, whole; <kbd>s</kbd> there edits the view's sample |
| [Charts](charting.md) | The sample, whole; the chart's **Rows** row goes, and its note names the sample and its seed |
| [Export](exporting-data.md) | The sample's rows, with the query, filters and sort: the way to keep a sample on disk |
| [Views](views.md) | The sample's settings, never its rows: applied again, it draws the same rows from the seed |

## Memory

The sample is held in memory and never written to disk. The form's size line
shows the cost when the table has measured its rows: `100,000 rows · ~380.0 MiB`.

| When | datui |
|---|---|
| The estimate is more than the memory available now | Warns on the form, naming the setting; <kbd>Enter</kbd> again draws anyway, with no running check |
| Memory runs low while it is drawn | Stops, keeps the rows so far, and says so: `Sample stopped at 3.9 GiB (58,700 rows): memory ran low`. A sample that keeps its rows to the end (an unknown total, equal per value) stops when what it holds could not fit twice, keeping those |
| There is no estimate | Draws, the running check as the backstop |

[`analysis.sample_memory_limit`](../reference/settings.md#analysis) sets a
fixed ceiling, checked before and while it is drawn; unset, the memory
available now decides; `0` turns off the warning and the stop:

```toml
[analysis]
sample_memory_limit = "8GiB"
```
