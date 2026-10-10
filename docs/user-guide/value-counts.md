# Value counts

<kbd>F</kbd> at the table counts how many rows hold each value of the column
under the column cursor, with a summary of the column above the counts. A number column
opens as a histogram. <kbd>Esc</kbd> goes back.

Choose the column first with <kbd>h</kbd> <kbd>l</kbd>, or jump to it with
<kbd>g</kbd>. On the counts, <kbd>←</kbd> <kbd>→</kbd> move to the previous or
next column, and the table's column cursor moves with them.

## Which carrier flies most

Open **NYC flights (2013)** from **Example datasets** on the home screen, press
<kbd>g</kbd>, type `carrier`, <kbd>Enter</kbd>, then <kbd>F</kbd>:

```text
Value Counts · carrier · all 336,776 rows
 Rows 336776   Distinct 16   Nulls 0

 carrier   Count▼        %  Cum %
▎UA         58665    17.4%  17.4%   ████████████████████████████████████████████
 B6         54635    16.2%  33.6%   █████████████████████████████████████████
 EV         54173    16.1%  49.7%   ████████████████████████████████████████▋
 DL         48110    14.3%  64.0%   ████████████████████████████████████▏
```

Four carriers fly almost two thirds of the flights. <kbd>Enter</kbd> on `B6`
shows its 54,635 flights as a drill-down, `Group: carrier=B6`; <kbd>Esc</kbd>
comes back to the counts.

<kbd>→</kbd> moves to `flight`, a number column, which opens as a histogram
of its counts. Its summary adds `Sum`, `Mean`, `Min` and `Max`:

```text
Value Counts · flight · Histogram, 40 bins · all 336,776 rows
 Rows 336776   Distinct 3844   Nulls 0   Sum 664096549   Mean 1971.9236   Min 1
 Max 8500
```

<kbd>c</kbd> turns a number's histogram into the list of its values, and
back; the header says `List` then.

## Histogram

A number column's counts show as a histogram: 40 bins from the least value to
the greatest, or a bin per value when an integer column spans fewer than 40.
When the full range is more than ten times as wide as the 1st to 99th
percentile, the bins span that narrower range instead, and the values outside
it are counted: `1,207 values outside p1-p99`. The bins are built from the
counts, so they are exact whenever the counts are. <kbd>c</kbd> shows the list.
Another column opens in the form that suits its type.

## What the screen shows

| Part | What it says |
|---|---|
| Header | The column; for a number, `Histogram, 40 bins` or `List`; and what was counted: `all 336,776 rows`, or `sample of 100,000 of 657,752 rows` |
| Summary | `Rows`, `Distinct` (not counting null) and `Nulls`; `Sum`, `Mean`, `Min` and `Max` for numbers; `Min` and `Max` for dates and times. A sample has no `Sum` |
| Rows | Each value's row count, its percent of all rows, the running percent, and a bar scaled to the most common value's |
| `∅` | The nulls, on a row of their own: ranked by their rows when sorted by count, last when sorted by value |
| `other (N values)` | Past the 1,000 most common values, the rest in one row, without a bar |

Values are formatted as in the table, and <kbd>,</kbd> turns digit grouping
on or off here too.

## Keys

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A page |
| <kbd>Home</kbd> <kbd>End</kbd> or <kbd>G</kbd> | First and last row |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Previous or next column; a column counted before shows at once |
| <kbd>Enter</kbd> | The rows holding the value, as a drill-down. <kbd>Esc</kbd> there comes back |
| <kbd>s</kbd> | Sort by count or by value; the header's mark says which |
| <kbd>c</kbd> | A number's histogram, or the list of its values |
| <kbd>a</kbd> | Count every row, when the counts are of a sample |
| <kbd>y</kbd> | [Copy](copying.md) the counts as TSV: every value with its count, percent and cumulative percent |
| <kbd>e</kbd> | [Export](exporting-data.md) the same table to a file |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |
| <kbd>Esc</kbd> | Back to the table. While every row is being counted, stop and keep the sample |

## What is read

The counts cover the view: the query, the filters and any drill-down apply.
One pass over the column counts every row, in the background. <kbd>Esc</kbd>,
or moving to another column, stops it.

When the view is one Parquet or IPC file that is remote, or that holds more
than 100 times the sample size (10,000,000 rows by default), it is
[sampled](analysis-features.md#sampling) first instead: a few of its row groups
are read, and the header says `sample of`. <kbd>a</kbd> then counts every row. A
view the sampler would have to read whole anyway, such as a directory of
files or a CSV, is counted exactly from the start. The sample size is
`[analysis] sample_rows`; `0` always counts every row.
