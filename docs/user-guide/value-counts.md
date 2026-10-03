# Count values

Press <kbd>F</kbd> at the table to see how many rows hold each value of the
column under the column cursor, with a summary of the column above them.
<kbd>Esc</kbd> goes back.

Move the cursor with <kbd>h</kbd> <kbd>l</kbd> or <kbd>g</kbd>. <kbd>←</kbd>
<kbd>→</kbd> on the counts move to the previous or next column, and the
table's cursor goes with them.

## Which carrier flies most

Open **NYC flights (2013)** from **Public datasets** on the home screen, press
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

<kbd>→</kbd> moves to `flight`, a number, and the summary answers whether
the column can be added up:

```text
 Rows 336776   Distinct 3844   Nulls 0   Sum 664096549   Mean 1971.9236   Min 1
 Max 8500
```

## What the screen shows

| Part | What it says |
|---|---|
| Header | The column, and what was counted: `all 336,776 rows`, or `sample of 100,000 of 657,752 rows` |
| Summary | `Rows`, `Distinct` (null not among them) and `Nulls`; `Sum`, `Mean`, `Min` and `Max` for numbers; `Min` and `Max` for dates and times. A sample has no `Sum` |
| Lines | Each value's rows, its percent of all of them, the running percent, and a bar beside the most common value's |
| `∅` | The nulls, on a line of their own: ranked by their rows when sorted by count, last when sorted by value |
| `other (N values)` | Past the 1,000 most common values, the rest on one line, without a bar |

Values are written as the table writes them: <kbd>,</kbd> groups digits
here too.

## Keys

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A page |
| <kbd>Home</kbd> <kbd>End</kbd> or <kbd>G</kbd> | First and last line |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Previous or next column; a column counted before shows at once |
| <kbd>Enter</kbd> | The rows holding the value, as a drill-down. <kbd>Esc</kbd> there comes back |
| <kbd>s</kbd> | Sort by count or by value; the header's mark says which |
| <kbd>a</kbd> | Count every row, when the counts are of a sample |
| <kbd>y</kbd> | [Copy](copying.md) the counts as TSV: every value with its count, percent and cumulative percent |
| <kbd>e</kbd> | [Export](exporting-data.md) the same table to a file |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |
| <kbd>Esc</kbd> | Back to the table. While every row is being counted, stop and keep the sample |

## What is read

The counts are of the view: the query, the filters and a drill-down apply. One
pass over the column counts every row, in the background; <kbd>Esc</kbd> or
another column stops it.

A remote view, or one over 100 times the sample size (10,000,000 rows at the
default), held in one Parquet or IPC file, is
[sampled](analysis-features.md) first instead: a few of its row groups are
read, and the header says `sample of`. <kbd>a</kbd> then counts every row. A
view the sampler would have to read whole anyway, such as a directory of
files or a CSV, is counted exactly from the start. The sample size is
`[analysis] sample_rows`; `0` always counts every row.
