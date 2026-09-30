# Check data quality

Press <kbd>a</kbd>, choose **Data Quality**, and press <kbd>Enter</kbd>.
Use it to find missing values, repeated values and changes across files or time.

The pane is **Setup**: every setting a run takes, before anything is read.
Press <kbd>Enter</kbd> to run it as it stands, or change it first. Nothing in
Setup reads the data; <kbd>Enter</kbd> does, once.

| Section | What you set |
|---|---|
| **Rows & sample** | The [sample](analysis-features.md#sampling) every analysis tool reads: which rows, how they are picked, how many, the seed |
| **Columns** | Text read as time, time roles, and the intervals between them |
| **Study** | Grain, comparison, values or file metadata only, latency threshold, and the window an interval goes in |
| **Read** | What Run will read: a sampling pass, a count, rows a run already read, or no read at all |

<kbd>s</kbd> opens the Sample form over Setup; its <kbd>Enter</kbd> applies the
sample to Setup and returns there. <kbd>Esc</kbd> discards everything changed
in Setup. After a run, <kbd>e</kbd> opens Setup again.

## Check missing values

Open the [quick-start penguin data](../getting-started/quick-start.md#1-open-the-data)
with `--null-value NA`, then:

1. Press <kbd>a</kbd>, choose **Data Quality** and press <kbd>Enter</kbd>.
2. Press <kbd>Enter</kbd> in Setup. The table is smaller than the sample size,
   so every row is read.
3. Open **Columns** with <kbd>2</kbd> and inspect `body_mass_g`.

The source has 344 rows and two null body-mass values: a null rate of
`2 / 344`, about **0.58%**. A filter changes the rows checked; to ignore it,
press <kbd>s</kbd>, set **Rows from** to the unfiltered source, and press
<kbd>Enter</kbd> twice: once to apply the sample, once to run.

## Inspect a finding

On **Overview**, select a finding and press <kbd>Enter</kbd>: its numbers, the
evidence and what to check. Press <kbd>Enter</kbd> again to open the matching
rows; on a sampled run these are the sample's rows, the ones the finding
counted. <kbd>Esc</kbd> returns.

| Page | Use it for |
|---|---|
| **Overview** | Problems, notes and clean columns, most important first |
| **Columns** | Each column's findings, nulls, distinct values, parse rates and ranges |
| **Segments** | Compare files, partitions, days or row chunks |
| **Trends** | Each column across the whole range |
| **Intervals** | The time between two dates in each segment, and every count behind it |

<kbd>←</kbd> <kbd>→</kbd> move between the pages.

## Compare parts of a dataset

Press <kbd>e</kbd> for Setup. Move to **Grain** and press <kbd>←</kbd>
<kbd>→</kbd>, or <kbd>Space</kbd> for the list: by file, by a partition
column, by day, week or month of a date column, or in chunks of rows. Set
**Compare** to the segment before or a baseline the same way, then press
<kbd>Enter</kbd> to run.

**Segments** lists each segment's rows, its share of null cells and its
largest change: a row count that halved or doubled, or a column's rate that
moved past sampling noise. <kbd>o</kbd> puts the largest changes first,
<kbd>Enter</kbd> shows a segment's columns beside the one it is compared with,
and <kbd>b</kbd> makes the selected segment the baseline.

**Trends** draws each column across the whole range, the rows per segment
first, pooling consecutive segments into bars; <kbd>m</kbd> changes the
measure. A sample thin per segment, such as 100,000 rows over years of days,
names only large changes on Segments; Trends shows the smaller ones. To judge
single days, sample **Equal per value** of the date, or read every row.

## Measure the time between dates

1. In Setup, move to **Time roles** and press <kbd>Space</kbd>. Give each role
   its column: datui does not infer a column's meaning from its name.
2. **Intervals** lists the pairs measured, such as `event to received`. Press
   <kbd>Space</kbd> on it to choose any start and end. Setup names a role that
   is in no interval.
3. Optionally set **Latency over** to count breaches, and with a daily grain,
   **Window by** to put each delay on the day it started or ended.
4. Press <kbd>Enter</kbd> to run, then <kbd>5</kbd> for **Intervals**.

| Count | Out of |
|---|---|
| Both ends | The segment's rows |
| Missing start, Missing end, Unparsed start, Unparsed end | The segment's rows |
| Negative, Zero, Over the threshold | Rows with both ends |

A breach is `duration > threshold`: exactly an hour is not over an hour.
<kbd>Enter</kbd> on an interval opens its detail at any terminal size;
<kbd>Enter</kbd> on a count there shows its rows; on a sample, they are cut
from the rows the run kept rather than read from the file again. Valid from to valid to reads as a validity
period: no end is **open**, and an end before its start **ends first**.

## Times stored as text

A column of times written as text, such as `01/31/2024 08:15:00`, is read as
time only when you say how:

1. In Setup, move to **Text as time** and press <kbd>Space</kbd>.
2. Choose the column, then its format. Each format says how many of the values
   on screen it reads, and the one that reads the most comes first.
3. The column now splits by day, week or month under **Grain**, and can take a
   **Time role**.

The format applies to this study only: every other check still sees the text.
A format with an offset, such as `2024-01-31T08:15:00Z` or `+05:00`, reads
each value as an instant in UTC; a time with no zone beside it is read as UTC,
and Setup says so.
Values the format does not read are reported as **Unparsed times**, not as
missing values, and <kbd>Enter</kbd> on the finding opens their rows. A role or
grain on text with no format is named in Setup before you run.

## Check the read size

Setup's **Read** section says what <kbd>Enter</kbd> will read before it reads:
one sampling pass (which also counts the grain's segments when it streams), a
count of the grain's column for exact segment totals, rows a run already read,
or nothing when the report is already on screen or in the session cache.
Changing roles, text formats, comparison, row chunks or a coarser window of a
counted grain reads nothing; a new seed, size or scope reads a new sample.
<kbd>p</kbd> shows the access plan in full. Reading every row asks for
confirmation first; <kbd>Esc</kbd> there leaves the sample and the report as
they were.

While a run reads, the progress names its stage, whether that stage reads the
source, and the rows seen where the read can count them. <kbd>Esc</kbd>
cancels: a sampling pass, or a full scan's passes, stop at the next batch. A
read that cannot stop is shown as finishing, in the header and in Setup, until
it ends; until then Run waits, and the last report stays.

Under the verdict, every report says what it covers:

```text
✓ No problems found  12 of 12 columns clean
Checks  7 sampled · 2 metadata · 1 unavailable
Rows    100,000 of 36,839,175 sampled (0.27%) · 36,839,175 traversed
Limits  Nearly unique: needs every row checked
```

`No problems found` on a sample is not the same as on every row: **Checks**
counts what ran and over what, and what could not run; **Limits** says why, and
names thin segments and unread footers. **Rows** gives the rows behind the
numbers, and the rows the run's reads passed through to get them.

The header says what each result was measured on:
`Data Quality · sample of 100,000 of 36,839,175 rows`. Missing-column and
type-conflict findings describe the loaded source's footers, whatever rows the
sample covers.

See the [reference](../reference/data-quality.md) for Setup, keys, metric
formulas and sampling, or the [key list](../reference/keyboard-shortcuts.md#analysis).
