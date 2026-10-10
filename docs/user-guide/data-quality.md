# Check data quality

Data Quality reports missing values, repeated values and changes across files
or time: <kbd>a</kbd>, **Data Quality**, <kbd>Enter</kbd>.

The pane opens on **Setup**, which holds every setting for a run. Press
<kbd>Enter</kbd> to run as it is, or change settings first. Nothing in Setup
reads the data; only <kbd>Enter</kbd> does, once.

| Section | What you set |
|---|---|
| **Rows & sample** | The [sample](analysis-features.md#sampling) every analysis tool reads: which rows, how they are picked, how many, the seed |
| **Columns** | Text read as time, time roles, the intervals between them, and what each column must hold |
| **Study** | Grain, the windows rows are expected in, comparison, values or file metadata only, latency threshold, and the window an interval goes in |
| **Read** | What Run will read: a sampling pass, a count, rows a run already read, or no read at all |

<kbd>s</kbd> opens the Sample form over Setup. <kbd>Enter</kbd> in the form
applies the sample to Setup and returns there. <kbd>Esc</kbd> discards everything changed
in Setup. After a run, <kbd>e</kbd> opens Setup again.

## Check missing values

Open **Food nutrition (fast food)** from **Example datasets**, then:

1. Press <kbd>a</kbd>, choose **Data Quality** and press <kbd>Enter</kbd>.
2. Press <kbd>Enter</kbd> in Setup. The table's 515 rows are fewer than the
   sample size, so every row is read.

| Finding | Columns | Reads |
|---|---|---|
| ▲ Mixed spellings | `item` | 2 values spelled more than one way: `4 Piece Chicken Nuggets` and `4 piece Chicken Nuggets`, and the same for 6 |
| Missing values | `vit_a`, `fiber`, `protein` | 0.19% to 41.6% |
| Missing together | `vit_c`, `calcium` | 210 rows (40.8%) |
| Nearly unique | `item` | 10 repeated (98.1% unique) |
| Single value | `salad` | always "Other" |

Press <kbd>2</kbd> for **Columns**: each column's missing count and findings.
A filter changes which rows are checked. To ignore it, press <kbd>s</kbd>, set
**Rows from** to the unfiltered source, and press <kbd>Enter</kbd> twice: once
to apply the sample, once to run.

On a large file the run reads a sample. On **NYC yellow taxis (January 2025)**
with **Random seed** `1`, the header reads
`Data Quality · sample of 100,000 of 3,475,226 rows`, and the one note is
`passenger_count`, `RatecodeID` and three more columns missing together in
16,000 rows (16.0%) of the sample.

## Inspect a finding

On **Overview**, select a finding and press <kbd>Enter</kbd> to see its
numbers, the evidence and what to check. Press <kbd>Enter</kbd> again to open
the matching rows. On a sampled run, these are the sample's rows that the
finding counted. <kbd>Esc</kbd> returns to the finding.

| Finding | <kbd>Enter</kbd> on its detail opens |
|---|---|
| **Duplicate rows** | Every row that has a copy, copies together, most copied first |
| **Numbers as text**, **Dates as text** | The values that do not parse, which stop a cast |
| Several columns, such as **Missing values** | The rows missing in any of them. The detail lists each column's count; the rows with any of them are a range, not a sum |
| **Numbers**, **Dates** or **Codes as text** that all parse | Nothing: the detail says every value parses |

The rows come from the ones the run kept, so opening them reads nothing. A full
scan keeps no rows, and an older sample may have been released. In those cases
<kbd>Enter</kbd> first shows what reading the rows would take; only a second
<kbd>Enter</kbd> reads them, and <kbd>Esc</kbd> backs out without reading.

To see fewer findings, press <kbd>c</kbd> to show one column's or
<kbd>t</kbd> to show one type's. <kbd>o</kbd> orders them by rows affected,
then by rate. Problems stay above notes, and the line above the list says what
is narrowed. <kbd>Esc</kbd> shows every finding again. None of this reads or
measures anything.

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

**Trends** draws each column across the whole range, starting with the rows
per segment, and pools consecutive segments into bars. <kbd>m</kbd> changes the
measure. When the sample is thin per segment, such as 100,000 rows over years
of days, **Segments** names only large changes; **Trends** shows the smaller
ones. To judge
single days, sample **Equal per value** of the date, or read every row.

## Read a trend

On **Trends**, a sampled report draws the sampled rows under the exact row
counts, and says how many segments the sample reached thinly or not at all. A
segment that has rows but no sampled row gets its own mark (`·`, `?` in
ASCII), never a short bar. Select a line and press <kbd>Enter</kbd> for its
bars; <kbd>↑</kbd> <kbd>↓</kbd> step through them.

| Row | Says |
|---|---|
| Span | The bar's calendar range, or its first and last segment |
| Segments | How many it pools, how many were not sampled, how many under 30 sampled rows |
| Rows | Rows sampled of the exact count, or every row read |
| The measure | The count, out of how many rows or values, and the rate |
| 95% interval | Where the rate likely sits, from the sample; none when every row was read |
| Previous bar | The rate before and now, the move in points, and whether it is clear or within sampling noise; the baseline's bar when comparing with one |

When segments are thin, press <kbd>w</kbd>. Setup opens with the next coarser
window set (days to weeks, weeks to months). Its **Read** says what that costs,
often nothing, since days sum into weeks. <kbd>Enter</kbd> runs it, and
<kbd>Esc</kbd> keeps the grain you had. Nothing on these pages reads.

## Find gaps in time windows

A window with no rows counts as a gap only after you say rows are expected in it:

1. With a time-window grain, move to **Expected** in Setup and press
   <kbd>Space</kbd>.
2. Choose **Windows**: every window of the grain, or for hours and days,
   weekdays only.
3. Optionally type **From** and **Before**, such as `2024-01-01` and
   `2025-01-01`. Left blank, the range runs from the first window found to the last.
4. Press <kbd>Enter</kbd>, then <kbd>Enter</kbd> to run. With a report on screen
   and nothing else changed, this reads nothing: the windows are checked
   against the counts the report holds.

**Trends** then summarizes the expected windows, and <kbd>g</kbd> lists each
stretch of them that has no rows:

| Gap | Means |
|---|---|
| empty | No rows in the scope, by the exact count the run took |
| not sampled | Rows there, with their count, and none in the sample |
| out of scope | Outside the time range the scope reads: the run did not look |

A range is checked over at most 20,000 windows; past that, Trends says so and
asks for a shorter range or a coarser grain.

## Measure the time between dates

1. In Setup, move to **Time roles** and press <kbd>Space</kbd>. Give each role
   its column: datui does not infer a column's meaning from its name.
2. **Intervals** lists the pairs measured, such as `event to received`. Press
   <kbd>Space</kbd> on it to choose any start and end. Setup points out a role
   that is in no interval.
3. Optionally set **Latency over** to count breaches, and with a daily grain,
   **Window by** to put each delay on the day it started or ended.
4. Press <kbd>Enter</kbd> to run, then <kbd>5</kbd> for **Intervals**.

| Count | Out of |
|---|---|
| Both ends | The segment's rows |
| Missing start, Missing end, Unparsed start, Unparsed end | The segment's rows |
| Negative, Zero, Over the threshold | Rows with both ends |

A breach is `duration > threshold`: exactly an hour is not over an hour.
<kbd>Enter</kbd> on an interval opens its detail at any terminal size, and
<kbd>Enter</kbd> on a count there shows its rows. On a sample, those rows come
from the rows the run kept, not from a new read of the file. An interval from
valid from to valid to is a validity period: a missing end is **open**, and an
end before its start **ends first**.

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
each value as an instant in UTC. A time with no zone is read as UTC, and Setup
says so.
Values the format does not read are reported as **Unparsed times**, not as
missing values, and <kbd>Enter</kbd> on the finding opens their rows. If a role or
grain uses a text column with no format, Setup says so before you run.

## Declare what a column must hold

Declare what a column must hold, and the run reports the rows that break it:

1. In Setup, move to **Column intent** and press <kbd>Space</kbd>.
2. Choose a column and press <kbd>Space</kbd> for its form.
3. Set what applies: tick **Key** for the columns whose values together
   identify one row, tick **Required** if every row must have a value, type the
   **Allowed** values separated by commas (quote one that holds a comma:
   `"a, b"`), or set a **Minimum** and **Maximum**. Text can be **Read as** a
   whole number or decimal, and a range then compares the number.
   <kbd>Enter</kbd> applies.
4. <kbd>Enter</kbd> on the list returns to Setup; <kbd>Enter</kbd> there runs.

| Finding | Counts |
|---|---|
| Repeated key | Rows that share their key with another row |
| Incomplete key | Rows with no value in part of the key |
| Required, missing | Rows with no value |
| Not allowed | Values outside the allowed set, out of the column's values |
| Out of range | Values below the minimum or above the maximum |
| Unparsed numbers | Text that does not read as the number |

Each is reported as a problem, with its rows one <kbd>Enter</kbd> away. Intent
adds no read of its own: it is measured on the rows the run reads, and changing
it reuses the rows a run already read. On a sample, a key finds repeats among
the sampled rows only. A repeat found there is a real repeat, but finding none
says nothing about the rest of the data. Setup's **Read** says so before you run; set the sample's method
to **Every row** to check every row, which adds one pass over the key's columns.

## Export the report

On any report page, press <kbd>x</kbd>. Type a path, choose **JSON** or
**Markdown** with <kbd>Tab</kbd> and <kbd>←</kbd> <kbd>→</kbd>, and press
<kbd>Enter</kbd>.

| Format | Holds |
|---|---|
| JSON | Every measurement, the setup it was measured with, the source and the reads, versioned for tools ([schema](../reference/data-quality.md#exported-report)) |
| Markdown | The verdict, coverage, findings with their evidence, the checks, the gaps and the setup |

The report is written from what is on screen: nothing is read, and the data
need not still be there. A file that exists is overwritten only after you
confirm, and only once the whole report is written; see
[Overwriting](exporting-data.md#overwriting). A failed write keeps the dialog
open, with the path as typed and the reason under it.

## Check the read size

Before anything is read, Setup's **Read** section says what <kbd>Enter</kbd> will read:
one sampling pass (which also counts the grain's segments when it streams), a
count of the grain's column for exact segment totals, rows a run already read,
or nothing when the report is already on screen or in the session cache.
After a sampled run, changing roles, text formats, column intent, row chunks,
or moving to a coarser window of a counted grain reads nothing. After any run,
changing the comparison or expected windows reads nothing too. A new seed, size or scope reads a
new sample.
The **Read** heading names the rows runs have kept for reuse, such as
`100,000 rows kept · 12.4 MiB`; <kbd>d</kbd> releases them, and the next run
that would have used them reads its sample again.

A full scan of a remote dataset (S3, GCS, Azure) reads its scope once per check.
When the whole dataset fits within `[analysis] quality_local_copy` (2 GiB by
default) and the free disk space, Read says `1 fetch of 8 objects (16.5 MiB) to
a local copy · up to 7 passes over it`. Each object is fetched once into the
cache directory, and every pass, and every later full scan of the dataset,
reads the copy. The **Read** heading then adds `local copy · 16.5 MiB`, and <kbd>d</kbd> releases it with
the rows. Otherwise Read says the passes go to the source, and why, such as
`No local copy: 3.0 GiB over the 2.0 GiB limit`. See
[Local copy of a remote source](../reference/data-quality.md#local-copy-of-a-remote-source).
<kbd>p</kbd> shows the access plan in full. Reading every row asks for
confirmation first; <kbd>Esc</kbd> there leaves the sample and the report as
they were.

While a run reads, the progress shows its stage, whether that stage reads the
source, and the rows seen so far when the read can count them. <kbd>Esc</kbd>
cancels: a sampling pass, or a full scan's passes, stop at the next batch, and
a local copy being fetched stops at its next chunk and is removed. A read that
cannot stop shows as finishing, in the header and in Setup, until it ends.
Until then, Run waits, nothing else reads, and the last report stays.

Under the verdict, every report says what it covers:

```text
✓ No problems found  12 of 12 columns clean
Checks  7 sampled · 2 metadata · 1 unavailable
Rows    100,000 of 36,839,175 sampled (0.27%) · 36,839,175 traversed
Limits  Nearly unique: needs every row checked
```

**Checks** counts the checks that ran, what they ran over, and those that
could not run. **Limits** says why, and names thin segments and unread footers.
**Rows** gives the rows behind the numbers, and how many rows the run's reads
passed through to get them.

The header says what each result was measured on:
`Data Quality · sample of 100,000 of 36,839,175 rows`. Missing-column and
type-conflict findings come from the source files' footers (their stored
schemas), whatever rows the sample covers.

See the [reference](../reference/data-quality.md) for the metric definitions,
and [Keyboard shortcuts](../reference/keyboard-shortcuts.md#analysis-data-quality)
for every key.
