# Data quality reference

For a first check, follow [Check data quality](../user-guide/data-quality.md).
Data Quality reads the shared analysis [sample](../user-guide/analysis-features.md#sampling), as its [Setup](#setup) says.

## Reading the report

The first line counts problems, notes and clean columns. Under it, on every
report, the coverage says how far that verdict reaches. Below, findings are
grouped under **Problems**, **Notes** and **Clean**. <kbd>Enter</kbd> on a
finding lists its numbers, evidence and what to check; <kbd>Enter</kbd>
again shows its rows. On a sampled run those are the sample's rows, so the
count the finding gave is the count in the table.

### Narrowing and ordering

| Key | Overview shows |
|---|---|
| <kbd>c</kbd> | Only the findings that name one column, chosen from a list with each column's count; **All columns** undoes it |
| <kbd>t</kbd> | Only one type's findings, by the check that made them (Missing values covers Missing together, Mostly and Always missing); **All types** undoes it |
| <kbd>o</kbd> | Ranked (the default), then by rows affected, then by rate: rows affected over rows checked, compared exactly. The number ordered by is on every row |
| <kbd>Esc</kbd> | Every finding again, in the order chosen; a second <kbd>Esc</kbd> leaves |

Problems stay above notes in every order, and ties keep the ranked order. A
finding over several columns orders by its largest column's count. The line
above the list says what is narrowed and how it is ordered, such as
`2 of 7 findings · column price · by rows`. Narrowing and ordering read the
report on screen: nothing is read or measured again.

### Evidence

The detail of a finding over several columns lists each column's count and
rate, then `Rows with any of them`: the largest column's count when that is
all of them, otherwise a range from the largest count to their sum (at most
the rows checked), marked not counted. **Missing together** columns are null on
the same rows, so their count is the rows.

| Finding | Rows it opens | Count shown | Examples in the detail |
|---|---|---|---|
| Duplicate rows | Every row equal to another in every column; copies together, most copied first | Rows with a copy | The three most copied rows, with their copies |
| Numbers, Dates or Codes as text | Non-null text the reading does not parse | Non-null values less those that parse | Up to three values that do not parse |
| Unparsed times | Text the chosen time format does not read | Unparsed values | Up to three of them |
| Nearly unique | Every row whose value repeats | Rows beyond one per value; more open | The most repeated value |
| Missing in files, Type mismatch | Every row of the named files | Rows of those files | The files and the values a conflict hides |
| Repeated key | Every row whose declared key another row also holds | Rows sharing a key value | |
| Any other | Rows matching the check | The finding's rows, or a range for grouped columns | |

Rows open from the rows the run kept, in memory: no read, and the same rows
whether or not the source is still there. A sample is kept while it is the
sample the report measured. Examples come from kept rows too, so a full scan
shows the counts without them. When the rows are not kept, the detail says why,
and <kbd>Enter</kbd> shows a **Read Rows** dialog before anything is read:

| Row | Says |
|---|---|
| Rows | The finding and its columns |
| Why | A full scan keeps no rows; the rows read are no longer kept; or the rows are in the named files |
| Reads | The sample again from its seed, every row of the scope once (duplicates), every row of the scope to count the matches and then the rows on screen, or the named files; with a read size where one can be estimated |
| Shows | How many rows, when one count is all of them |
| Source | Local or remote, read only |

<kbd>Enter</kbd> reads; <kbd>Esc</kbd> goes back to the finding having read
nothing. A finding with no rows to show (a text column whose every value
parses) says so, and <kbd>Enter</kbd> closes it.

| Coverage line | Says |
|---|---|
| Checks | The ten checks, and Column intent when any is declared, by what they read: `exact` (every row in scope), `sampled` (the sample), `metadata` (file footers); then `skipped`, with nothing in the data to look at (no float column, one file), and `unavailable`, which apply but this run could not answer (values not read, no rows in the scope, or a sample where the answer needs every row). A scope with no rows calls no column clean: its verdict is `No rows to check` |
| Rows | Rows read of the total: `100,000 of 36,839,175 sampled (0.27%)`, `all 1,204 read, exact`, `none: the scope has no rows`, or `none read, file metadata only`; `up to 500 per value` for an Equal per value sample; then the rows the run's reads passed through, summed over every pass, when the reads counted them: `36,839,175 traversed`, `at least …` when some read could not count, `no source read` when the run used rows already read |
| Limits | Why each unavailable check did not run; segments with fewer than 30 sampled rows (`4 of 31 segments under 30 sampled rows`); segments the scope has rows in and the sample drew none of (`3 segments with rows, none sampled`); `footers of 200 of 5,000 files read` on a dataset too large to read every footer, where the file checks cover only those; `time roles form no interval`; `key repeats among 10,000 sampled rows only` for a declared key on a sample; `intent on code: not in scope` |

The coverage comes from what the run measured; showing it reads nothing.
Traversed rows are counted as each read hands them on, after the filters the
scan applies, so a pass that skips nulls counts fewer. A Polars scan does not
report bytes or requests, so neither is shown. On a terminal under 19 rows the
coverage keeps two lines and Rows gives way first (the header says the rows
too); a line cut short counts what it left out (`+2 more`).

<kbd>Enter</kbd> on the **Clean** entry lists the checks the run made: the
columns each covered, what it found, or why it was skipped or unavailable.
The six most important show first and <kbd>Enter</kbd> shows all of them.
Column intent, when declared, comes first. When a
run finds nothing, the list is on the page under the coverage.

| Finding | Tier | Means |
|---|---|---|
| NaN or infinite | Problem | Float values that are NaN or ±infinity; one NaN makes a sum or mean NaN |
| Empty text / Blank text | Problem | Text that is `""` or only whitespace: looks filled in, carries nothing |
| Mixed spellings | Problem | Values equal after trimming and lowercasing, such as `"West"` and `"west "` |
| Duplicate rows | Problem | Rows identical in every column |
| Always missing | Problem | A column with no value in any row checked |
| Missing in files / Type mismatch | Problem | Files without the column, or holding it in a type the dataset cannot read |
| Mostly missing | Note | Columns with no value in more than half the rows checked, listed above other missing values |
| Missing together | Note | Columns only ever null together: no row misses one without the others, so one cause is likely |
| Missing values | Note | Nulls, as one finding with each column's rate inside it, highest first |
| Numbers as text / Dates as text | Note | At least 95% of a text column parses as numbers or ISO dates |
| Codes as text | Note | Whole numbers with leading zeros or a fixed width: a code, fine as text |
| Nearly unique | Note | A whole-number or text column at least 95% unique whose values still repeat; a duplicate if it is a key |
| Unparsed times | Problem | Text read as time whose values the chosen format does not read; <kbd>Enter</kbd> opens their rows |
| Single value | Note | One value in every row checked |
| Repeated key | Problem | Rows sharing a value of the declared key; see [Column intent](#column-intent) |
| Incomplete key | Problem | Rows with no value in some part of the declared key |
| Required, missing | Problem | Rows with no value in a column declared required |
| Not allowed | Problem | Values outside a column's declared allowed set |
| Out of range | Problem | Values below a column's declared minimum or above its maximum |
| Unparsed numbers | Problem | Text declared to read as a number that does not |

A dataset-grain sample is spread across the whole scope, as Describe's is:
one Parquet or IPC file is read as a few dozen short runs across it, anything
else in one streamed pass that keeps a seeded sample. The header says how many
rows were sampled of how many.

## Setup

Choosing Data Quality opens **Setup**, whatever another tool has read: every
setting a run takes, staged until <kbd>Enter</kbd> runs them. Nothing in Setup
reads values; it knows the schema, the rows already on screen, and what earlier
runs kept. After a run, <kbd>e</kbd> opens it again.

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between the rows |
| <kbd>←</kbd> <kbd>→</kbd> | Change Grain, Compare, Values, Latency over or Window by in place; on another row, <kbd>→</kbd> opens it |
| <kbd>Space</kbd> | Open the row: the Sample form, the Time roles, Intervals, Column intent or Expected editor, or a list of choices (type to narrow, <kbd>Enter</kbd> chooses) |
| <kbd>s</kbd> | Open the Sample form; its <kbd>Enter</kbd> applies the sample to Setup and returns there |
| <kbd>p</kbd> | Show the access plan |
| <kbd>Enter</kbd> | Run, from any row |
| <kbd>Esc</kbd> | Discard every staged change, the sample's included, and go back to the report, or to the tool list before the first |

Run commits the setup: its sample becomes the one every tool reads, and the
other tools' results, taken from the old sample, go. A setup the report on
screen was measured with shows that report again, and one in the session cache
shows its cached report; neither reads. A full scan asks first, and
<kbd>Esc</kbd> there leaves the draft staged and the sample and report as they
were. While a cancelled run is still stopping, Run waits and Setup says why.

| Row | Choices |
|---|---|
| Sample | The shared sample: scope, method, rows and seed |
| Text as time | Text columns read as a date or datetime through a chosen format, for this study only |
| Time roles | Event, effective/as-of, period end, created, published, received, processed, valid from, and valid to |
| Intervals | Which starts and ends are measured, from every pair the assigned roles make; offered once two roles are assigned |
| Column intent | What columns must hold: the key, and per column required, allowed values, a range, or text read as a number. See [Column intent](#column-intent) |
| Grain | Whole dataset; by file, when the dataset has several; by each partition column; by hour, day, week or month of any date or time column, or text read as time (hours only where there are times); or in chunks of 100,000 or 1,000,000 rows |
| Expected | With a time-window grain: none, every window, or weekdays only (hours and days); From and Before, a date or UTC timestamp each, blank for the first and last window found. See [Expected windows and gaps](#expected-windows-and-gaps) |
| Compare | None, the segment before (partitions and files in the order their names count), or a baseline segment |
| Values | Read, or metadata only (footers, no values); whether a read is sampled or every row is the sample's method |
| Latency over | None, 1 hour, 1 day or 1 week; offered once there is an interval. A breach is `duration > threshold`, strictly |
| Window by | With a time-window grain and an interval: the grain's column, each interval's start, or each interval's end, which puts a delay across midnight on the day it ended |

The Read section says what Run will do, before it does it:

| Read | When |
|---|---|
| No read: the report is already here | The setup is the report's, or the session cache holds it |
| Only Compare or Expected changed: no read | The setup differs from the report on screen only in its comparison or expected windows; Run compares the segments the report holds, and checks the windows against its counts, after a full scan too |
| Uses the rows a run already read | A sampled setup whose sample (scope, method, size, seed), dataset and view match rows a run read this session; any grain, role or format |
| Seeded runs of the file | A random sample of one Parquet or IPC file: the whole source, or a view with no filter, query or reshape (a sort is fine); a few dozen short reads |
| One pass that streams every eligible row | Any other random or equal-per-value sample; the pass counts the scope too |
| Read before; released to free memory | Those rows were read this session and released to the memory budget: Run reads them again |
| Counts every row by the grain's column in that pass | A partition or time-window grain on a streamed sample: exact segment totals from the one pass |
| Segment totals from a count already read | The same grain was counted before, with these rows |
| Segment totals summed from the hourly or daily counts | A coarser window of the same column: hours sum into days, weeks and months, days into weeks and months |
| Plus one count of the grain's column | A partition or time-window grain that nothing has counted: seeded runs or first rows, a new grain on rows already read, or a finer window; kept for later runs |
| Too many segments … to count | The grain had more than 1,000,000 keys; a coarser grain is needed |
| Every eligible row, in up to N passes | A full scan: one collect per check, and one more to count an unknown scope |
| Window by each interval's start or end: N of those passes | A full scan whose intervals start or end on more than one column: one grouping each |
| File metadata only | Values set to metadata only |
| Column intent: checked on the rows read, no extra read | Intent declared on a sampled run: measured on the sample's rows in memory |
| Key: finds repeats among the N sampled rows only | A declared key on a sample smaller than the scope |
| Column intent: counted in the profile pass; the key adds one pass over its columns | A full scan with a declared key: one more pass, counted among the passes |
| Column intent needs values: not checked | Intent declared with Values set to metadata only |
| Expected windows: checked against the segment counts, no read | Expected is set: gaps come from the counts the run takes anyway |

Before a run, source-scoped setups report unknown row counts and read sizes,
and metadata-only runs do not count rows. A local sample estimates its read as
a ceiling (`up to`); the run reports the exact eligible count. The scope is
applied before sampling, so sampling never reaches beyond its bounds.
<kbd>p</kbd> shows the access plan: source, scope, grain, sample, rows
evaluated, value reads, requests, source files, the extra reads a type conflict
costs, writes (none), the passes, what column intent costs, and the estimate
basis. Remote bytes and
requests are unknown until measured, and say so.

The Time roles row opens an explicit mapping table; every role starts
unassigned. Under it, each date, time and text column is listed with its type,
or the format a text column is read with, and a few of its values from the rows
on screen; the column the focused role holds is marked. Datui recognizes
physical date and datetime types but never guesses a column's meaning from its
name. With a source scope, the list includes source columns hidden by the
current view.

The Intervals row opens the list of every start and end the assigned roles
make, <kbd>Space</kbd> to measure one or not. Until one is chosen, the
suggested pairs are measured: event to published, received or processed;
period end to published; published to received; received to processed; and
valid from to valid to. Setup names any assigned role that is in no interval.

| | |
|---|---|
| Valid from to valid to | A validity period: a missing end is **open**, and an end before its start **ends first**. Overlaps and gaps between periods need an entity key and consecutive rows, which a sample does not hold, so they are not counted |
| Window by | Only with a time-window grain. By their start or end, intervals are grouped once per column they start or end on; on a full scan each grouping is a pass, and Read says how many before Run |
| Time zones | A datetime with a zone, or text read with an offset, is its instant in UTC. A date or datetime with no zone is read as if it were UTC, and Setup says so when it meets a zoned one. Windows start on UTC boundaries |

### Text as time

<kbd>Space</kbd> on Text as time lists the scope's text columns with a value
from the rows on screen. Choosing one lists the formats: `%Y-%m-%d %H:%M:%S`,
`%Y-%m-%dT%H:%M:%S`, with fractional seconds, with an offset
(`%Y-%m-%dT%H:%M:%S%.f%#z` and `%Y-%m-%d %H:%M:%S%.f%#z`, which read `Z`,
`+05:00`, `-0500` and `+05`), `%Y-%m-%d %H:%M`,
`%m/%d/%Y %H:%M:%S`, `%m/%d/%Y %I:%M:%S %p`, `%d/%m/%Y %H:%M:%S`,
`%d.%m.%Y %H:%M:%S`, and the dates `%Y-%m-%d`, `%Y%m%d`, `%m/%d/%Y`,
`%d/%m/%Y`, `%d.%m.%Y`. Each says how many of the values on screen it reads,
and those that read the most come first. **text, not a time** takes the format
away, and a time-window grain on the column with it.

| | |
|---|---|
| Applies to | Grain and time roles. Every other check, and the column's own findings, see the stored text |
| Time zone | With an offset format, each value is its instant in UTC; without one, a time with no zone, read as UTC beside a zoned one |
| Values it does not read | Counted per column as **Unparsed times**, a problem, apart from missing values; in intervals, as unparsed starts and ends; in a time-window grain, with the rows that have no time |
| Not applied to | The sample's time range and an equal-per-value sample, which read date and time columns as stored |

## Keys and result tabs

The control bar has one shape on every report page: <kbd>Esc</kbd> Back, the
page's own action, then <kbd>e</kbd> Setup, <kbd>s</kbd> Sample,
<kbd>←</kbd> <kbd>→</kbd> Page, <kbd>v</kbd> View Rows and <kbd>x</kbd> Export.

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Previous or next page: Overview, Columns, Segments, Trends, Intervals |
| <kbd>Enter</kbd> | Open a finding, or show its rows; open a Trends line's bars; open an interval's detail, or the rows behind the count under the cursor (asking first when the run kept none); on an empty Segments, Trends or Intervals page, open the Setup row that fills it |
| <kbd>c</kbd> / <kbd>t</kbd> | On Overview, only one column's or one type's findings |
| <kbd>e</kbd> | Open Setup |
| <kbd>s</kbd> | Open Setup with the shared [Sample](../user-guide/analysis-features.md#sampling) form over it |
| <kbd>p</kbd> | Show the detailed access plan |
| <kbd>1</kbd>–<kbd>5</kbd> | Overview, Columns, Segments, Trends, Intervals, directly |
| <kbd>o</kbd> | On Overview, order findings ranked, by rows or by rate; on Segments, list the largest change first, or back in order |
| <kbd>m</kbd> | Cycle the measure Trends draws: null, empty, whitespace, non-finite, distinct, integer-parse, decimal-parse |
| <kbd>w</kbd> | On Trends, stage the next coarser window in Setup: an hour to a day, a day to a week, a week to a month, 100,000-row chunks to 1,000,000. Nothing runs until <kbd>Enter</kbd> there |
| <kbd>g</kbd> | On Trends, list the expected windows with no rows; only once Setup states Expected |
| <kbd>b</kbd> | Use the highlighted segment as the comparison baseline; deltas update without another data read |
| <kbd>r</kbd> | On a sampled report, run again with a new sample seed, for every tool; waits while a cancelled read finishes |
| <kbd>x</kbd> | Export the report on screen to JSON or Markdown; see [Exported report](#exported-report) |
| <kbd>Tab</kbd> | Move between the result and the tool list |
| <kbd>Esc</kbd> | Back one level: a narrowed Overview shows every finding first; from a page, close Analysis |

### While a run reads

The progress names the stage the run is in (preparing, reading the sample or
reusing the last one, counting rows or segment rows, profiling columns,
checking duplicates and spellings, profiling segments, computing intervals,
assembling the report), whether that stage reads the source or works on rows
already read, and the rows the stage has seen where its read can count them.
There is no percentage: a run has no total to measure against.

<kbd>Esc</kbd> cancels, and a run stops between stages. Inside a stage:

| Read | On <kbd>Esc</kbd> |
|---|---|
| Random or Equal per value sample | Stops at its next batch, or between seeded runs |
| Full scan's passes (profiles, duplicates, spellings, segments, intervals, shared nulls) and a segment count | Stop at the next batch on the streaming engine (`[performance] polars_streaming`, on by default); with it off, each runs to its end |
| Values a type conflict hides | Stop between files |
| First rows sample; a full scan's row count when the total is not known | Runs to its end |

A run that stops at its next batch flashes `Run cancelled`. For a read that
runs to its end, the header and Setup say
`Cancellation requested; source read finishing` until the worker exits; a run
that should have stopped and is still going after a second says
`Cancellation requested; run stopping`. Either way nothing reads beside it: Run
and <kbd>r</kbd> say why they wait, and so do a finding's rows, <kbd>v</kbd> and the
other tools, unless the rows were kept. The last report stays, labeled with
the setup it was measured with, and Setup opens on the setup that was running.
A sample read before the cancel is kept for the next run.

Every result states eligible and evaluated rows and whether values are exact,
sampled, or metadata-only.

- **Overview** — the report. <kbd>Enter</kbd> on a finding lists its numbers,
  what to check and its evidence: the spellings, the most
  repeated value, the files behind a missing or mistyped column and the values
  a conflict hides, values with the most rows first. A finding taller than the
  screen scrolls with <kbd>↑</kbd> <kbd>↓</kbd>. <kbd>Enter</kbd> again opens the matching rows in a
  temporary table (every column's rows, for a grouped finding) from the rows
  the run kept, or, when it kept none, asks first with what the read would be
  (see [Evidence](#evidence)). <kbd>Esc</kbd> returns to the finding.
  <kbd>c</kbd>, <kbd>t</kbd> and <kbd>o</kbd> narrow and order the list (see
  [Narrowing and ordering](#narrowing-and-ordering)).
- **Columns** — a mark per column (problem, note or clean), its missing
  count and its findings. <kbd>Enter</kbd> opens the column's detail: its
  findings first, then what was measured on it, one aligned row each (type,
  missing, distinct, empty and blank text, NaN, range, most common value, text
  length, what the text parses as, spellings). Values read as the table shows
  them, number format included; spellings are quoted, so a trailing space
  shows. <kbd>↑</kbd> <kbd>↓</kbd> move to the next column; <kbd>Enter</kbd>
  or <kbd>Esc</kbd> returns to the list.
- **Segments** — needs a Grain other than the whole dataset; until then the
  page says so and <kbd>Enter</kbd> opens the plan on Grain. One row per
  segment (`year=2019`, a file, a row chunk, a window) in the order its name
  counts: its rows (`38 of 6,812`: sampled, of the exact count), its share of
  null cells, and, when compared, the largest change against the compared
  segment. A row count that halved or doubled comes first (`rows 1,203
  (-82%)`), then any column's null, empty, blank or NaN rate (`price nulls
  -12.0 pp`). On a sample a rate change is named only when it is past sampling
  noise, so a daily grain over a sample names only large ones; the page says
  how many sampled rows a segment has. <kbd>o</kbd> lists the largest change
  first. <kbd>Enter</kbd> on a segment lists every column's measures beside the
  compared segment, the ones past noise first and the rest dimmed;
  <kbd>Esc</kbd> goes back.
- **Trends** — one line per column over the whole range, with the rows per
  segment first: each bar pools as many consecutive segments as the width
  needs (`each bar 35 days`), so a thin day's sample never makes a bar alone.
  On a sample, `rows` is the exact count and `sampled rows` the rows the sample
  drew; a segment the scope has rows in and the sample drew none of keeps its
  place, and a bar of only such segments is drawn with its own mark (`·`, `?`
  in ASCII), never a short bar or a blank. The page counts those segments and
  the thin ones, under 30 sampled rows, and with a coarser window available
  says <kbd>w</kbd> stages it. Columns whose measure moves most come first, and
  columns that draw the same line segment by segment, such as columns missing
  together, share one. <kbd>m</kbd> changes the measure. Without a grain that
  orders segments the page says so, and <kbd>Enter</kbd> opens Grain in Setup.
  <kbd>Enter</kbd> on a line opens its bars, the line drawn with a pointer
  under the bar selected; <kbd>↑</kbd> <kbd>↓</kbd> walk them. From the
  measurements the report holds:

  | Row | Says |
  |---|---|
  | Span | Calendar range of the bar's windows, inclusive (`2024-01-29 to 2024-02-25`), with rows that have no time said apart; on other grains its first and last segment |
  | Segments | Segments pooled, how many not sampled, how many under 30 sampled rows |
  | Rows | `412 sampled of 3,210 (12.8%)`, or every row read |
  | The measure | Count of its denominator (rows, or values for a distinct or parse share) and the rate; on a rows line, rows per segment: the mean, the smallest and the largest |
  | 95% interval | The [Wilson interval](#data-quality-metric-definitions) of the rate on a sample, said to rest on under 30 rows when it does; none needed when every row was read, and none for a distinct share |
  | Previous bar, Baseline bar | Against the bar before, or with a baseline, the bar holding it: before and now, the move in points, and `a clear change`, `within sampling noise` or `under a point`, judged as Segments judges a change (a distinct share is not judged) |

  With Expected set in Setup, the page also sums up the expected windows, and
  <kbd>g</kbd> lists them; see [Expected windows and gaps](#expected-windows-and-gaps).
- **Intervals** — one row per interval and segment: the interval, its segment
  where the width allows, p50, and the share over the threshold (or negative,
  with no threshold) of the rows with both ends; wider, the rows with both ends,
  each end's missing count and p95. <kbd>Enter</kbd> opens the detail, from the
  measurements the report holds:

  | Row | Counts |
  |---|---|
  | Start, End | The role and its column, and whether it is text read as time |
  | Segment | The segment and the grain that cut it, by the window clock |
  | Rows | Rows in the segment |
  | Both ends | Rows with both ends present and read: the denominator below |
  | Missing start, Missing end | Null in the source, of the segment's rows |
  | Unparsed start, Unparsed end | Text the format did not read, of the segment's rows; only for text read as time |
  | Negative, Zero | End before start, and end at start, of the rows with both ends |
  | p50, p90, p95, p99, Maximum | Durations, in whole seconds |
  | Threshold, Over | `duration > threshold`, strictly, of the rows with both ends |

  <kbd>↑</kbd> <kbd>↓</kbd> move between the counts, and <kbd>Enter</kbd>
  shows the rows behind the one under the cursor, cut from the rows the run
  kept: on a sampled run the sample's. When the run kept none (a full scan,
  or a sample since released), a **Read Rows** dialog says what reading them
  takes first, as for a finding (see [Evidence](#evidence)). A row chunk or a
  file is not a value to filter on, so its rows do not open, and the detail
  says so. <kbd>Esc</kbd> returns to the list. With nothing to show, the page
  says why; when roles or a pair are missing, <kbd>Enter</kbd> opens Time
  roles or Intervals in Setup.

### Expected windows and gaps

A window with no rows is a gap only against a stated expectation. **Expected**
in Setup, with a time-window grain, says which windows rows belong in: every
window, or Monday to Friday's hours or days, from **From** (floored to its
window) and before **Before**; either blank takes the first or last window the
run found. Weekend windows under weekdays only are counted apart, never as
gaps. Each expected window with no sampled row is one of:

| Gap | When |
|---|---|
| empty | Not among the run's segment counts: no rows in the scope. Only where the counts are exact: every row read, or every window counted |
| not sampled | The count has rows in it and the sample drew none; the rows are given. With no count, every window without a sampled row is this, said as not counted |
| out of scope | The scope is a time range on the grain's column, and the window is not wholly inside it |

Gaps are checked from the counts the run takes for exact segment totals, so
stating them reads nothing; changing only Expected re-labels the report on
screen. Consecutive windows of one kind are listed as one run (across the
weekends not expected), the windows first. At most 20,000 windows are checked;
a longer range is refused whole, never checked in part, and at most 500 runs
are listed, the rest counted. Windows are cut in UTC, as the grain cuts them.

<a id="sampling-and-budgets"></a>

## Sampling

Every grain reads the shared [sample](../user-guide/analysis-features.md#sampling), the same rows every tool
reads, and splits it into segments. Only a full scan (the sample's method set
to Every row) asks first: the **Full Scan** dialog, <kbd>Enter</kbd> to run,
<kbd>Esc</kbd> to cancel.

| | |
|---|---|
| Dataset grain | The whole sample is one segment |
| File, partition, chunk, window grain | The sample's rows, split by the segment each came from. A **Random** sample gives each segment its share, so a small one gets few rows; **Equal per value** of the partition column gives every segment the same number. Choosing Equal per value sets the grain to that column when no grain is set. A segment's total comes from what is already known (a file's rows from its footer when whole files are in scope, a row chunk's size, the rows an Equal per value sample counted while it read), from the count a streamed sample takes of the grain's column in its one pass, from a finer window's count summed, and otherwise from one count of the grain's column, kept with the rows |
| Row chunks | Use the selected scope's physical order; sampled rows keep their original chunk labels |
| Time windows | By hour, day, week or month of a date or time column, starting on the calendar boundary for their width (weeks start on Monday) and named by where they start (`2024-01-31`, `week of 2024-01-29`, `2024-01`); a zoned column is cut and named in UTC; a window is cut at the same place whether sampled or scanned |
| File mapping | Available on source scopes and on views that preserve source-row provenance; otherwise Segments says it is unavailable |
| Remote sources | Read-only; the access plan always reports zero remote writes |

### What is reused

A sampled run keeps the rows it read, every column of them and where each row
sat in the scope. Those rows are keyed by what chose them: the dataset as
opened, the view, and the sample's scope, method, size and seed. Everything
else in Setup belongs to the report.

| Edit | Reads |
|---|---|
| Time roles, text as time on a role, column intent, latency threshold | Nothing: the retained rows are measured again |
| Compare, expected windows | Nothing: worked out from the report on screen, whether sampled or a full scan |
| Row chunks | Nothing: each row's position was kept |
| A coarser window of a grain already counted | Nothing: hours sum into days, weeks and months, and days into weeks and months |
| Another partition, a finer window, or a window on newly read text | One count of the grain's column, kept with the rows |
| Scope, method, size or seed | A new sample, unless the rows for that sample are still held |
| Nothing changed, or a report in the session cache | Nothing |

Windows are cut on the stored clock with no time zone (UTC for a zoned
column), where every hour lies in one day and every day in one week and one
month, so a sum of the finer counts is exactly the coarser count. A week is
not summed into months.

Retained rows and reports share a 256 MiB budget for the session. Past it,
reports that retained rows can remake go first, then the oldest rows; the newest
rows and the newest report always stay. Setup names rows that were released and
will be read again. The rows are a snapshot of the session: a file changed on
disk is not noticed until it is opened again.

## Column intent

The profile can say a column is nearly unique; only you can say it is a key.
Column intent declares what columns must hold, and a run counts what breaks it.
Every rule is optional, and nothing is declared until you declare it. The
Column intent row in Setup lists the scope's columns with what each must hold;
<kbd>Space</kbd> opens a column's form.

| Rule | Takes | Offered for |
|---|---|---|
| Key | The columns whose values together name one row, in any number | Every column |
| Required | Every row has a value | Every column |
| Read as | Whole number or decimal: text read as a number for the range, and text that does not read is counted | Text; text read as time takes its reading from Text as time |
| Allowed | Values separated by commas, outer spaces dropped, up to 100. A value in double quotes is kept as typed: `"a, b"` holds a comma, `" open"` a space, and `""` is a quote inside one | Text, whole numbers, true/false |
| Minimum, Maximum | A number; a date `2024-01-31`; or a date and time `2024-01-31 08:00:00`. On a date and time column, a date alone as the maximum takes in its whole day | Numbers, dates and times, and text read as either |

A bound the column's type cannot read, or a minimum above the maximum, is said
on the form's own line, and <kbd>Enter</kbd> does not apply it. A declared
column the scope does not have is named under Columns before Run and in the
report's Limits.

Intent is measured on the rows a run reads, in the same pass as the column
profile: on a sample, over the sample in memory, so the rows a run already read
serve a new declaration with no read; on a full scan, the rules are sums in the
profile pass, and the key is one grouping of its columns, a pass of its own
that Setup counts before Run.

| Run | A key with no repeat says |
|---|---|
| Every row | The key is unique in the scope |
| A sample | No repeat among the sampled rows. Sampled rows are distinct rows, so a repeat found is a repeat in the data, but rows outside the sample are not checked; the coverage says `key repeats among N sampled rows only` |
| Metadata only | Nothing: the check is unavailable, values not read |

Each violation is a problem with its count and what it is out of, and
<kbd>Enter</kbd> on it opens its rows as any finding's open: from the rows the
run kept, or on a full scan after a **Read Rows** dialog. Out of range names the lowest value below the range and
the highest above it. When the rows are in memory (a sample, or a scope no
larger than it), Not allowed and Unparsed numbers also list their commonest
values with their rows; a full scan keeps no rows to list them from. A
one-column key replaces the Nearly unique note on that column: its repeats are
counted by the key's own finding, and on an exact run the Nearly unique check
reports them.

## Exported report

<kbd>x</kbd> on a report page writes the report on screen to a file, from the
results in memory: nothing is read, and the source may be gone. The dialog
takes a path and a format; the path gets the format's extension when it has
none, changing the format changes a typed `.json` to `.md` or back, and a typed
`.json` or `.md` writes that format whatever the Format row says. A file
that exists is overwritten only once confirmed, and declining keeps the dialog.

| Format | Holds |
|---|---|
| JSON | Every measurement below, versioned |
| Markdown | The source, rows measured, the verdict, coverage, each finding with its headline and evidence, the checks, the intervals, the gaps and the setup |

The JSON is one object. `format` is always `datui-data-quality-report`, and
`version` is `1`. A field may be added within a version; one removed, renamed or
changed in meaning is a new version.

| Field | Holds |
|---|---|
| `format`, `version` | `datui-data-quality-report`, `1` |
| `datui_version` | The datui that wrote it |
| `exported_at` | When the file was written, RFC 3339 in UTC; not when the data was read |
| `source` | `location` (the URL as opened, or the local path made absolute), `remote`, `format`, `files` and up to 100 `file_names` for a dataset of several files, `bytes` and `modified` (RFC 3339, UTC) of a local file as the run that read the rows began (a report remade from rows a run kept keeps that run's), and `view`: the query, SQL, search, filters and reshape a view scope measured. No content hash: that would be a read. `null` for a report no run labeled |
| `setup` | `scope`, `values` (`sample`, `full` or `metadata`), `sample` (`method`, `rows`, `seed`), `grain`, `comparison`, `baseline_segment`, `time_formats` (`column`, `kind`, `format`), `time_roles` (`role`, `column`), `intervals`, `window_by`, `latency_threshold_seconds`, `intent` (`key`, and per column `column`, `required`, `allowed`, `min`, `max`, `read_as`), and `expected` (`weekdays`, `from`, `before`, as typed), `null` when no windows are stated |
| `run` | `precision` (`exact`, `sampled` or `metadata`), `total_rows`, `evaluated_rows`, `per_value`, `source_files`, `footers_read`, and `reads` (`source_reads`, `counted`, `rows_traversed`) when the run's reads were watched |
| `verdict` | The headline, as on screen |
| `coverage` | `exact`, `sampled`, `metadata`, `skipped`, `unavailable` (`reason`, `checks`), `rows`, `limits` |
| `checks` | Per check: `name`, `looks_for`, `applies_to`, `outcome` (`passed`, `found`, `skipped`, `unavailable`), `detail`, `basis` |
| `findings` | Per finding: `severity` (`problem`, `note`, `clean`), `title`, `columns`, `affected_rows`, `evaluated_rows`, `summary`, `headline`, `evidence` |
| `columns` | Per column: `name`, `dtype`, `evaluated_rows`, `null_count`, `distinct_count`, `empty_count`, `whitespace_count`, `nan_count`, `positive_infinity_count`, `negative_infinity_count`, `min`, `max`, `dominant_value`, `dominant_count`, `min_length`, `max_length`, and the integer, decimal, date and datetime parse counts |
| `duplicates` | `groups`, `extra_rows`, `rows_involved`, `evaluated_rows` |
| `segments` | Per segment: `label`, `total_rows`, `evaluated_rows`, `null_cells`, `null_rate`, `compared_with`, `largest_change` |
| `intervals` | Per interval and segment: `interval`, `segment`, `start_column`, `end_column`, `rows`, `both_ends`, `missing_start`, `missing_end`, `unparsed_start`, `unparsed_end`, `negative`, `zero`, `p50_seconds` to `p99_seconds`, `max_seconds`, `threshold_seconds`, `over_threshold` |
| `intent` | `null` when nothing is declared; otherwise `measured`, `precision`, `evaluated_rows`, `key` (`columns`, `missing`, `groups`, `extra_rows`, `rows_involved`), per column `column`, `dtype`, `values`, `missing`, `unparsed`, `outside`, `compared`, `below`, `above`, `lowest`, `highest`, and `absent` |
| `gaps` | `null` when no windows are stated; otherwise `status` (`checked`, `no_values`, `no_windows`, `too_many`), `column`, `every`, `cadence`, `windows_in_range` (for `too_many`), and when checked `from` and `before` (UTC), `expected`, `weekend`, `with_rows`, `empty`, `not_sampled`, `out_of_scope`, `counted`, `runs` (`kind`, `first`, `last`, `span`, `windows`, `rows`) and `more_runs` |

A number not measured is `null`, never `0`. With the setup, the source and the
seed, the same datui draws the same sample and measures the same numbers from
data that has not changed.

## Data-quality metric definitions

| Metric | Formula and read |
|---|---|
| Null rate | Null values ÷ evaluated rows; reads the selected column |
| Empty / whitespace rate | Exact empty or trim-to-empty strings ÷ evaluated rows; reads string values |
| NaN / infinity | Separate counts for NaN, positive infinity and negative infinity; reads floating-point values |
| Distinct | Distinct non-null values observed in the evaluated rows; sampled runs do not claim dataset-wide uniqueness |
| Dominant share | Most frequent non-null value count ÷ evaluated non-null rows |
| Range / length | Minimum and maximum value, character length for text, or element count for lists |
| Parse share | Values accepted by the named integer, decimal, ISO-date or ISO-datetime parser ÷ evaluated non-null text values; a text column is reported at 95% or more, once, as its most specific reading |
| Shared missing rows | For columns with the same null count, the rows null in all of them; equal to the count means the same rows |
| Duplicate groups | Groups of identical complete evaluated rows; extra rows is Σ(group size − 1), rows involved is Σ(group size) |
| Category variants | Original text values that become equal after outer-whitespace removal and lowercase normalization |
| Nearly unique | Non-null rows − distinct values, on exact profiles of whole-number and text columns only, reported when distinct values are at least 95% of non-null rows and at least one value repeats. That counts rows beyond one per value; the drill-in opens every row that shares one, which is always more |
| Absent values | Rows held by files whose footer has no such column ÷ rows in the loaded source; read from footers, not values |
| Type conflicts | Rows held by files that store the column in a type the scan cannot read ÷ rows in the loaded source; read from footers, not values |
| Segment null rate | Null cells ÷ (evaluated rows × profiled logical columns) in that segment |
| Trend bar rate | Σ count ÷ Σ denominator over the bar's segments with sampled rows; rows per segment is Σ rows ÷ segments, a segment the sample missed counting zero sampled rows |
| 95% interval | Wilson score interval at z = 1.96 on a bar's count of its denominator: centre (p + z²/2n) ÷ (1 + z²/n), half-width z·√(p(1−p)/n + z²/4n²) ÷ (1 + z²/n). It assumes a simple random sample; seeded runs of one file are clustered, so read it as a floor on the uncertainty there |
| Bar change | Against the previous bar, or the baseline's: clear at 1 pp or more and, on a sample, the two-proportion z-test at 4 or more standard errors; a distinct share is shown, not judged, as on Segments |
| Largest change | Against the compared segment: a row count that halved or doubled, else the biggest percentage-point move in any column's null, empty, blank or NaN rate, named when it reaches 1 pp and, on a sample, when a two-proportion z-test puts it at 4 or more standard errors; on an exact profile with no such move, the first column whose minimum or maximum moved |
| Lifecycle latency | End role timestamp − start role timestamp per row, on rows with both ends present and read; each end's missing count is of all rows (a row can miss both, so both ends is counted, not derived), text the format does not read is counted apart from missing, and negative values are retained |
| Negative / zero / breach | Durations below zero, of exactly zero, and above the threshold (`duration > threshold`, strictly) ÷ rows with both ends; compared on the exact difference, so half a second early is negative |
| Unparsed times | Non-null text values the chosen format does not read ÷ non-null values of the column; counted in the pass that profiles the columns |
| Repeated key | Rows whose complete key value another row also holds ÷ rows checked; groups are key values held by more than one row, extra rows Σ(group size − 1). Rows missing part of the key are left out and counted as Incomplete key |
| Required, missing | Null values ÷ rows checked |
| Not allowed | Non-null values not exactly equal to one in the set ÷ non-null values; text compared as stored, so case and spaces count; whole numbers as numbers |
| Out of range | Values `< minimum` or `> maximum` ÷ values read; a bound is inclusive. Times compare as instants in UTC, a time with no zone read as UTC |
| Unparsed numbers | Non-null text that does not cast to the declared number ÷ non-null values, as Numbers as text parses |

Lifecycle percentiles use the evaluated duration values in sorted order. Date
values are interpreted at midnight; datetime values retain their physical time
unit. The role mapping is a user assertion and is included in the visible plan.

A nearly unique column is reported only from an exact profile. A null rate measured
on a sample stands for the whole; a distinct count does not, and an identifier
that repeats ten times in a billion rows is unique in every sample of it.

## Columns a file never had, and columns it holds in another type

Absent columns and type conflicts come from the footers datui read when the
dataset opened, not from values, so they are reported whatever the sample
reads, even with values not read, and counted over the whole loaded source.
The finding names the files by number, as the Sample form's Files list numbers
them, and <kbd>Enter</kbd> opens the rows those files contributed. Where footers were sampled, both counts are a floor, and the measured
fact says how many footers were read. A full scan also reads the first five
values each conflicting file holds at the type it wrote; the access plan's
Conflict values row states how many extra reads that costs. The
[Dataset Info](../user-guide/dataset-info.md#notes) notes report the same facts at open time
and offer to read a conflicting column as text.
