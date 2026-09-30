# Data quality reference

For a first check, follow [Check data quality](../user-guide/data-quality.md).
Data Quality reads the shared analysis [sample](../user-guide/analysis-features.md#sampling), as its [Setup](#setup) says.

## Reading the report

The first line counts problems, notes and clean columns. Below it, findings
are grouped under **Problems**, **Notes** and **Clean**. <kbd>Enter</kbd> on a
finding lists its numbers, evidence and what to check; <kbd>Enter</kbd>
again shows its rows. On a sampled run those are the sample's rows, drawn again
from its seed, so the count the finding gave is the count in the table.

<kbd>Enter</kbd> on the **Clean** entry lists the checks the run made: the
columns each covered, what it found, or why it did not run (Nearly unique
needs every row checked; the file checks need a source of several files).
The six most important show first and <kbd>Enter</kbd> shows all ten. When a
run finds nothing, the list is on the page under the verdict.

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
| <kbd>←</kbd> <kbd>→</kbd> | Change Grain, Compare, Values or Latency in place; on another row, <kbd>→</kbd> opens it |
| <kbd>Space</kbd> | Open the row: the Sample form, the Time roles editor, or a list of choices (type to narrow, <kbd>Enter</kbd> chooses) |
| <kbd>s</kbd> | Open the Sample form; its <kbd>Enter</kbd> applies the sample to Setup and returns there |
| <kbd>p</kbd> | Show the access plan |
| <kbd>Enter</kbd> | Run, from any row |
| <kbd>Esc</kbd> | Discard every staged change, the sample's included, and go back to the report, or to the tool list before the first |

Run commits the setup: its sample becomes the one every tool reads, and the
other tools' results, taken from the old sample, go. A setup the report on
screen was measured with shows that report again, and one in the session cache
shows its cached report; neither reads. A full scan asks first, and
<kbd>Esc</kbd> there leaves the draft staged and the sample and report as they
were. While a cancelled read is still finishing, Run waits and Setup says why.

| Row | Choices |
|---|---|
| Sample | The shared sample: scope, method, rows and seed |
| Text as time | Text columns read as a date or datetime through a chosen format, for this study only |
| Time roles | Event, effective/as-of, period end, created, published, received, processed, valid from, and valid to |
| Grain | Whole dataset; by file, when the dataset has several; by each partition column; by hour, day, week or month of any date or time column, or text read as time (hours only where there are times); or in chunks of 100,000 or 1,000,000 rows |
| Compare | None, the segment before (partitions and files in the order their names count), or a baseline segment |
| Values | Read, or metadata only (footers, no values); whether a read is sampled or every row is the sample's method |
| Latency over | None, 1 hour, 1 day or 1 week; offered once two roles make an interval |

The Read section says what Run will do, before it does it:

| Read | When |
|---|---|
| No read: the report is already here | The setup is the report's, or the session cache holds it |
| Uses the rows the last run read | A sampled setup whose sample, dataset and view match the last run's, and whose grain those rows serve |
| Seeded runs of the file | A random sample of one Parquet or IPC file as loaded: a few dozen short reads |
| One pass that streams every eligible row | Any other random or equal-per-value sample; the pass counts the scope too |
| Plus one count of the grain's column | A sampled partition or time-window grain whose segment totals nothing has counted; kept for later runs |
| Every eligible row, in up to N passes | A full scan: one collect per check, and one more to count an unknown scope |
| File metadata only | Values set to metadata only |

Before a run, source-scoped setups report unknown row counts and read sizes,
and metadata-only runs do not count rows. A local sample estimates its read as
a ceiling (`up to`); the run reports the exact eligible count. The scope is
applied before sampling, so sampling never reaches beyond its bounds.
<kbd>p</kbd> shows the access plan: source, scope, grain, sample, rows
evaluated, value reads, requests, source files, the extra reads a type conflict
costs, writes (none), the passes, and the estimate basis. Remote bytes and
requests are unknown until measured, and say so.

The Time roles row opens an explicit mapping table; every role starts
unassigned. Under it, each date, time and text column is listed with its type,
or the format a text column is read with, and a few of its values from the rows
on screen; the column the focused role holds is marked. Datui recognizes
physical date and datetime types but never guesses a column's meaning from its
name. With a source scope, the list includes source columns hidden by the
current view. Intervals are measured for these pairs: event to published,
received or processed; period end to published; published to received; and
received to processed. Setup lists the pairs the roles make, or says that they
make none.

### Text as time

<kbd>Space</kbd> on Text as time lists the scope's text columns with a value
from the rows on screen. Choosing one lists the formats: `%Y-%m-%d %H:%M:%S`,
`%Y-%m-%dT%H:%M:%S`, with fractional seconds, `%Y-%m-%d %H:%M`,
`%m/%d/%Y %H:%M:%S`, `%m/%d/%Y %I:%M:%S %p`, `%d/%m/%Y %H:%M:%S`,
`%d.%m.%Y %H:%M:%S`, and the dates `%Y-%m-%d`, `%Y%m%d`, `%m/%d/%Y`,
`%d/%m/%Y`, `%d.%m.%Y`. Each says how many of the values on screen it reads,
and those that read the most come first. **text, not a time** takes the format
away, and a time-window grain on the column with it.

| | |
|---|---|
| Applies to | Grain and time roles. Every other check, and the column's own findings, see the stored text |
| Time zone | None: values are read as local times, with no offset |
| Values it does not read | Counted per column as **Unparsed times**, a problem, apart from missing values; in intervals, as unparsed starts and ends; in a time-window grain, with the rows that have no time |
| Not applied to | The sample's time range and an equal-per-value sample, which read date and time columns as stored |

## Keys and result tabs

The control bar has one shape on every report page: <kbd>Esc</kbd> Back, the
page's own action, then <kbd>e</kbd> Setup, <kbd>s</kbd> Sample,
<kbd>←</kbd> <kbd>→</kbd> Page and <kbd>v</kbd> View Rows.

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Previous or next page: Overview, Columns, Segments, Trends |
| <kbd>Enter</kbd> | Open a finding, or show its rows; on an empty Segments or Trends page, open the Setup row that fills it |
| <kbd>e</kbd> | Open Setup |
| <kbd>s</kbd> | Open Setup with the shared [Sample](../user-guide/analysis-features.md#sampling) form over it |
| <kbd>p</kbd> | Show the detailed access plan |
| <kbd>1</kbd>–<kbd>4</kbd> | Overview, Columns, Segments, Trends, directly |
| <kbd>o</kbd> | On Segments, list the largest change first, or back in order |
| <kbd>m</kbd> | Cycle the measure Trends draws: null, empty, whitespace, non-finite, distinct, integer-parse, decimal-parse |
| <kbd>b</kbd> | Use the highlighted segment as the comparison baseline; deltas update without another data read |
| <kbd>r</kbd> | On a sampled report, run again with a new sample seed, for every tool; waits while a cancelled read finishes |
| <kbd>Tab</kbd> | Move between the result and the tool list |
| <kbd>Esc</kbd> | Back one level; from a page, close Analysis |

### While a run reads

The progress names the stage the run is in (preparing, reading the sample or
reusing the last one, counting rows or segment rows, profiling columns,
checking duplicates and spellings, profiling segments, computing intervals,
assembling the report), whether that stage reads the source or works on rows
already read, and the rows seen where the read can count them. There is no
percentage: a run has no total to measure against.

<kbd>Esc</kbd> cancels. A streamed sample stops at its next batch, and a run
stops between stages; a single read in the middle of a stage runs to its end.
Until the worker exits, the header and Setup say
`Cancellation requested; source read finishing`, and Run and <kbd>r</kbd> wait
rather than start a second read beside it. The last report stays, labeled with
the setup it was measured with, and Setup opens on the setup that was running.
A sample read before the cancel is kept for the next run.

Every result states eligible and evaluated rows and whether values are exact,
sampled, or metadata-only.

- **Overview** — the report. <kbd>Enter</kbd> on a finding lists its numbers,
  what to check and its evidence: the spellings, the most
  repeated value, the files behind a missing or mistyped column and the values
  a conflict hides, values with the most rows first. A finding taller than the
  screen scrolls with <kbd>↑</kbd> <kbd>↓</kbd>. <kbd>Enter</kbd> again opens the matching rows in a
  temporary table (every column's rows, for a grouped finding): from the table
  when every row was read, from the sample when one was. <kbd>Esc</kbd> returns
  to the report.
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
  Columns whose measure moves most come first, and columns that draw the same
  line, such as columns missing together, share one. <kbd>m</kbd> changes the
  measure. Below, the time between dates for assigned role pairs: missing
  endpoints, negative durations, p50/p90/p95/p99, and maximum duration. Each
  part says what it needs when it has nothing, and <kbd>Enter</kbd> opens it in
  Setup: Time roles when the data has date columns or text read as time, Grain
  otherwise.

<a id="sampling-and-budgets"></a>

## Sampling

Every grain reads the shared [sample](../user-guide/analysis-features.md#sampling), the same rows every tool
reads, and splits it into segments. Only a full scan (the sample's method set
to Every row) asks first: the **Full Scan** dialog, <kbd>Enter</kbd> to run,
<kbd>Esc</kbd> to cancel.

| | |
|---|---|
| Dataset grain | The whole sample is one segment |
| File, partition, chunk, window grain | The sample's rows, split by the segment each came from. A **Random** sample gives each segment its share, so a small one gets few rows; **Equal per value** of the partition column gives every segment the same number. Choosing Equal per value sets the grain to that column when no grain is set. A segment's total comes from what is already known (a file's rows from its footer when whole files are in scope, a row chunk's size, the rows an Equal per value sample counted while it read), and otherwise from one count of the grain's column, kept for the session |
| Row chunks | Use the selected scope's physical order; sampled rows keep their original chunk labels |
| Time windows | By hour, day, week or month of a date or time column, starting on the calendar boundary for their width (weeks start on Monday) and named by where they start (`2024-01-31`, `week of 2024-01-29`, `2024-01`); a window is cut at the same place whether sampled or scanned |
| File mapping | Available on source scopes and on views that preserve source-row provenance; otherwise Segments says it is unavailable |
| Remote sources | Read-only; the access plan always reports zero remote writes |

Complete profiles are reused during the session when the dataset, current
view, and full setup (including sample seed, time roles and text formats)
match; the session keeps the four most recent. Reopening Data Quality then
shows the cached result without reading values again; changing the view or the
setup requires a new run. A sampled run that differs from the last only in how
it cuts the rows, such as grain, roles or text formats, cuts the rows the last
run read instead of reading them again.

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
| Largest change | Against the compared segment: a row count that halved or doubled, else the biggest percentage-point move in any column's null, empty, blank or NaN rate, named when it reaches 1 pp and, on a sample, when a two-proportion z-test puts it at 4 or more standard errors; on an exact profile with no such move, the first column whose minimum or maximum moved |
| Lifecycle latency | End role timestamp − start role timestamp per row; missing endpoints are counted separately, text the format does not read is counted apart from missing, and negative values are retained |
| Unparsed times | Non-null text values the chosen format does not read ÷ non-null values of the column; counted in the pass that profiles the columns |

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
