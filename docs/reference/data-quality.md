# Data quality metrics

What each Data Quality finding, count and measure means, and what an exported
report holds. For a walkthrough, see [Check data quality](../user-guide/data-quality.md).
The sample every run reads is described in [Analysis](../user-guide/analysis-features.md#sampling),
and the keys in [Keyboard shortcuts](keyboard-shortcuts.md#analysis-data-quality).

## Findings

Problems are listed above notes. A column with neither is clean.

| Finding | Tier | Means |
|---|---|---|
| NaN or infinite | Problem | Float values that are NaN or ±infinity; one NaN makes a sum or mean NaN |
| Empty text / Blank text | Problem | Text that is `""` or only whitespace: it looks filled in but carries nothing |
| Mixed spellings | Problem | Values equal after trimming and lowercasing, such as `"West"` and `"west "` |
| Duplicate rows | Problem | Rows identical in every column |
| Always missing | Problem | A column with no value in any row checked |
| Missing in files / Type mismatch | Problem | Files without the column, or holding it in a type the dataset cannot read |
| Mostly missing | Note | Columns with no value in more than half the rows checked, listed above other missing values |
| Missing together | Note | Columns only ever null together: no row misses one without the others, so one cause is likely |
| Missing values | Note | Nulls, as one finding with each column's rate inside it, highest first |
| Numbers as text / Dates as text | Note | At least 95% of a text column parses as numbers or ISO dates |
| Codes as text | Note | Whole numbers with leading zeros or a fixed width: a code, fine as text |
| Nearly unique | Note | A whole-number or text column at least 95% unique whose values still repeat; if it is meant to be a key, the repeats are duplicates |
| Clipping | Problem | Audio: runs of 3 or more samples at full scale, the waveform cut flat at the limit |
| Runs of zeros | Note | Audio: runs of exact zeros 10 ms or longer (16 samples at least): dropouts, or digital silence at the ends |
| DC offset | Note | Audio: a channel whose mean is 1% of full scale or more from zero |
| Unparsed times | Problem | Text read as time whose values the chosen format does not read; <kbd>Enter</kbd> opens their rows |
| Single value | Note | One value in every row checked |
| Repeated key | Problem | Rows sharing a value of the declared key; see [Column intent](#column-intent) |
| Incomplete key | Problem | Rows with no value in some part of the declared key |
| Required, missing | Problem | Rows with no value in a column declared required |
| Not allowed | Problem | Values outside a column's declared allowed set |
| Out of range | Problem | Values below a column's declared minimum or above its maximum |
| Unparsed numbers | Problem | Text declared to read as a number that does not |

### What a finding counts

<kbd>Enter</kbd> on a finding opens the rows below. A finding over several
columns counts `Rows with any of them`: the largest column's count when that
covers them all, otherwise a range from that count to their sum. **Missing
together** columns are null on the same rows, so their count is exact.

| Finding | Rows it opens | Count shown | Examples in the detail |
|---|---|---|---|
| Duplicate rows | Every row equal to another in every column; copies together, most copied first | Rows with a copy | The three most copied rows, with their copies |
| Numbers, Dates or Codes as text | Non-null text the reading does not parse | Non-null values less those that parse | Up to three values that do not parse |
| Unparsed times | Text the chosen time format does not read | Unparsed values | Up to three of them |
| Nearly unique | Every row whose value repeats | Rows beyond one per value; more open | The most repeated value |
| Missing in files, Type mismatch | Every row of the named files | Rows of those files | The files and the values a conflict hides |
| Repeated key | Every row whose declared key another row also holds | Rows sharing a key value | |
| Clipping, Runs of zeros | Every sample at full scale, or every exact zero, in the channel | Samples in the runs | Each channel's runs |
| Any other | Rows matching the check | The finding's rows, or a range for grouped columns | |

## Coverage

Under the verdict, every report says what it covered:

| Coverage line | Says |
|---|---|
| Checks | The ten checks, plus Column intent when any is declared, grouped by what they read: `exact` (every row in scope), `sampled` (the sample) or `metadata` (file footers). Then `skipped`: checks with nothing in the data to look at (no float column, one file). Then `unavailable`: checks that apply but this run could not answer (values not read, no rows in the scope, or a sample where the answer needs every row). A scope with no rows calls no column clean; its verdict is `No rows to check` |
| Rows | Rows read of the total: `100,000 of 36,839,175 sampled (0.27%)`, `all 1,204 read, exact`, `none: the scope has no rows`, or `none read, file metadata only`; `up to 500 per value` for an Equal per value sample; then the rows the run's reads passed through, summed over every pass, when the reads counted them: `36,839,175 traversed`, `at least …` when some read could not count, `no source read` when the run used rows already read; and `passes read a local copy, fetched once (16.5 MiB)` or `… fetched earlier` when a full scan read one |
| Limits | Why each unavailable check did not run; segments with fewer than 30 sampled rows (`4 of 31 segments under 30 sampled rows`); segments the scope has rows in and the sample drew none of (`3 segments with rows, none sampled`); `footers of 200 of 5,000 files read` on a dataset too large to read every footer, where the file checks cover only those; `time roles form no interval`; `key repeats among 10,000 sampled rows only` for a declared key on a sample; `intent on code: not in scope` |

## Data-quality metric definitions

| Metric | Formula and read |
|---|---|
| Null rate | Null values ÷ evaluated rows; reads the selected column |
| Empty / whitespace rate | Exact empty or trim-to-empty strings ÷ evaluated rows; reads string values |
| NaN / infinity | Separate counts for NaN, positive infinity and negative infinity; reads floating-point values |
| Distinct | Distinct non-null values observed in the evaluated rows; sampled runs do not claim dataset-wide uniqueness |
| Dominant share | Count of the most frequent non-null value ÷ evaluated non-null rows |
| Range / length | Minimum and maximum value, character length for text, or element count for lists |
| Parse share | Values accepted by the named integer, decimal, ISO-date or ISO-datetime parser ÷ evaluated non-null text values; a text column is reported at 95% or more, once, as its most specific reading |
| Shared missing rows | For columns with the same null count, the rows null in all of them; equal to the count means the same rows |
| Duplicate groups | Groups of identical complete evaluated rows; extra rows is Σ(group size − 1), rows involved is Σ(group size) |
| Category variants | Original text values that become equal after outer-whitespace removal and lowercase normalization |
| Nearly unique | Non-null rows − distinct values, on exact profiles of whole-number and text columns only, reported when distinct values are at least 95% of non-null rows and at least one value repeats. That counts rows beyond one per value; the drill-down opens every row that shares one, which is always more |
| Absent values | Rows held by files whose footer has no such column ÷ rows in the loaded source; read from footers, not values |
| Type conflicts | Rows held by files that store the column in a type the scan cannot read ÷ rows in the loaded source; read from footers, not values |
| Clipping, runs of zeros, DC offset | Audio files only, on a full run over the whole source or an untouched view: one more pass reads every sample of the file. A run at full scale is 3 or more samples at the most positive or negative value the valid bits allow, or at ±1.0 for float; a run of zeros is 10 ms or longer and at least 16 samples; the offset is the channel's mean ÷ full scale |
| Segment null rate | Null cells ÷ (evaluated rows × profiled logical columns) in that segment |
| Trend bar rate | Σ count ÷ Σ denominator over the bar's segments with sampled rows; rows per segment is Σ rows ÷ segments, where a segment the sample missed counts as zero sampled rows |
| 95% interval | Wilson score interval at z = 1.96 on a bar's count of its denominator: center (p + z²/2n) ÷ (1 + z²/n), half-width z·√(p(1−p)/n + z²/4n²) ÷ (1 + z²/n). It assumes a simple random sample; seeded runs of one file are clustered, so read it as a floor on the uncertainty there |
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

## Setup rows

| Row | Choices |
|---|---|
| Sample | The shared sample: scope, method, rows and seed |
| Text as time | Text columns read as a date or datetime through a chosen format, for this study only |
| Time roles | Event, effective/as-of, period end, created, published, received, processed, valid from, and valid to |
| Intervals | Which starts and ends are measured, from every pair the assigned roles make; offered once two roles are assigned |
| Column intent | What columns must hold: the key, and per column required, allowed values, a range, or text read as a number. See [Column intent](#column-intent) |
| Grain | Whole dataset; by file, when the dataset has several; by each partition column; by hour, day, week or month of any date or time column, or text read as time (hours only where there are times); or in chunks of 100,000 or 1,000,000 rows |
| Expected | With a time-window grain: none, every window, or weekdays only (hours and days); From and Before, a date or UTC timestamp each, blank for the first and last window found. See [Expected windows and gaps](#expected-windows-and-gaps) |
| Compare | None, the segment before (partitions and files in name order, with numbers in names compared as numbers), or a baseline segment |
| Values | Read, or metadata only (footers, no values). Whether a read is sampled or reads every row is set by the sample's method |
| Latency over | None, 1 hour, 1 day or 1 week; offered once there is an interval. A breach is `duration > threshold`, strictly |
| Window by | With a time-window grain and an interval: the grain's column, each interval's start, or each interval's end, which puts a delay across midnight on the day it ended |

### Time roles and intervals

| | |
|---|---|
| Valid from to valid to | A validity period: a missing end is **open**, and an end before its start **ends first**. Overlaps and gaps between periods need an entity key and consecutive rows, which a sample does not hold, so they are not counted |
| Window by | Only with a time-window grain. Windowed by start or end, intervals are grouped once for each column they start or end on. On a full scan each grouping is a pass, and the Read line says how many before Run |
| Time zones | A datetime with a zone, or text read with an offset, is its instant in UTC. A date or datetime with no zone is read as if it were UTC, and Setup says so when it meets a zoned one. Windows start on UTC boundaries |

### Text as time

The formats text can be read as: `%Y-%m-%d %H:%M:%S`,
`%Y-%m-%dT%H:%M:%S`, with fractional seconds, with an offset
(`%Y-%m-%dT%H:%M:%S%.f%#z` and `%Y-%m-%d %H:%M:%S%.f%#z`, which read `Z`,
`+05:00`, `-0500` and `+05`), `%Y-%m-%d %H:%M`,
`%m/%d/%Y %H:%M:%S`, `%m/%d/%Y %I:%M:%S %p`, `%d/%m/%Y %H:%M:%S`,
`%d.%m.%Y %H:%M:%S`, and the dates `%Y-%m-%d`, `%Y%m%d`, `%m/%d/%Y`,
`%d/%m/%Y`, `%d.%m.%Y`.

| | |
|---|---|
| Applies to | Grain and time roles. Every other check, and the column's own findings, see the stored text |
| Time zone | With an offset format, each value is its instant in UTC; without one, a time with no zone, read as UTC beside a zoned one |
| Values it does not read | Counted per column as **Unparsed times**, a problem, apart from missing values; in intervals, as unparsed starts and ends; in a time-window grain, with the rows that have no time |
| Not applied to | The sample's time range and an equal-per-value sample, which read date and time columns as stored |

### Grains

| | |
|---|---|
| Dataset grain | The whole sample is one segment |
| File, partition, chunk, window grain | The sample's rows, split by the segment each came from. A **Random** sample gives each segment its share, so a small one gets few rows; **Equal per value** of the partition column gives every segment the same number. Choosing Equal per value sets the grain to that column when no grain is set. A segment's total comes from what is already known (a file's rows from its footer when whole files are in scope, a row chunk's size, the rows an Equal per value sample counted while it read), from the count a streamed sample takes of the grain's column in its one pass, from a finer window's count summed, and otherwise from one count of the grain's column, kept with the rows |
| Row chunks | Use the selected scope's physical order; sampled rows keep their original chunk labels |
| Time windows | By hour, day, week or month of a date or time column, starting on the calendar boundary for their width (weeks start on Monday) and named by where they start (`2024-01-31`, `week of 2024-01-29`, `2024-01`); a zoned column is cut and named in UTC; a window is cut at the same place whether sampled or scanned |
| File mapping | Available on source scopes and on views that preserve source-row provenance; otherwise Segments says it is unavailable |
| Remote sources | Read-only; the access plan always reports zero remote writes. A full scan may copy the objects locally first: see [Local copy of a remote source](#local-copy-of-a-remote-source) |

### Expected windows and gaps

A window with no rows is a gap only against **Expected**: every window, or
Monday to Friday's hours or days, from **From** to before **Before**. Weekend
windows under weekdays only are counted apart, never as gaps. At most 20,000
windows are checked; a longer range is refused whole. Windows are cut in UTC.

| Gap | When |
|---|---|
| empty | The run's segment counts have no rows in the scope for it. Reported only where the counts are exact: every row read, or every window counted |
| not sampled | The window's count has rows but the sample drew none; the row count is given. With no count, every window without a sampled row is not sampled, marked as not counted |
| out of scope | The scope is a time range on the grain's column, and the window is not wholly inside it |

## Read plans

Setup's Read line says what Run will read before it reads:

| Read | When |
|---|---|
| Report on screen: this setup · no read; Session cache: this setup · no read | The setup is the report's, or the session cache holds it |
| Changed: Compare, Expected · no read | The setup differs from the report on screen only in its comparison or expected windows; Run compares the segments the report holds, and checks the windows against its counts, after a full scan too |
| Rows: from an earlier run · no source read | A sampled setup whose sample (scope, method, size, seed), dataset and view match rows a run read this session; any grain, role or format |
| Seeded runs of the file | A random sample of one Parquet or IPC file: the whole source, or a view with no filter, query or reshape (a sort is fine); a few dozen short reads |
| 1 streaming pass over every eligible row | Any other random or equal-per-value sample; the pass counts the scope too |
| Released since last read · read again | Those rows were read this session and released, by <kbd>d</kbd> or the memory budget: Run reads them again |
| Segment totals: exact, counted by the grain's column in that pass | A partition or time-window grain on a streamed sample: exact segment totals from the one pass |
| Segment totals: from an earlier count · no read | The same grain was counted before, with these rows |
| Segment totals: summed from earlier hourly or daily counts | A coarser window of the same column: hours sum into days, weeks and months, days into weeks and months |
| +1 count of the grain's column · exact segment totals, kept | A partition or time-window grain that nothing has counted: seeded runs or first rows, a new grain on rows already read, or a finer window; kept for later runs |
| Too many segments … to count | The grain had more than 1,000,000 keys; a coarser grain is needed |
| Every eligible row · up to N passes over the scope, 1 per check | A full scan of a local source: one collect per check, and one more to count an unknown scope |
| 1 fetch of N objects (size) to a local copy · up to N passes over it | A full scan of a remote dataset that can be copied: see [Local copy of a remote source](#local-copy-of-a-remote-source) |
| Local copy kept for later full scans · d releases | Said with the fetch |
| Released since last copy · fetched again | The copy was released by <kbd>d</kbd>: Run fetches it again |
| Every eligible row · up to N passes over the local copy (size) · no source read | A full scan of a dataset whose copy a run fetched this session |
| Every eligible row · up to N passes over the source | A full scan of a remote dataset with no copy, with the reason on the next line, `No local copy:` and one of: size over the limit, more than the free disk, free disk unknown, the scope reads part of the source, object sizes unknown when it opened, a copy fetched this session that did not read as the source, or local copies off |
| Window by each interval's start or end: N of those passes, 1 per column | A full scan whose intervals start or end on more than one column: one grouping each |
| File metadata only | Values set to metadata only |
| Column intent: on the rows read · no extra read | Intent declared on a sampled run: measured on the sample's rows in memory |
| Key: repeats among the N sampled rows only | A declared key on a sample smaller than the scope |
| Column intent: in the profile pass · key adds 1 pass | A full scan with a declared key: one more pass, counted among the passes |
| Column intent: not checked, needs values | Intent declared with Values set to metadata only |
| Expected windows: from the segment counts · no read | Expected is set: gaps come from the counts the run takes anyway |

### Local copy of a remote source

A full scan of an S3, GCS or Azure dataset fetches each object once into the
cache directory and makes every pass over that copy when all of these hold:

| Condition | Why |
|---|---|
| The scope is the whole source, or a view with no filter, query, reshape or drill-down that shows every column | A narrower scope's passes may read less than the whole objects |
| No binary column | Binary columns are never read, and a copy would fetch them |
| Every object's size is known from the listing or the footer read that opened it | The budget is checked before Run, with no request |
| The total fits `[analysis] quality_local_copy` (`"2GiB"` by default; 0 never copies) and the free disk in the cache directory | The copy never takes more than either |

| | |
|---|---|
| Requests | One GET per object, streamed to disk; no list or head |
| Disk | The objects' listed sizes, under `quality-copies` in the cache directory |
| Kept | For later full scans of the dataset: after any edit, including a new role or grain, a run reads the copy and nothing from the source |
| Released | By <kbd>d</kbd> in Setup (the Read rule names it, as `local copy · 16.5 MiB`), by opening the dataset again or another one, and when datui exits. A run still reading the copy keeps it until the run ends |
| Cancel or failure | The fetch stops at its next chunk, and the objects copied so far are removed |
| Left behind | A copy left by a datui that did not exit cleanly, or quit while a run read it, is removed by the next copy any datui makes |
| An object changed since it opened | A size or ETag that differs from the listing's fails the run: open the dataset again |
| A local write fails | A full disk, or two keys that name one file on a disk that ignores case, fails the run; `quality_local_copy = 0` reads the source instead |
| Still read from the source | The values a type conflict hides, read per file as before |

## Trends and intervals

A Trends bar's detail:

| Row | Says |
|---|---|
| Span | Calendar range of the bar's windows, inclusive (`2024-01-29 to 2024-02-25`), with rows that have no time said apart; on other grains its first and last segment |
| Segments | Segments pooled, how many not sampled, how many under 30 sampled rows |
| Rows | `412 sampled of 3,210 (12.8%)`, or every row read |
| The measure | Count of its denominator (rows, or values for a distinct or parse share) and the rate; on a rows line, rows per segment: the mean, the smallest and the largest |
| 95% interval | The [Wilson interval](#data-quality-metric-definitions) of the rate on a sample, said to rest on under 30 rows when it does; none needed when every row was read, and none for a distinct share |
| Previous bar, Baseline bar | Against the bar before, or with a baseline, the bar holding it: before and now, the move in points, and `a clear change`, `within sampling noise` or `under a point`, judged as Segments judges a change (a distinct share is not judged) |

An interval's detail:

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

## Column intent

What columns must hold, declared in Setup's Column intent row. Every rule is
optional.

| Rule | Takes | Offered for |
|---|---|---|
| Key | The columns whose values together name one row, in any number | Every column |
| Required | Every row has a value | Every column |
| Read as | Whole number or decimal: text read as a number for the range, and text that does not read is counted | Text; text read as time takes its reading from Text as time |
| Allowed | Values separated by commas, outer spaces dropped, up to 100. A value in double quotes is kept as typed: `"a, b"` holds a comma, `" open"` a space, and `""` is a quote inside one | Text, whole numbers, true/false |
| Minimum, Maximum | A number; a date `2024-01-31`; or a date and time `2024-01-31 08:00:00`. On a date and time column, a date alone as the maximum takes in its whole day | Numbers, dates and times, and text read as either |

A declared key with no repeat says:

| Run | A key with no repeat says |
|---|---|
| Every row | The key is unique in the scope |
| A sample | No repeat among the sampled rows. Sampled rows are distinct rows, so a repeat found is a repeat in the data, but rows outside the sample are not checked; the coverage says `key repeats among N sampled rows only` |
| Metadata only | Nothing: the check is unavailable, values not read |

Out of range names the lowest value below the range and the highest above it. A
one-column key replaces the Nearly unique note on that column.

## Exported report

<kbd>x</kbd> on a report page writes the report on screen, from memory: nothing
is read.

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
| `source` | `location` (the URL as opened, or the local path made absolute), `remote`, `format`, `files` and up to 100 `file_names` for a dataset of several files, `bytes` and `modified` (RFC 3339, UTC) of a local file as the run that read the rows began (a report remade from rows a run kept keeps that run's), and `view`: the query (q, SQL or Text), filters and reshape a view scope measured. No content hash: that would be a read. `null` for a report no run labeled |
| `setup` | `scope`, `values` (`sample`, `full` or `metadata`), `sample` (`method`, `rows`, `seed`), `grain`, `comparison`, `baseline_segment`, `time_formats` (`column`, `kind`, `format`), `time_roles` (`role`, `column`), `intervals`, `window_by`, `latency_threshold_seconds`, `intent` (`key`, and per column `column`, `required`, `allowed`, `min`, `max`, `read_as`), and `expected` (`weekdays`, `from`, `before`, as typed), `null` when no windows are stated |
| `run` | `precision` (`exact`, `sampled` or `metadata`), `total_rows`, `evaluated_rows`, `per_value`, `source_files`, `footers_read`, and `reads` (`source_reads`, `counted`, `rows_traversed`, and `local_copy` (`bytes`, `objects`, `fetched_by_this_run`) when a full scan's passes read one) when the run's reads were watched |
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

## Missing columns and type conflicts

Absent columns and type conflicts come from the footers datui read when the
dataset opened, not from values. So they are reported whatever the sample
reads, even when values are not read, and they are counted over the whole
loaded source. The finding names the files by number, as the Sample form's
Files list numbers them, and <kbd>Enter</kbd> opens the rows those files
contributed. Where footers were sampled, both counts are a floor, and the
finding says how many footers were read. A full scan also reads the first five
values each conflicting file holds at the type it wrote; the access plan's
Conflict values row states how many extra reads that costs. The
[Info panel](../user-guide/dataset-info.md#notes) notes report the same facts at open time
and offer to read a conflicting column as text.
