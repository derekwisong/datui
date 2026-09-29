# Analysis

Press <kbd>a</kbd> to open the analysis view on the current data. A list of
tools sits on the right; <kbd>Tab</kbd> moves focus between the list and the
result, <kbd>↑</kbd> <kbd>↓</kbd> pick a tool, <kbd>Enter</kbd> runs it.
<kbd>Esc</kbd> returns to the table.

Analysis runs on the data as you see it, after any query and filters.

## Describe

Summary statistics per column, like Polars'
[`describe`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.describe.html):
count, nulls, mean, standard deviation, min, 25th percentile, median, 75th
percentile and max. Scroll with the arrow keys.

## Distribution

Fits each numeric column against fourteen distributions — Normal, Log-Normal,
Uniform, Power Law, Exponential, Beta, Gamma, Chi-Squared, Student's t,
Poisson, Bernoulli, Binomial, Geometric and Weibull — and reports the best
fit, along with the Shapiro-Wilk statistic and p-value, coefficient of
variation, outlier count (IQR method), skewness and kurtosis. Color marks fit quality: green is good, yellow is moderate, red is a
poor fit or a column with many outliers or extreme shape.

Press <kbd>Enter</kbd> on a column for the detail view: a Q-Q plot against the
chosen distribution and a histogram with the theoretical curve overlaid.
<kbd>↑</kbd> <kbd>↓</kbd> switch the distribution being compared, <kbd>s</kbd>
toggles the histogram between linear and log scale, <kbd>Esc</kbd> returns to
the table.

## Correlation matrix

![Correlation Matrix Demo](../demos/09-correlation-matrix.gif)

Pairwise correlations between every numeric column, colored by strength.
Move around with the arrow keys and press <kbd>Enter</kbd> on a cell for the
pair: the Pearson coefficient with a plain reading of it, R², the p-value,
and how many row pairs it was computed from.

## Data quality

Choose **Data Quality** to check the rows in scope and get a report: what is
likely wrong, what depends on intent, and which columns are clean. Before the
first run the pane is the [Sample](#sampling) form, and nothing is read until
<kbd>Enter</kbd> runs it; then the report opens. The header says which rows
were checked, as every tool's does; the tabs under it are the pages, and
<kbd>←</kbd> <kbd>→</kbd> move between them. <kbd>e</kbd> edits the plan. A full
value scan always asks for confirmation.

### Reading the report

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
| Single value | Note | One value in every row checked |

A dataset-grain sample is spread across the whole scope, as Describe's is:
one Parquet or IPC file is read as a few dozen short runs across it, anything
else in one streamed pass that keeps a seeded sample. The header says how many
rows were sampled of how many.

### The plan

The plan shows the sample, profile grain, what is read, comparison, estimated
rows and transfer direction; <kbd>p</kbd> shows the estimate basis. The rows
are the shared [sample](#sampling): <kbd>s</kbd>, or <kbd>Enter</kbd> on the
Sample row, opens its form.

| Setting | Choices |
|---|---|
| Sample | The shared sample: scope, method, rows and seed |
| Grain | Whole dataset, source file when row provenance is available, Hive partition, fixed row chunks, or hourly, daily, weekly, or monthly windows on an assigned time role |
| Values | Read, or metadata only (footers, no values); whether a read is sampled or every row is the sample's method |
| Comparison | None, previous ordered segment, or first-segment baseline |
| Time roles | Event, effective/as-of, period end, created, published, received, processed, valid from, and valid to |

<kbd>e</kbd> edits a copy of the plan; <kbd>Esc</kbd> discards it.

Before a run, source-scoped plans report unknown row counts and read sizes,
and metadata-only runs do not count rows. A local dataset-grain sample
estimates its read as a ceiling (`up to`), since a single Parquet or IPC file
is read in short runs and anything else is streamed once; the run reports the
exact eligible count. The scope is applied before sampling, so sampling never
reaches beyond its bounds.

The Time roles row opens an explicit mapping table; every role starts
unassigned. Under it, each date and time column is listed with its type and a
few of its values from the rows on screen, and the column the focused role holds
is marked. Datui recognizes physical date and datetime types but never
guesses their business meaning from column names. With a source scope, the
picker includes source time columns hidden by the current view.

### Keys and result tabs

The control bar has one shape on every page: <kbd>Esc</kbd> Back, the page's own
action, then <kbd>s</kbd> Sample, <kbd>←</kbd> <kbd>→</kbd> Page, <kbd>v</kbd>
View Rows and <kbd>e</kbd> Plan.

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Previous or next page: Overview, Columns, Segments, Trends, Plan |
| <kbd>Enter</kbd> | Run the plan, open a finding, or show its rows; on an empty Segments or Trends page, open the plan setting that fills it |
| <kbd>e</kbd> | Edit a copy of the plan; <kbd>Esc</kbd> discards edits |
| <kbd>s</kbd> | Open the shared [Sample](#sampling) form |
| <kbd>p</kbd> | Show the detailed access plan |
| <kbd>1</kbd>–<kbd>4</kbd> | Overview, Columns, Segments, Trends, directly |
| <kbd>[</kbd> <kbd>]</kbd> | Choose a column in Segments or Trends, once the plan's Grain splits the rows |
| <kbd>m</kbd> | Cycle the measurement: null, empty, whitespace, non-finite, distinct, integer-parse, decimal-parse |
| <kbd>b</kbd> | Use the highlighted segment as the comparison baseline; deltas update without another data read |
| <kbd>r</kbd> | Rerun with a new sample seed, for every tool |
| <kbd>Esc</kbd> | Back one level |

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
  count and its findings; <kbd>Enter</kbd> opens the column's measurements.
- **Segments** — needs a Grain other than the whole dataset; until then the
  page says so and <kbd>Enter</kbd> opens the plan on Grain. It keeps both row
  denominators visible and shows the chosen
  column measurement, its percentage-point change against the selected
  comparison, and the largest change any column made against that comparison;
  the first segment with a material one is where a shift starts.
- **Trends** — charts the measurement across ordered row chunks or time
  windows, and reports the time between dates for assigned role pairs: missing
  endpoints, negative durations, p50/p90/p95/p99, and maximum duration. Each
  part says what it needs when it has nothing, and <kbd>Enter</kbd> opens it:
  Time roles when the data has date columns, Grain otherwise.

### Sampling and budgets

Every grain reads the shared [sample](#sampling), the same rows every tool
reads, and splits it into segments. Only a full scan (the sample's method set
to Every row) asks for confirmation.

| | |
|---|---|
| Dataset grain | The whole sample is one segment |
| File, partition, chunk, window grain | The sample's rows, split by the segment each came from. A **Random** sample gives each segment its share, so a small one gets few rows; **Equal per value** of the partition column gives every segment the same number. A segment's total is shown when it is known without reading it (a file's rows from its footer when whole files are in scope, a row chunk's size) and is otherwise unknown on a sample |
| Row chunks | Use the selected scope's physical order; sampled rows keep their original chunk labels |
| Time windows | `1h`, `1d`, `1w`, `1mo` on each assigned time role, starting on the calendar boundary for their width (weeks start on Monday); a window is cut at the same place whether sampled or scanned |
| File mapping | Available on source scopes and on views that preserve source-row provenance; otherwise Segments says it is unavailable |
| Remote sources | Read-only; the access plan always reports zero remote writes |

Complete profiles are reused during the session when the dataset, current
view, and full plan (including sample seed and time roles) match. Reopening
Data Quality then shows the cached result without reading values again;
changing the view or plan requires a new run.

### Data-quality metric definitions

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
| Largest change | Over every column, the biggest percentage-point move in null rate or distinct share against the compared segment; reported when it reaches 1 pp, otherwise the first column whose minimum or maximum moved |
| Lifecycle latency | End role timestamp − start role timestamp per row; missing endpoints are counted separately and negative values are retained |

Lifecycle percentiles use the evaluated duration values in sorted order. Date
values are interpreted at midnight; datetime values retain their physical time
unit. The role mapping is a user assertion and is included in the visible plan.

A nearly unique column is reported only from an exact profile. A null rate measured
on a sample stands for the whole; a distinct count does not, and an identifier
that repeats ten times in a billion rows is unique in every sample of it.

### Columns a file never had, and columns it holds in another type

Absent columns and type conflicts come from the footers datui read when the
dataset opened, not from values, so they are reported at every compute budget
and counted over the whole loaded source whatever the plan's scope is. The
finding detail names the largest twenty files by their Scope-page numbers,
and <kbd>Enter</kbd> opens the rows those files contributed as a `files`
scope. Where footers were sampled, both counts are a floor, and the measured
fact says how many footers were read. A full scan also reads the first five
values each conflicting file holds at the type it wrote; the access plan's
Conflict values row states how many extra reads that costs. The
[Dataset Info](dataset-info.md#notes) notes report the same facts at open time
and offer to read a conflicting column as text.

## Sampling

Every analysis tool reads the same sample: which rows, how they are picked,
how many, and the seed. The first tool you run on a dataset shows the
**Sample** form in its pane with the cursor in it: change a setting or press
<kbd>Enter</kbd> to run with the form as it stands; <kbd>Esc</kbd> goes back to
the tool list. After that, every tool you pick runs at once on the same sample. A
value the data does not hold, or rows that match nothing, is refused with what
the data does hold. After that,
<kbd>s</kbd> opens the form from any tool; <kbd>Enter</kbd> applies it and runs
the tool on screen again, and <kbd>Esc</kbd> discards the edit. The other tools' results go with the old
sample, so switching tools compares like with like. The header says what was
read: `Describe · sample of 100,000 of 36,839,175 rows · source year=2020..2022`.

| Setting | Choices |
|---|---|
| Rows from | **All rows** (the table as shown, with its count), **The source, unfiltered** (only when a filter or query changes the rows), **Partitions**, **Files**, **Row range**, **Time range**; a choice appears only when the table has it |
| Method | **Random** (default), **Equal per value**, **First rows**, **Every row** |
| Per value of | For Equal per value: the column to split by; partition columns come first |
| Sample size | 1,000 to 1,000,000 rows, or rows per value for Equal per value; the default is `[performance] analysis_sample_rows` |
| Random seed | For Random and Equal per value: any whole number, typed; the same seed reads the same rows, so `0` or `1` is a sample anyone can repeat. <kbd>r</kbd> draws a new one |

Each kind of rows brings its own settings, with what it needs to know:

| Rows from | Settings |
|---|---|
| Partitions | **Partition**: the column. **Values**: one value, a list (`2019,2021`) or an inclusive range (`2020..2022`) compared in the column's own type; the values the source holds are listed under it |
| Files | **Files**: their numbers, like `1,3`; the numbered files are listed under it and ticked as they are typed, <kbd>PgUp</kbd> <kbd>PgDn</kbd> scrolls |
| Row range | **From row** and **To row**, inclusive and 1-based, in the order the table shows; it starts as the whole table |
| Time range | **Column**, **From** and **Before**: dates or RFC 3339 timestamps, the Before date not included |

Partitions, files and time ranges read the source, ignoring the query and filters.

| Method | What it reads |
|---|---|
| Random | A seeded random sample across all the rows chosen |
| Equal per value | Up to the sample size from each value of a column, so a small partition is represented beside a large one; refused past 10,000 values or 2,000,000 rows kept |
| First rows | The first rows in order: the fastest read, and only the head |
| Every row | No sampling |

| Key | Action |
|---|---|
| <kbd>s</kbd> | Open the Sample form |
| <kbd>v</kbd> | View the sample's rows in the table viewer: sort, filter, query, copy or export them; <kbd>Esc</kbd> returns to the tool |
| <kbd>r</kbd> | Draw another sample (a new seed) |
| <kbd>a</kbd> | Read every row, after confirming the count; sets the method to Every row |
| <kbd>Esc</kbd> | Cancel a run in progress |

How a spread sample is read depends on the source:

| Source | Sample | Reads |
|---|---|---|
| One Parquet or IPC file, unfiltered | 50 runs of rows at seeded places across it | The row groups those runs fall in |
| Anything else: a directory or hive table, a filter, a query, CSV | A seeded uniform sample, kept while the rows stream past | Every row once, holding only the sample |

A directory of many files is streamed because a run in it opens the footer of
every file before it; on a 135-file table in S3, one streamed pass over 37
million rows took 3 seconds against 6 for fifty runs. The sort is left out of
an analysis read: no statistic depends on it. <kbd>a</kbd> refuses while a
cancelled full read is still finishing.

```toml
[performance]
analysis_sample_rows = 100000   # the sample's starting size; 0 starts at Every row
```

or `--sample-rows N` for one run. Data Quality reads the same sample, at the
same size, as every other tool.
