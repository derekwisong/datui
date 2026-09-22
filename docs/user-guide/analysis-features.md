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

Fits each numeric column against Normal, LogNormal, Uniform, PowerLaw and
Exponential and reports the best fit, along with the Shapiro-Wilk statistic
and p-value, coefficient of variation, outlier count (IQR method), skewness and
kurtosis. Color marks fit quality: green is good, yellow is moderate, red is a
column with many outliers or extreme shape.

Press <kbd>Enter</kbd> on a column for the detail view: a Q-Q plot against the
chosen distribution and a histogram with the theoretical curve overlaid.
<kbd>↑</kbd> <kbd>↓</kbd> switch the distribution being compared, <kbd>s</kbd>
toggles the histogram between linear and log scale, <kbd>Esc</kbd> returns to
the table.

## Correlation matrix

![Correlation Matrix Demo](../demos/09-correlation-matrix.gif)

Pairwise correlations between every numeric column, colored by strength.
Move around with the arrow keys and press <kbd>Enter</kbd> on a cell for the
pair: coefficient, p-value, sample size, a text scatter plot and the summary
statistics of both columns.

## Data quality

Choose **Data Quality** to profile selected rows at several scales. The first
screen is an inert plan: no values are read until you press <kbd>Enter</kbd>. It shows the
scope, profile grain, compute mode, comparison, estimated rows and transfer
direction. Press <kbd>p</kbd> for the estimate basis. A full value scan asks for
confirmation.

| Setting | Choices |
|---|---|
| Scope | Current view, whole loaded source, a view row range, selected source files, one source partition value, or a source time range |
| Grain | Whole dataset, source file when row provenance is available, Hive partition, fixed row chunks, or hourly, daily, weekly, or monthly windows on an assigned time role |
| Compute | Metadata only, seeded sample (bounded prefix for dataset grain; per-segment after a full selected-scope read for other grains), or full scan |
| Comparison | None, previous ordered segment, or first-segment baseline |
| Time roles | Event, effective/as-of, period end, created, published, received, processed, valid from, and valid to |

Press <kbd>e</kbd> to edit a copy of the plan. <kbd>Esc</kbd> discards edits.
Use Left/Right on Scope for presets, or <kbd>Enter</kbd> on Scope for a precise
selection:

| Scope entry | Meaning |
|---|---|
| `view` / `source` | Current table pipeline / loaded source, ignoring the active query, filters, and sort |
| `rows 100..200` | Inclusive 1-based row range in the current view order |
| `files 1,3` | Source files by the numbered inventory on the Scope page; PageUp/PageDown scrolls it |
| `partition region=west` | Rows with that source-column value; `∅` selects null |
| `time event=2024-01-01..2024-02-01` | Source rows in an ISO date or RFC 3339 timestamp interval; end is exclusive and date-only bounds mean UTC midnight |

Source-scoped plans report unknown row counts and read sizes before a run.
Metadata-only runs do not count rows. A dataset-grain bounded sample reports an
eligible row count only when its probe reaches the end of the scope or a valid
count was already cached; otherwise that count stays unknown. Other grains
sample each segment after reading the full selected scope, so they require
confirmation and report exact eligible and per-segment row counts afterward.
The selected scope is applied before sampling, so sampling never reaches beyond
its bounds. The Time roles row opens an explicit mapping table; every role starts
unassigned. Datui recognizes physical date and datetime types but never guesses
their business meaning from column names. With a source scope, the picker
includes source time columns hidden by the current view.

After a run, use <kbd>1</kbd>–<kbd>4</kbd> for Overview, Columns, Segments,
and Trends. Results state eligible and evaluated rows and whether values are
exact, sampled, or metadata-only. Overview includes dataset notes and neutral
observations. Press <kbd>Enter</kbd> on one for its definition, denominator,
provenance, and its available examples: category variants, the files behind an
absent column or a type conflict, and the values a conflict hides. For an exact
null, empty, whitespace, non-finite, constant, key-like or category-variant
observation, <kbd>Enter</kbd> again opens matching
rows in a temporary table; <kbd>Esc</kbd> returns to the same observation. An
absent or type-conflict observation opens the rows its files contributed, at any
compute budget, because the files it names come from footers rather than values.
The row view uses the same scope and may read the source again. A sampled observation says when an
exact row view requires a full profile. Columns reports null,
empty, whitespace, non-finite, distinct,
parse, and range measurements. Segments keeps both row denominators visible and
shows the chosen column measurement, its percentage-point change against
the selected comparison, and the largest change any column made against that
same comparison; the first segment with a material one is where a shift
starts. Press <kbd>[</kbd>/<kbd>]</kbd> to choose a column
and <kbd>m</kbd> to cycle null, empty, whitespace, non-finite, distinct,
integer-parse, and decimal-parse rates. Trends charts that measurement across
ordered row chunks or time windows and reports lifecycle
latency for accepted role pairs, including missing endpoints, negative
durations, p50/p90/p95/p99, and maximum duration.
On Segments, highlight a row and press <kbd>b</kbd> to make it the baseline;
comparison deltas update from the measured profiles without another data read.

Sampling is a compute choice, not a grain. Dataset-grain sampling selects rows
without replacement from at most the first 50,000 eligible rows; it is not a
random sample of the entire dataset. For file, partition, chunk, and window
grain, a streaming full-scope read retains up to 50,000 seeded rows per segment
without replacement. The access plan says the value-read size is unknown and
asks for confirmation. A per-segment budget multiplies by the number of
segments, so the run is refused rather than silently kept if it would retain
more than 500,000 rows, 512 MiB, or 10,000 segments; narrow the scope or lower
the budget. File mapping
is available on source scopes and
on current views that preserve source-row provenance; otherwise the Segments
screen says that it is unavailable. Row chunks use the selected scope's physical
order, and sampled rows keep their original chunk labels. Time windows are
offered at `1h`, `1d`, `1w` and `1mo` on each assigned time role and start on
the calendar boundary for their width, so weeks start on Monday. A window is
cut at the same place whether the run sampled or scanned it. Remote sources are
read-only and the access plan always reports zero remote writes.

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
| Parse share | Values accepted by the named integer, decimal, ISO-date or ISO-datetime parser ÷ evaluated non-null text values |
| Duplicate groups | Groups of identical complete evaluated rows; extra rows is Σ(group size − 1), rows involved is Σ(group size) |
| Category variants | Original text values that become equal after outer-whitespace removal and lowercase normalization |
| Key-like repeats | Non-null rows − distinct values, on exact profiles only, reported when distinct values are at least 95% of non-null rows and at least one value repeats. That counts rows beyond one per value; the drill-in opens every row that shares one, which is always more |
| Absent values | Rows held by files whose footer has no such column ÷ rows in the loaded source; read from footers, not values |
| Type conflicts | Rows held by files that store the column in a type the scan cannot read ÷ rows in the loaded source; read from footers, not values |
| Segment null rate | Null cells ÷ (evaluated rows × profiled logical columns) in that segment |
| Largest change | Over every column, the biggest percentage-point move in null rate or distinct share against the compared segment; reported when it reaches 1 pp, otherwise the first column whose minimum or maximum moved |
| Lifecycle latency | End role timestamp − start role timestamp per row; missing endpoints are counted separately and negative values are retained |

Lifecycle percentiles use the evaluated duration values in sorted order. Date
values are interpreted at midnight; datetime values retain their physical time
unit. The role mapping is a user assertion and is included in the visible plan.

A key-like column is reported only from an exact profile. A null rate measured
on a sample stands for the whole; a distinct count does not, and an identifier
that repeats ten times in a billion rows is unique in every sample of it.

### Columns a file never had, and columns it holds in another type

Two checks are about which files hold which columns rather than about values. A
cell a file never had arrives as a null and a cell a file holds in an unreadable
type is not read at all, so no measurement over values can find either one.
Both come from the footers datui already read when the dataset opened, which
means they are reported at every compute budget, including metadata-only.

Because footers describe the source rather than the run, these two checks count
every file and every row of the loaded source whatever the plan's scope is. The
observation detail says so, and gives each file by the number the Scope page
uses, with the rows it holds and the type it stores the column in. Press
<kbd>Enter</kbd> to open the rows those files contributed; the drill-in is a
`files` scope, because there is no value to filter on. The largest twenty files
are named, and those are the ones <kbd>Enter</kbd> opens; the count beside them
covers every file, so the view holds fewer rows than the count states and the
detail pane says so.

On a dataset too large to read every footer, a file nobody looked at is
indistinguishable from a file missing nothing, so both counts are a floor rather
than a total and the measured fact says how many footers were read.

A full scan also reads the values a type conflict hides: for each conflicting
column, the first five values of each named file, read at the type that file
wrote. That is one extra one-column read per file, and the access plan's
**Conflict values** row states how many before anything runs. Other compute
budgets report the counts and say a full profile is needed for the values. The
Info panel's notes offer the same column as text for the whole view, which
changes what every screen reads; this reads a few values and changes nothing.

## Sampling

By default every tool uses every row. On very large datasets, set a threshold
to analyze a sample instead. Data Quality is the exception: it does not read
`sampling_threshold`, because its plan already chooses between metadata, a
seeded sample of a stated size, and a full scan, and says what each will read
before it runs.

```toml
[performance]
sampling_threshold = 1000000   # sample when a table has this many rows or more
```

or `--sampling-threshold 1000000` for one run (`0` forces the full dataset).
When a result is sampled the tool says so, and <kbd>r</kbd> draws a new sample.
This setting applies to Describe, Distribution and Correlation. Data Quality
uses its own compute budget instead.
