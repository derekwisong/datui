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

Choose **Data Quality** to profile selected rows at several scales. On a
local file the default plan runs at once and the result opens first; the
strip at the top echoes the plan that ran, and <kbd>e</kbd> edits it. On a
remote source the first screen is an inert plan: no values are read until
you press <kbd>Enter</kbd>. The plan shows the scope, profile grain, compute
mode, comparison, estimated rows and transfer direction; <kbd>p</kbd> shows
the estimate basis. A full value scan always asks for confirmation.

| Setting | Choices |
|---|---|
| Scope | Current view, whole loaded source, a view row range, selected source files, one source partition value, or a source time range |
| Grain | Whole dataset, source file when row provenance is available, Hive partition, fixed row chunks, or hourly, daily, weekly, or monthly windows on an assigned time role |
| Compute | Metadata only, seeded sample (bounded prefix for dataset grain; per-segment after a full selected-scope read for other grains), or full scan |
| Comparison | None, previous ordered segment, or first-segment baseline |
| Time roles | Event, effective/as-of, period end, created, published, received, processed, valid from, and valid to |
| Sample rows | Rows kept per segment for a sampled compute: <kbd>←</kbd> <kbd>→</kbd> cycle 1,000 to 50,000; the default comes from [`[performance] quality_sample_rows`](configuration.md#performance) |

<kbd>e</kbd> edits a copy of the plan. In the editor, <kbd>←</kbd> <kbd>→</kbd> on Scope
picks a preset and <kbd>Enter</kbd> on Scope opens a precise entry:

| Scope entry | Meaning |
|---|---|
| `view` / `source` | Current table pipeline / loaded source, ignoring the active query, filters, and sort |
| `rows 100..200` | Inclusive 1-based row range in the current view order |
| `files 1,3` | Source files by the numbered inventory on the Scope page; <kbd>PgUp</kbd> <kbd>PgDn</kbd> scrolls it |
| `partition region=west` | Rows with that source-column value; `∅` selects null |
| `time event=2024-01-01..2024-02-01` | Source rows in an ISO date or RFC 3339 timestamp interval; end is exclusive and date-only bounds mean UTC midnight |

Before a run, source-scoped plans report unknown row counts and read sizes,
and metadata-only runs do not count rows. A dataset-grain bounded sample
reports an eligible row count only when its probe reaches the end of the scope
or a valid count was already cached; other grains sample each segment after
reading the full selected scope, so they require confirmation and report exact
eligible and per-segment counts afterward. The scope is applied before
sampling, so sampling never reaches beyond its bounds.

The Time roles row opens an explicit mapping table; every role starts
unassigned. Datui recognizes physical date and datetime types but never
guesses their business meaning from column names. With a source scope, the
picker includes source time columns hidden by the current view.

### Keys and result tabs

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Run the plan, inspect an observation, or open its exact matching rows |
| <kbd>e</kbd> | Edit a copy of the plan; <kbd>Esc</kbd> discards edits |
| <kbd>p</kbd> | Show the detailed access plan |
| <kbd>1</kbd>–<kbd>4</kbd> | Overview, Columns, Segments, Trends |
| <kbd>[</kbd> <kbd>]</kbd> | Choose a column in Segments or Trends |
| <kbd>m</kbd> | Cycle the measurement: null, empty, whitespace, non-finite, distinct, integer-parse, decimal-parse |
| <kbd>b</kbd> | Use the highlighted segment as the comparison baseline; deltas update without another data read |
| <kbd>r</kbd> | Rerun with a new sample seed |
| <kbd>Esc</kbd> | Back one level |

Every result states eligible and evaluated rows and whether values are exact,
sampled, or metadata-only.

- **Overview** — dataset notes and neutral observations. <kbd>Enter</kbd> on
  one shows its definition, denominator, provenance and available examples:
  category variants, the files behind an absent column or a type conflict, and
  the values a conflict hides. For an exact null, empty, whitespace,
  non-finite, constant, key-like or category-variant observation,
  <kbd>Enter</kbd> again opens matching rows in a temporary table;
  <kbd>Esc</kbd> returns to the same observation. The row view uses the same
  scope and may read the source again; a sampled observation says when an
  exact row view requires a full profile.
- **Columns** — null, empty, whitespace, non-finite, distinct, parse, and
  range measurements.
- **Segments** — keeps both row denominators visible and shows the chosen
  column measurement, its percentage-point change against the selected
  comparison, and the largest change any column made against that comparison;
  the first segment with a material one is where a shift starts.
- **Trends** — charts the measurement across ordered row chunks or time
  windows, and reports lifecycle latency for accepted role pairs: missing
  endpoints, negative durations, p50/p90/p95/p99, and maximum duration.

### Sampling and budgets

Sampling is a compute choice, not a grain.

| | |
|---|---|
| Dataset grain | Selects without replacement from a bounded prefix of the scope — about twice the Sample rows budget, never more than the first 50,000 eligible rows; not a random sample of the entire dataset |
| File, partition, chunk, window grain | A streaming full-scope read retains up to the plan's Sample rows budget of seeded rows per segment (engine cap 50,000), without replacement; the access plan says the value-read size is unknown and asks for confirmation |
| Budgets | The Sample rows budget multiplies by the number of segments; a run that would retain more than 500,000 rows, 512 MiB, or 10,000 segments is refused rather than silently trimmed — narrow the scope or lower the Sample rows budget |
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

Absent columns and type conflicts come from the footers datui read when the
dataset opened, not from values, so they are reported at every compute budget
and counted over the whole loaded source whatever the plan's scope is. The
observation detail names the largest twenty files by their Scope-page numbers,
and <kbd>Enter</kbd> opens the rows those files contributed as a `files`
scope. Where footers were sampled, both counts are a floor, and the measured
fact says how many footers were read. A full scan also reads the first five
values each conflicting file holds at the type it wrote; the access plan's
Conflict values row states how many extra reads that costs. The
[Dataset Info](dataset-info.md#notes) notes report the same facts at open time
and offer to read a conflicting column as text.

## Sampling

Describe, Distribution and Correlation read a table of up to 100,000 rows
whole. A larger one is analyzed from a sample of 100,000 rows, spread across
the whole table rather than taken from its start, and the title says so:
`Distribution Analysis · sample of 100,000 of 36,839,175 rows`.

| Key | Action |
|---|---|
| <kbd>r</kbd> | Draw another sample |
| <kbd>a</kbd> | Read every row, after confirming the count |
| <kbd>Esc</kbd> | Cancel a run in progress |

How the sample is drawn depends on what the view is:

| View | Sample | Reads |
|---|---|---|
| A Parquet or IPC file or hive directory, unfiltered | 50 runs of rows at seeded places across the table | The row groups those runs fall in |
| Anything else: a filter, a query, CSV, several files | A seeded uniform sample, kept while the rows stream past | Every row once, holding only the sample |

The sort is never part of an analysis: no statistic depends on it.

```toml
[performance]
analysis_sample_rows = 100000   # 0 reads every row of every table
```

or `--sample-rows N` for one run. Data Quality does not use this setting: its
plan chooses between metadata, a seeded sample of a stated size and a full
scan, and says what each will read before it runs.
