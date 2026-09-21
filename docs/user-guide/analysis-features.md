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

Choose **Data Quality** to profile the current view, loaded source, or a bounded
prefix of the current view. The first screen is an
inert plan: no values are read until you press <kbd>Enter</kbd>. It shows the
scope, profile grain, compute mode, comparison, estimated rows and transfer
direction. Press <kbd>p</kbd> for the estimate basis. A full value scan asks for
confirmation.

| Setting | Choices |
|---|---|
| Scope | Current view, whole loaded source, first 10,000 view rows, or first 1,000,000 view rows |
| Grain | Whole dataset, source file when row provenance is available, Hive partition, fixed row chunks, or weekly windows on an assigned time role |
| Compute | Metadata only, a seeded sample from a bounded prefix, or full scan |
| Comparison | None, previous ordered segment, or first-segment baseline |
| Time roles | Event, effective/as-of, period end, created, published, received, processed, valid from, and valid to |

Press <kbd>e</kbd> to edit a copy of the plan. <kbd>Esc</kbd> discards edits.
Use Left/Right on Scope to change which rows are eligible. Whole source ignores
the active query, filters, and sort; the access plan reports its row count and
read size as unknown until the run. A bounded prefix is selected before
sampling, so sampling never reaches past its limit. The Time roles row opens an explicit mapping table; every role starts
unassigned. Datui recognizes physical date and datetime types but never guesses
their business meaning from column names. With whole-source scope, the picker
includes source time columns hidden by the current view.

After a run, use <kbd>1</kbd>–<kbd>4</kbd> for Overview, Columns, Segments,
and Trends. Results state eligible and evaluated rows and whether values are
exact, sampled, or metadata-only. Overview includes dataset notes and neutral
observations. Press <kbd>Enter</kbd> on one for its definition, denominator,
provenance, and available category-variant examples. For an exact null, empty,
whitespace, non-finite, constant, or category-variant observation,
<kbd>Enter</kbd> again opens matching
rows in a temporary table; <kbd>Esc</kbd> returns to the same observation.
The row view uses the same scope and may read the source again. A sampled observation says when an
exact row view requires a full profile. Columns reports null,
empty, whitespace, non-finite, distinct,
parse, and range measurements. Segments keeps both row denominators visible and
shows the chosen column measurement and its percentage-point change against
the selected comparison. Press <kbd>[</kbd>/<kbd>]</kbd> to choose a column
and <kbd>m</kbd> to cycle null, empty, whitespace, non-finite, distinct,
integer-parse, and decimal-parse rates. Trends charts that measurement across
ordered row chunks or time windows and reports lifecycle
latency for accepted role pairs, including missing endpoints, negative
durations, p50/p90/p95/p99, and maximum duration.
On Segments, highlight a row and press <kbd>b</kbd> to make it the baseline;
comparison deltas update from the measured profiles without another data read.

Sampling is a compute choice, not a grain. It selects rows without replacement
from at most the first 50,000 eligible rows in the selected scope; it is not a random sample of the
entire dataset. Segment totals outside dataset grain are unknown in a sampled
run, and displayed as such. File mapping is available only while
the current view still preserves source-row provenance; otherwise the Segments
screen says that it is unavailable. Row chunks use the current view's physical
order, and sampled rows keep their original chunk labels. Remote sources are
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
| Range / text length | Minimum and maximum value, or minimum and maximum character length for text |
| Parse share | Values accepted by the named integer, decimal, ISO-date or ISO-datetime parser ÷ evaluated non-null text values |
| Duplicate groups | Groups of identical complete evaluated rows; extra rows is Σ(group size − 1), rows involved is Σ(group size) |
| Category variants | Original text values that become equal after outer-whitespace removal and lowercase normalization |
| Segment null rate | Null cells ÷ (evaluated rows × profiled logical columns) in that segment |
| Lifecycle latency | End role timestamp − start role timestamp per row; missing endpoints are counted separately and negative values are retained |

Lifecycle percentiles use the evaluated duration values in sorted order. Date
values are interpreted at midnight; datetime values retain their physical time
unit. The role mapping is a user assertion and is included in the visible plan.

## Sampling

By default every tool uses every row. On very large datasets, set a threshold
to analyze a sample instead:

```toml
[performance]
sampling_threshold = 1000000   # sample when a table has this many rows or more
```

or `--sampling-threshold 1000000` for one run (`0` forces the full dataset).
When a result is sampled the tool says so, and <kbd>r</kbd> draws a new sample.
