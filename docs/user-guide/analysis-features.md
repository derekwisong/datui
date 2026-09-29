# Analysis

Press <kbd>a</kbd> to open the analysis view on the current data. A list of
tools sits on the right; <kbd>Tab</kbd> moves focus between the list and the
result, <kbd>↑</kbd> <kbd>↓</kbd> pick a tool, <kbd>Enter</kbd> runs it.
<kbd>Esc</kbd> returns to the table.

Describe, Distribution and Correlation use the current query and filters.
Data Quality can use that view or an explicitly chosen source scope.

## Describe

Summary statistics per column, like Polars'
[`describe`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.describe.html):
count, nulls, mean, standard deviation, min, 25th percentile, median, 75th
percentile and max. Scroll with the arrow keys.

## Distribution

Compares each numeric column against fourteen distributions — Normal, Log-Normal,
Uniform, Power Law, Exponential, Beta, Gamma, Chi-Squared, Student's t,
Poisson, Bernoulli, Binomial, Geometric and Weibull — and reports the best
fit, along with the Shapiro-Wilk statistic and p-value, coefficient of
variation, outlier count (IQR method), skewness and kurtosis. Color marks fit quality: green is good, yellow is moderate, red is a
poor fit or a column with many outliers or extreme shape. Constant columns
are labeled **Constant**; a column can also report **No clear fit**. A best
match is a statistical comparison, not proof of a distribution.

Press <kbd>Enter</kbd> on a column for the detail view: a Q-Q plot against the
chosen distribution and a histogram with the theoretical curve overlaid.
<kbd>↑</kbd> <kbd>↓</kbd> switch the distribution being compared, <kbd>s</kbd>
toggles the histogram between linear and log scale, <kbd>Esc</kbd> returns to
the table.

## Correlation matrix

Pairwise correlations between every numeric column, colored by strength.
Move around with the arrow keys and press <kbd>Enter</kbd> on a cell for the
pair: the Pearson coefficient with a plain reading of it, R², the p-value,
and how many row pairs it was computed from. Correlation is undefined for a
constant column.

## Data quality

Choose **Data Quality** for missing values, repeated values, schema differences
and changes across files or time. It has its own scope and sampling controls;
its results can describe the current view or the source dataset.

See [Data quality](data-quality.md) for the workflow and metric definitions.

<a id="keys-and-result-tabs"></a>
<a id="sampling-and-budgets"></a>
<a id="data-quality-metric-definitions"></a>
<a id="columns-a-file-never-had-and-columns-it-holds-in-another-type"></a>

## Sampling

Describe, Distribution and Correlation read tables of up to 100,000 rows
in full by default. Larger tables use a sample of 100,000 rows spread across
the dataset. The result title shows the sample size and total row count.

| Key | Action |
|---|---|
| <kbd>r</kbd> | Draw another sample |
| <kbd>a</kbd> | Read every row after confirming the count |
| <kbd>Esc</kbd> | Cancel a run in progress |

| View | Sampling method | What gets read |
|---|---|---|
| One unfiltered Parquet or IPC file | 50 runs of rows at seeded positions | Row groups containing those runs |
| A directory, filtered view, query or other format | Seeded uniform sample retained from a streaming pass | Every row once, while keeping only the sample |

Analysis omits the view's sort because these statistics do not depend on row
order. A cancelled full read may still be finishing in the background; another
full read is unavailable until it finishes.

```toml
[performance]
analysis_sample_rows = 100000   # 0 reads every row of every table
```

or `--sample-rows N` for one run. Data Quality does not use this setting: its
plan chooses between metadata, a seeded sample of a stated size and a full
scan, and says what each will read before it runs.
