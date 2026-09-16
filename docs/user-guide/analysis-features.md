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

## Sampling

By default every tool uses every row. On very large datasets, set a threshold
to analyze a sample instead:

```toml
[performance]
sampling_threshold = 1000000   # sample when a table has this many rows or more
```

or `--sampling-threshold 1000000` for one run (`0` forces the full dataset).
When a result is sampled the tool says so, and <kbd>r</kbd> draws a new sample.
