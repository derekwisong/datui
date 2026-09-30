# Analysis

Press <kbd>a</kbd> to open the analysis view on the current data. A list of
tools sits on the right; <kbd>Tab</kbd> moves focus between the list and the
result, <kbd>↑</kbd> <kbd>↓</kbd> pick a tool, <kbd>Enter</kbd> runs it.
<kbd>Esc</kbd> returns to the table.

Analysis runs on the data as you see it, after any query and filters, unless
the [sample](#sampling) is set to read the source.

## Describe

Summary statistics per column, like Polars'
[`describe`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.describe.html):
count, nulls, mean, standard deviation, min, 25th percentile, median, 75th
percentile and max. Date, datetime, time and duration columns get all of them
but the standard deviation, written as the table writes them; text columns get
min and max. When they do not all fit, the header counts those out of view
(`+4 →`) and <kbd>←</kbd> <kbd>→</kbd> scroll to the last. Distribution scrolls
its columns the same way.

## Distribution

Compares each numeric column against fourteen distributions — Normal, Log-Normal,
Uniform, Power Law, Exponential, Beta, Gamma, Chi-Squared, Student's t,
Poisson, Bernoulli, Binomial, Geometric and Weibull — and names the one the
values are consistent with, along with the Shapiro-Francia normality statistic
and p-value, coefficient of variation, outlier count (IQR method), skewness
and kurtosis.

| What | How |
|---|---|
| Parameters | Maximum likelihood for normal, log-normal, uniform, exponential, gamma (Minka's approximation), beta, Weibull, power law (from the smallest value), Poisson, Bernoulli and geometric; moments for chi-squared, Student's t and binomial |
| P-value | Kolmogorov-Smirnov on up to 500 values, calibrated by 199 samples drawn from the fit and refitted, so estimating the parameters from the data is accounted for. `<0.005` when none of the samples came close |
| Verdict | Among the families not rejected (p of 0.01 or more), the lowest AIC; a simpler family that holds is named instead unless the richer one is decisively better (AIC 10 or more lower). `No clear fit` when every family is rejected |
| `n/a` | The family cannot describe the values: a log-normal of negative values, a Poisson of fractions |

A p-value is how surprising the values would be if they came from the fitted
distribution, not the probability that they did. The tests assume independent
draws: a time series such as a price over years is dependent, and its
histogram need not match any family.

Press <kbd>Enter</kbd> on a column for the detail view: the verdict, then a Q-Q
plot and a histogram comparing the values with the family chosen in the list,
drawn with that family's fitted parameters. <kbd>↑</kbd> <kbd>↓</kbd> choose
another family to compare with, which does not change the verdict; <kbd>s</kbd>
toggles the histogram between linear and log scale; <kbd>Esc</kbd> returns to
the table.

## Correlation matrix

Pairwise correlations between every numeric column, colored by strength.
Move around with the arrow keys and press <kbd>Enter</kbd> on a cell for the
pair: the Pearson coefficient with a plain reading of it, R², the p-value,
and how many row pairs it was computed from. Correlation is undefined for a
constant column.

## Data quality

Choose **Data Quality** to check the rows in scope and get a report: what is
likely wrong, what depends on intent, and which columns are clean. It opens on
its Setup, which starts from the same [sample](#sampling) as every other tool
and reads nothing until you run it.

See [Check data quality](data-quality.md) for the workflow and the
[reference](../reference/data-quality.md) for Setup, keys and metric
definitions.

<a id="keys-and-result-tabs"></a>
<a id="sampling-and-budgets"></a>
<a id="data-quality-metric-definitions"></a>
<a id="columns-a-file-never-had-and-columns-it-holds-in-another-type"></a>

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
| Equal per value | Up to the sample size from each value of a column, so a small partition is represented beside a large one. At most 2,000,000 rows are kept: past that, every value keeps the same smaller number, and the header says what it was lowered to. Refused past 10,000 values |
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
