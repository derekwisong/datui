# Make a chart

Press <kbd>c</kbd> to chart the current query and filters. Press <kbd>Esc</kbd>
to return to the table.

## Plot two columns

Open the [quick-start penguin data](../getting-started/quick-start.md), then:

1. Press <kbd>c</kbd> and select **XY**.
2. Set **Plot style** to **Scatter**.
3. Choose `flipper_length_mm` for **X** and `body_mass_g` for **Y**.

Use <kbd>Tab</kbd> to move through settings and <kbd>Space</kbd> to open a
column picker. Type part of a name to narrow it; on Y, <kbd>Space</kbd>
toggles a series and <kbd>Enter</kbd> closes the picker. The Y picker leaves
out the X column.
The chart contains the 342 rows with both measurements present.

Points are drawn in X order, so a line runs left to right whatever order the
table is in. Nulls are dropped per series: a row with no X is left out, and a
missing Y drops that series' point and breaks its line. Other series keep
their points.

Esc keeps the chart as you left it: sort or filter the table, press
<kbd>c</kbd>, and the same columns chart the new view. A column the view no
longer has is dropped from the chart. Opening another dataset starts over.

## Chart types

| Tab | Plots | Columns | Options |
|---|---|---|---|
| **XY** | Line, scatter or bar | One numeric or temporal X, up to seven numeric Y | Y from zero, log scale, legend |
| **Histogram** | Counts per bin | One numeric column | Bins, range |
| **Box Plot** | Quartiles and outliers | One numeric column | Range |
| **KDE** | A smoothed density curve | One numeric column | Bandwidth, range |
| **Heatmap** | Density of two variables | Numeric X and Y | Bins |
| **Bar** | One horizontal bar per category | A text, categorical, boolean or integer category, and a numeric value | Order |

XY's **Bar** style draws vertical bars at a numeric X; the **Bar** tab is for
categories.

## Chart one value per category

The Bar tab charts a grouped result: one row per category. Group first with a
[query](querying-data.md), then chart it. With the
[quick-start penguin data](../getting-started/quick-start.md):

1. Press <kbd>/</kbd> and run `select avg body_mass_g by species`.
2. Press <kbd>c</kbd>, then <kbd>6</kbd> for **Bar**.
3. Choose `species` for **Category** and `avg_body_mass_g` for **Value**.

| Option | What it does |
|---|---|
| **Category** | One bar per value. Text, categorical, boolean and integer columns |
| **Value** | The bar's length, printed beside it. Any numeric column but the category |
| **Order** | **Value**, largest first, or **Label**: text A to Z, numbers ascending |

| Case | What happens |
|---|---|
| A category repeats | Refused: the chart says how many rows and categories it found and suggests the query that groups them. Bars are not summed or averaged for you |
| More bars than rows | The bars that fit, then `+ 212 more` counting the rest |
| Negative values | Bars grow left from a zero line |
| A null category | Its own bar, labeled `∅` |
| A null value | That category is left out and counted under the plot |

A chart reads at most **Sample size** rows, so a grouped result with more
categories than that is sampled, and says so.

## Large tables

| Option | What it does |
|---|---|
| **Sample size** | Rows the chart reads. A larger table is sampled across all of it, and the chart says so under the plot: `sample of 10,000 of 3.5M rows`. **Every row** reads the whole view |
| **Range** | Histogram, Box Plot and KDE: **All** values, or **p1-p99** (the 1st to 99th percentile) to leave out outliers that squash the rest into a bin or two. The chart counts what it left out: `195 values outside p1-p99` |

The sample is drawn as the [analysis tools](analysis-features.md#sampling)
draw theirs, with the same seed: 50 runs of one Parquet or IPC file, or one
streamed pass over anything else. Another option, or another chart of columns
already read, draws from the same rows without reading the table again. An
exported PNG or EPS carries the same notes under the plot. The default size
comes from `row_limit` in the
[`[chart]` config section](../reference/settings.md#charts) and is 10,000.

## Keys

| Key | Action |
|---|---|
| <kbd>1</kbd>–<kbd>6</kbd> | Switch chart type directly, from anywhere (<kbd>[</kbd> <kbd>]</kbd> cycle) |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the option rows |
| <kbd>Enter</kbd> <kbd>Space</kbd> | Open a column row's picker, toggle an option, or cycle the plot style, range or bar order |
| <kbd>←</kbd> <kbd>→</kbd> | Cycle the plot style, range or bar order, or adjust bins, bandwidth or the sample size (<kbd>+</kbd> <kbd>-</kbd> too, <kbd>PgUp</kbd> <kbd>PgDn</kbd> for bigger steps on the sample size) |
| <kbd>e</kbd> | Export the chart |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> | Back to the table |

In the picker: type part of a name to narrow, <kbd>↑</kbd> <kbd>↓</kbd> move,
<kbd>Enter</kbd> or <kbd>Space</kbd> chooses (on the Y series row
<kbd>Space</kbd> toggles a series in or out), and <kbd>Esc</kbd> backs out of
the picker alone.

## Export

<kbd>e</kbd> in the chart view opens the export dialog. Choose **PNG** or
**EPS**, type a path, and press <kbd>Enter</kbd>. The extension is added when
missing, and you are asked before an existing file is overwritten. A bar chart
exports up to its first 100 bars; the category axis counts the rest.

## Colors

Series take `chart_series_color_1` through `chart_series_color_7` from the
[theme](../reference/settings.md#colors); bars and histograms take
`primary_chart_series_color`.
