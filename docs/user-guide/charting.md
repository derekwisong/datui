# Make a chart

Press <kbd>c</kbd> to chart the current query and filters. Press <kbd>Esc</kbd>
to return to the table.

## Plot two columns

Open **Palmer penguins** from **Public datasets**, as in the
[quick start](../getting-started/quick-start.md), then:

1. Press <kbd>c</kbd> and select **XY**.
2. Set **Style** to **Scatter** with <kbd>→</kbd>.
3. Choose `flipper_length_mm` for **X axis** and `body_mass_g` for **Y series**.

Use <kbd>Tab</kbd> to move through settings and <kbd>Space</kbd> to open a
column picker. Type part of a name to narrow it; on **Y series**,
<kbd>Space</kbd> toggles a series and <kbd>Enter</kbd> closes the picker. The
picker leaves out the X column. The two penguins with no measurements are left
out.

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
| **Bar** | One horizontal bar per category | A text, categorical, boolean or integer category, and **Count** or a numeric value | Order |

XY's **Bar** style draws vertical bars at a numeric X; the **Bar** tab is for
categories.

Axis numbers take the table's digit grouping and decimal separator
([number format](../reference/settings.md#number-formatting)), with one
notation and precision per axis, chosen from its range. Counts and integer
columns tick in whole numbers. An axis too narrow for its labels shortens them,
to `12.3k` or `12,3k`. Exports and the Distribution plots label their axes the
same way.

## Count rows per category

With **Palmer penguins** open:

1. Press <kbd>c</kbd>, then <kbd>6</kbd> for **Bar**.
2. Choose `species` for **Category** and **Count** for **Value**.

The bars read Adelie 152, Gentoo 124, Chinstrap 68: no query needed.

Counts are exact. The whole view is counted in one pass that keeps a count per
category and no rows, whatever the **Sample size**. When the view has more rows
than the sample size, the chart says so under the plot: `counts of 3.5M rows`.
A view that fits in the rows the chart already read is counted from those.

| Case | What happens |
|---|---|
| More than 100,000 categories | The count stops and the chart says so. Count by a column with fewer values |
| A null category | Its own bar, labeled `∅` |
| Equal counts | A to Z |
| Another category, or leaving the chart, while it counts | The count stops; nothing partial is kept |

## Chart one value per category

With a numeric **Value**, the Bar tab charts a grouped result: one row per
category. Group first with a [query](querying-data.md), then chart it. On
**NYC flights (2013)**:

1. Press <kbd>/</kbd> and run
   `SELECT carrier, AVG(arr_delay) AS delay, COUNT(*) AS flights FROM df GROUP BY carrier ORDER BY delay DESC`.
2. Press <kbd>c</kbd>, then <kbd>6</kbd> for **Bar**.
3. Choose `carrier` for **Category** and `delay` for **Value**.

Sixteen bars, F9 longest at 21.92 minutes. HA and AS arrive early on average,
so their bars grow left of zero.

| Option | What it does |
|---|---|
| **Category** | One bar per value. Text, categorical, boolean and integer columns |
| **Value** | The bar's length, printed beside it in the table's number format. **Count**, the rows per category, or any numeric column but the category |
| **Order** | **Value**, largest first, or **Label**: text A to Z, numbers ascending |

| Case | What happens |
|---|---|
| A category repeats | Refused: the chart says how many rows and categories it found and suggests the query that groups them, or **Count**. Bars are not summed or averaged for you |
| More bars than rows | The bars that fit, then `+ 212 more` counting the rest |
| Negative values | Bars grow left of zero |
| A null category | Its own bar, labeled `∅` |
| A null value | That category is left out and counted under the plot |

A chart of values reads at most **Sample size** rows, so a grouped result with
more categories than that is sampled, and says so.

## Examples on the built-in datasets

Each starts from the dataset of that name under **Public datasets**.

| Chart | Data and query | Settings | What you see |
|---|---|---|---|
| JFK delay by hour | NYC flights, the [JFK query](querying-data.md#run-a-query) | XY, Line; X axis `hour`, Y series `mean_delay` | A climb from 0.5 minutes at 5:00 to 26.1 at 21:00 |
| A year of delays | NYC flights, the [daily query](querying-data.md#dates-and-messy-text) | XY, Line; X axis `flight_date`, Y series `delay` | 2013 on a date axis; the peak is 83.54 on 2013-03-08 |
| Three names | US baby names, the [pivot](reshaping.md#pivot) of Emma, Jennifer and Olivia | XY, Line; X axis `year`, Y series `Emma`, `Jennifer`, `Olivia` | Jennifer's peak of 63,604 in 1972; Emma and Olivia rising after 2000. Emma and Olivia start in 1880; Jennifer, with no published counts before 1916, starts there |
| Launches per year | Space launches, the [count pivot](reshaping.md#count-with-a-pivot) | XY, Line; X axis `launch_year`, Y series `F`, `O` | `O`, launches that reached orbit, near 130 a year from the late 1960s to the mid-1980s, then a slump in the 1990s; `F`, failures, along the bottom |
| Central Park highs | NOAA daily weather, `by_year/YEAR=2024/ELEMENT=TMAX`, the [station query](remote-data.md#public-data) | XY, Line; X axis `day`, Y series `high_c` | 366 daily highs from −6.0 to 35.0 °C |
| Earthquakes on a map | Earthquakes (past month), no query | XY, Scatter; X axis `longitude`, Y series `latitude` | The Pacific Ring of Fire. A sample of 10,000; set **Sample size** to **Every row** for all of them |
| Calories by chain | Food nutrition, the [restaurant summary](copying.md#copy-a-table-into-a-note) | Bar; Category `restaurant`, Value `avg_calories` | Mcdonalds first at 640, Chick Fil-A last at 384 |

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
