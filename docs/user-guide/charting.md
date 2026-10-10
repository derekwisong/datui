# Make a chart

<kbd>c</kbd> charts the rows left after the query and filters. It starts
with a chart that suits the column under the column cursor. <kbd>Esc</kbd>
returns to the table.

| Cursor column | Chart `c` opens |
|---|---|
| A number | Its histogram |
| Text, categorical or boolean | A bar of its counts per value |
| A date or a time | A line of the first numeric column over it (with no numeric column, Y is left for you to pick) |

Until you change the type, the panel says `suggested for f64` under **Type**.
Pressing <kbd>c</kbd> again on the same column of the same dataset brings back
the chart as you left it. On another column, it suggests a new chart.

## The panel

The panel on the left holds four shelves, the same four for every type,
followed by the options. A shelf the chart type does not use stays in place,
dimmed, with the reason beside it (`density`, `same as X`). Moving through the
panel skips it.

| Shelf | Holds | Under it |
|---|---|---|
| **Type** | Line, Scatter, Bar, Histogram, Box, KDE, Heatmap | `suggested for <type>` |
| **X** | The column on the X axis | The time bucket of a date X on a line or scatter; the bar order; the histogram's bins |
| **Y** | A column, or several on a line or scatter (one series each); on a histogram, count or share | On a line, scatter or bar, the **Aggregate** row, and cumulative when it is on |
| **Color** | A category column: one series per value | Which values: `top 10 of 16 by rows`, `all 3`, `3 picked of 4,812` |

The plot's title row says how the rows were combined: `mean by month,
running sum, colored by carrier`, `count per bin`, `one box per carrier`.
Column names go on the axes instead: Y above the y axis, X below the right end
of the x axis. So a plain line or scatter has a blank title row. At the right
end of the title row, dimmed, the chart says which rows it read: `sample of
10,000 of 337k rows · seed 42891`, `1,207 values outside p1-p99`. The seed
draws the same sample again; a chart of every row shows no seed. When the row
is too narrow for both, these notes are cut short with `…` or left out.

## Plot two columns

Open **Palmer penguins** from **Example datasets**, as in the
[quick start](../getting-started/quick-start.md), then:

1. Press <kbd>c</kbd>, then <kbd>2</kbd> for **Scatter**.
2. Choose `flipper_length_mm` for **X** and `body_mass_g` for **Y**.

Use <kbd>↓</kbd> to move through the panel's rows and <kbd>Space</kbd> to
open a shelf's picker. Type part of a name to narrow the list. On **Y**,
<kbd>Space</kbd> toggles a column and <kbd>Enter</kbd> closes the picker, which
leaves out the X column. The chart leaves out the two penguins with no
measurements.

Points are drawn in X order, so a line runs left to right whatever order the
table is in. Nulls are dropped per series: a row with no X is left out, and a
missing Y drops that series' point and breaks its line. Other series keep
their points.

<kbd>Esc</kbd> keeps the chart as you left it. Sort or filter the table,
press <kbd>c</kbd> on the same column, and the same shelves chart the new view. A
column the view no longer has is dropped from the chart. Opening another
dataset starts over.

## Aggregate

On a line, scatter or bar chart, the **Aggregate** row under **Y** combines
rows. <kbd>←</kbd> <kbd>→</kbd> step through none, count, distinct, sum, mean,
median, stdev, quantile, min, max, first and last. With an aggregate, the rows
that share an X (and a color) become one point or one bar. It runs over every
row of the view, as one group-by in the background, and the footer shows
`Grouping 337k rows...` meanwhile. The title row says how many rows it used:
`all 336,776 rows`, or `rows in the groups shown` when Color leaves values out
and Other is off. Without an aggregate, a chart samples, and says so.

| Aggregate | Each point or bar | Y |
|---|---|---|
| **count** | The rows | None needed |
| **distinct** | The different values, nulls left out (`nunique` in a query counts a null as one) | Any column, text too. Switching to another aggregate drops a text Y |
| **sum**, **mean**, **median**, **min**, **max** | As named | A number |
| **stdev** | The sample standard deviation (n − 1). A group of one row has none and draws no point | A number |
| **quantile** | The percentile on the line under **Aggregate**, `p90` to start; <kbd>←</kbd> <kbd>→</kbd> there step 1, 5, 10, 25, 75, 90, 95 and 99. Linear interpolation between the two nearest values | A number |
| **first**, **last** | The first or last non-null value in the view's order: its sort, which **Aggregate** names (`last · by time_hour ▲`), or else the order the rows were read in (`last · by row order`) | A number |

| Option | What it does |
|---|---|
| Time bucket | Line and Scatter, under a date or datetime X: none, day, week (from Monday), month, quarter, year. Picking a bucket with no aggregate sets **mean**; picking an aggregate on a date X with no bucket sets **day** |
| **Cumulative** | Line and Scatter, with count, sum, mean, median, min or max: **off**, **running sum**, or **compound**. The other aggregates have none, since a running sum of distinct counts, deviations, percentiles or first values is not that measure so far. The rows are totaled in X order, per series, and each point is the total at the end of its X or bucket: a running sum of Y, or Y's rates compounded over every row, `(1 + y1)(1 + y2)… − 1`. While it is on, the aggregate is set aside (a count becomes a running count of rows) and **Aggregate** says so: `mean · compound of rows` |

If a sample of X shows more than about 200,000 different values, the chart is
refused before any rows are grouped, with advice to bucket X by a time unit or
choose an X with fewer values.

On **NYC flights (2013)**, with no query: press <kbd>c</kbd> on `carrier`, pick
`arr_delay` for **Y** and step the aggregate to **mean**. Sixteen bars, F9
longest at 21.92 minutes; HA and AS arrive early on average, so their bars
grow left of zero.

![A bar chart of mean arr_delay by carrier over all 336,776 rows: F9 longest at 21.92, HA at −6.92 and AS at −9.93 left of zero](../demos/screenshots/chart-carrier-bar.png)

Which airlines arrive late? F9, by 21.92 minutes on average. <kbd>c</kbd> on
`carrier`, **Y** `arr_delay`, <kbd>→</kbd> on **Aggregate** to **mean**.

## Color

**Color** splits a chart into one series per value of a category column, each
in its own palette color. Until you pick values, the ten values with the most
rows are drawn (or all of them, when the view has fewer), and the line under
**Color** says which. <kbd>Space</kbd> on that line lists every value with its
row count, most first. Type to narrow the list, <kbd>Space</kbd> toggles a
value (up to ten), and <kbd>Enter</kbd> charts the ones picked. Values are
counted over the whole view.

![Mean dep_delay by hour colored by origin, all 336,776 rows: three lines, EWR highest at 31.1 minutes at 19:00](../demos/screenshots/chart-color-origin.png)

Which New York airport leaves latest, and when? Newark in the evening,
31.1 minutes on average at 19:00. A line of `dep_delay` by `hour`,
**Aggregate** **mean**, **Color** `origin`.

**Other** groups the values without a series of their own into one more
series, drawn in `dimmed` and listed last in the legend. <kbd>←</kbd>
<kbd>→</kbd> on the line under **Color** turn it on or off. The line then reads
`top 10 + 6 other` or `3 picked + 4,809 other`. It starts on for a scatter and off for the other
types. When every value has a series there is no Other.

| Type | With Color |
|---|---|
| Line, Scatter | A line or set of points per value. Other is one more line, aggregated over the rest as the others are; on a scatter its points are drawn under the colored ones, so the cloud keeps its shape. Several Y columns are already one series each, so Color is dimmed |
| Bar | A bar per value in each category's row, under a legend of the values; Other is one more bar per category. Needs an aggregate |
| Histogram | Each value's bins drawn as an outline, since filled bars would hide each other. **Y** can be **share of group**: each bin's share of its group's rows inside the range, so groups of different sizes compare. Other is one more outline. The range (**p1-p99**) is taken from the whole column |
| KDE | A curve per value; Other is one more |
| Box | Dimmed: a category on **X** already makes one box per value, for the ten values with the most rows |
| Heatmap | Dimmed |

A null value is a series of its own, named `null`.

## Chart types

| Type | Plots | X | Y |
|---|---|---|---|
| **Line** | Lines in X order | A number, date or time | Up to ten numeric columns, or a count |
| **Scatter** | Points | A number, date or time | Up to ten numeric columns, or a count |
| **Bar** | One horizontal bar per category | A text, categorical, boolean or integer column | A numeric column with an aggregate, a count, or a column already one row per category |
| **Histogram** | Rows per bin | A number | Count or share |
| **Box** | Quartiles and whiskers | A category, one box per value, or none | A number |
| **KDE** | A smoothed density curve | A number | Density |
| **Heatmap** | Density of two variables | A number | A number |

| Option | Types | What it does |
|---|---|---|
| **Bins** | Histogram (under X), Heatmap | 5 to 100 bins for a histogram, 5 to 60 a side for a heatmap |
| **Bandwidth** | KDE | A multiple of the usual bandwidth, 0.2x to 5.0x |
| **Range** | Histogram, Box, KDE | **All** values, or **p1-p99** (the 1st to 99th percentile) to leave out outliers that squash the rest; the chart counts what it left out |
| Order | Bar (under X) | By value, largest first, or by label: text A to Z, numbers ascending |
| **Y from zero**, **Log scale** | Line, Scatter | Start the Y axis at zero; plot ln(1 + y), with the ticks labeled in y |
| **Legend** | Line, Scatter, Bar, Histogram, KDE | **auto** or **off** |
| **Grid** | Line, Scatter, Histogram, Box, KDE | Lines at the labeled ticks |
| **Rows** | Charts that sample | **Sample** of a size, or **Every row**; an aggregate reads every row and says `all, exact`. See [Large tables](#large-tables) |

## Axes, grid and legend

Ticks fall on round values, steps of 1, 2 or 5 times a power of ten (or 25,
250 and so on), as many as the plot has room for: about one label per 15
columns and one per 4 rows. The y axis runs from the round value below the
data to the one above it. Smaller unlabeled ticks mark the steps between
labels where there is room.

| Axis | Ticks and labels |
|---|---|
| Numbers | One notation and precision per axis, in the table's digit grouping and decimal separator ([number format](configuration.md#number-formatting)). Counts and integer columns tick in whole numbers |
| Dates and times | Calendar boundaries: years, months, days, hours, minutes. A label names the unit that turns there: `2026` at a new year, `Apr` at a new month, `Mar 5` at a new day among hours, otherwise `12` or `06:00`. The first label also names the year |
| Log scale | 1, 10, 100 and on, with 2 and 5 between when there is room, and 0 where the data starts there; one format, shortened to `1k`, `10k`, `1M` when narrow |
| Crowded | Fewer ticks first, then shorter labels (`12.3k`, `12,3k`). A crowded time axis labels only its ends and middle. An axis that would get only two labels takes a finer step when it fits: `0 2 4 6`, not `0 5` |

| Feature | What it does |
|---|---|
| **Grid** | Dotted lines at the labeled ticks, under the series, in `chart_grid`. <kbd>g</kbd> or the **Grid** row toggles it; `chart_grid` in the [`[analysis]` section](../reference/settings.md#analysis) sets whether a new chart starts with it (default off) |
| **Legend** | Names the series when there are two or more: a swatch and a name per series, with no frame, placed where it covers the fewest points (a corner, or the middle of an edge). The **Legend** row hides it |
| **Crosshair** | Line and Scatter: <kbd>x</kbd> passes the keys to the plot. <kbd>←</kbd> <kbd>→</kbd> move a vertical line from point to point (a column at a time where they crowd), <kbd>Home</kbd> <kbd>End</kbd> go to the ends, and under the plot a readout gives x and each series' value there: `date: 2020-04-30   high_temp: 67.2`. A series with no value there reads `∅`. <kbd>x</kbd>, <kbd>Tab</kbd> or <kbd>Esc</kbd> hand the keys back to the panel. A click on the plot puts the crosshair there |
| Marks | Lines in braille. A scatter marks each point with a dot, or in braille when there is more than one point per four cells. Histogram bars fill their bins |

Without UTF-8 the grid is `.` and `:` and the tick marks `+`. Exports and the
Distribution plots write their numbers the same way.

## Count rows per category

With **Palmer penguins** open, press <kbd>c</kbd> on `species` for a bar
chart of its counts, with no query needed: Adelie 152, Gentoo 124, Chinstrap 68.

Counts are exact, whatever the sample size. One pass over the whole view keeps
a count per category and no rows, and the title row says so: `all 344 rows`.

| Case | What happens |
|---|---|
| More than 100,000 categories | The count stops and the chart says so. Count by a column with fewer values |
| A null category | Its own bar, labeled `∅` |
| Equal counts | A to Z |
| Switching to another column, or leaving the chart, during the count | The count stops; nothing partial is kept |

## Chart one value per category

With the aggregate **none**, a bar chart takes a result that is already one row
per category: the bar is the row's value. Group first with a
[query](querying-data.md), then chart it. On **NYC flights (2013)**:

1. Press <kbd>:</kbd> and run
   `SELECT carrier, AVG(arr_delay) AS delay, COUNT(*) AS flights FROM df GROUP BY carrier ORDER BY delay DESC`.
2. Press <kbd>c</kbd> on `carrier`, pick `delay` for **Y**, and step the
   aggregate to **none**.

The same sixteen bars as the mean above, F9 longest at 21.92 minutes.

| Case | What happens |
|---|---|
| A category repeats | Refused: the chart says how many rows and categories it found and suggests the query that groups them, or an aggregate. Bars are not summed or averaged for you |
| More bars than the plot has rows | The bars that fit, then `+ 212 more` counting the rest |
| Negative values | Bars grow left of zero |
| A null category | Its own bar, labeled `∅` |
| A null value | That category is left out and counted in the title row |

A chart with no aggregate reads at most **Rows** rows, so a grouped result
with more categories than that is sampled, and says so.

## Examples on the built-in datasets

Each starts from the dataset of that name under **Example datasets**.

| Chart | Data and query | Settings | What you see |
|---|---|---|---|
| JFK delay by hour | NYC flights, the [JFK query](querying-data.md#run-a-query) | Line; X `hour`, Y `mean_delay` | A climb from 0.5 minutes at 5:00 to 26.1 at 21:00 |
| A year of delays | NYC flights, the [daily query](querying-data.md#dates-and-messy-text) | Line; X `flight_date`, Y `delay` | 2013 on a date axis; the peak is 83.54 on 2013-03-08 |
| Delay by carrier | NYC flights, no query | Bar; X `carrier`, Y `arr_delay`, mean | F9 longest at 21.92 minutes |
| Names per year | US baby names, no query | Line; X `year`, Y `name`, distinct | 1,889 different names in 1880, a peak of 32,510 in 2008 |
| Delay spread by month | NYC flights, no query | Line; X `month`, Y `dep_delay`, stdev | Widest in July at 51.6 minutes, narrowest in November at 27.6 |
| Late departures by month | NYC flights, no query | Line; X `month`, Y `dep_delay`, quantile `p90` | One flight in ten leaves 79 or more minutes late in July, 26 in September and November |
| Last flights of 2013 | NYC flights, sorted by `time_hour` | Bar; X `carrier`, Y `dep_delay`, last | WN's last departure of the year 48 minutes late, FL's 14 minutes early |
| Three names | US baby names, the [pivot](reshaping.md#pivot) of Emma, Jennifer and Olivia | Line; X `year`, Y `Emma`, `Jennifer`, `Olivia` | Jennifer's peak of 63,604 in 1972; Emma and Olivia rising after 2000. Emma and Olivia start in 1880; Jennifer, with no published counts before 1916, starts there |
| Launches per year | Space launches, the [count pivot](reshaping.md#count-with-a-pivot) | Line; X `launch_year`, Y `F`, `O` | `O`, launches that reached orbit, near 130 a year from the late 1960s to the mid-1980s, then a slump in the 1990s; `F`, failures, along the bottom |
| Central Park highs | NOAA daily weather, `by_year/YEAR=2024/ELEMENT=TMAX`, the [station query](remote-data.md#examples-on-public-data) | Line; X `day`, Y `high_c` | 366 daily highs from −6.0 to 35.0 °C |
| Earthquakes on a map | Earthquakes (past month), no query | Scatter; X `longitude`, Y `latitude` | The Pacific Ring of Fire. A sample of 10,000; set **Rows** to **Every row** for all of them |
| Calories by chain | Food nutrition, the [restaurant summary](copying.md#copy-a-table-into-a-note) | Bar; X `restaurant`, Y `avg_calories`, none | Mcdonalds first at 640, Chick Fil-A last at 384 |

![JFK's mean departure delay by hour, from the JFK query: a climb from 0.5 minutes at 5:00 to 26.1 at 21:00](../demos/screenshots/chart-jfk-hour.png)

When does a JFK departure leave late? In the evening, 26.1 minutes on average
at 21:00. The [JFK query](querying-data.md#run-a-query), then <kbd>c</kbd>
<kbd>1</kbd> on `mean_delay`, **X** `hour`.

![Emma, Jennifer and Olivia per year from the pivot: Jennifer peaks above 60,000 in the early 1970s, Emma and Olivia rise after 2000](../demos/screenshots/chart-names.png)

How did three names rise and fall? Jennifer peaks at 63,604 in 1972; Emma and
Olivia rise after 2000. The [pivot](reshaping.md#pivot), then <kbd>c</kbd>
<kbd>1</kbd>, **X** `year`, **Y** all three.

![Earthquakes of the past month, longitude against latitude: the Pacific Ring of Fire](../demos/screenshots/chart-quakes.png)

Where does the earth shake? Around the Pacific. <kbd>c</kbd> <kbd>2</kbd> on
`latitude`, **X** `longitude`. The feed rolls, so your month shows its own points.

## Large tables

| Option | What it does |
|---|---|
| **Rows** | Rows a chart without an aggregate reads. A larger table is sampled across all of it, and the chart says so at the right of the title row, with the sample's seed: `sample of 10,000 of 3.5M rows · seed 42891`. **Every row** reads the whole view. A **Line** over a larger table is not sampled: see below |
| Aggregate | Reads every row, whatever the sample size |
| The view's [sample](sampling.md) | Read whole: the **Rows** row is hidden, and the title row names the sample and its seed: `sample 100,000 of 3.48M · seed 42891` |

On **Rows**:

| Key | Does |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>Space</kbd> | Switch between **Sample 10,000** and **Every row (36.8M)**, which names the view's rows once the table has counted them |
| Digits | Type a sample size in place: `50000`, `50,000`, `50k`, `250k`, `2m`. A size of at least the view's rows is **Every row** |
| <kbd>Backspace</kbd> | Edit the size being typed |
| <kbd>Enter</kbd> | Read what the row says. Leaving the row reads it too |
| <kbd>Esc</kbd> | Put the row back as it was, without reading |

Nothing is read while you edit the row: the chart stays as drawn, and the
line under the row says `Enter to read`. An invalid size (`12x`, `0`) says why
there and is not read. **Sample** keeps its size while **Every row** is
chosen.

The sample is drawn as the [analysis tools](analysis-features.md#sampling)
draw theirs, with the same seed: 50 blocks of rows from one Parquet or IPC file, or one
streamed pass over anything else. Changing an option, or charting columns
already read, reuses the same rows without reading the table again. An
exported chart carries the same notes under the plot. The default size comes
from `chart_rows` in the
[`[analysis]` config section](../reference/settings.md#analysis) and is 10,000.

A **Line** chart with no aggregate and no color, over more rows than the sample
size, draws an envelope instead: X is cut into half as many steps as the sample
size, and each step draws its lowest and highest value, so every peak of a
waveform or a long time series stays on the plot where a sample would miss it.
It reads the view in two streamed passes: one for the row count and X's range,
then one for the steps. The title row says so: `min and max of 192M rows in
5,000 steps`. Scatter still samples. So does a Line over Parquet read in place
from S3, GCS or Azure, since the envelope would download the whole file twice.

## Keys

| Key | Action |
|---|---|
| <kbd>1</kbd>–<kbd>7</kbd> | Switch the type directly, from anywhere: Line, Scatter, Bar, Histogram, Box, KDE, Heatmap (<kbd>[</kbd> <kbd>]</kbd> step) |
| <kbd>↓</kbd> <kbd>↑</kbd> or <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between the panel's rows |
| <kbd>Space</kbd> <kbd>Enter</kbd> | Open a shelf's picker, or the Color values; toggle an option, or take its next value. The panel applies as it changes, so <kbd>Enter</kbd> acts as <kbd>Space</kbd> does |
| <kbd>←</kbd> <kbd>→</kbd> | Step the type, the time bucket, the aggregate, cumulative, bins, range, order or sample size (<kbd>+</kbd> <kbd>-</kbd> too, <kbd>PgUp</kbd> <kbd>PgDn</kbd> for bigger steps on the sample size); on a shelf of one column, the previous or next column; flip a toggle |
| <kbd>g</kbd> | Grid on or off |
| <kbd>x</kbd> | The crosshair, on a line or scatter chart |
| <kbd>e</kbd> | Export the chart |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> | Back to the table |

In a picker, type part of a name to narrow the list and use <kbd>↑</kbd>
<kbd>↓</kbd> to move. <kbd>Enter</kbd> chooses. <kbd>Space</kbd> chooses too
until you type, and then it types a space. On a line or scatter chart's **Y**
and on the Color values, <kbd>Space</kbd> toggles an entry in or out instead.
<kbd>Tab</kbd> or <kbd>Shift</kbd>+<kbd>Tab</kbd> chooses and moves to the next
or previous row, and <kbd>Esc</kbd> closes just the picker.

## Export

<kbd>e</kbd> in the chart view opens the export dialog, with the cursor on its path. Type a
path and press <kbd>Enter</kbd>; the keys are those of every
[dialog](../reference/dialogs.md), and <kbd>Ctrl</kbd>+<kbd>P</kbd> recalls a
path exported to before. A path ending `.png`, `.svg` or `.pdf` takes that
format; any other path gets **Format**'s extension added (`chart.v2` writes
`chart.v2.png`). You are asked before an
existing file is overwritten, and a failed export leaves it as it was
([Overwriting](exporting-data.md#overwriting)). A blank path, or a failed
write, keeps the dialog open with the reason under the fields.

| Field | What it sets |
|---|---|
| **Format** | **PNG**, **SVG** or **PDF**; SVG and PDF are vector, their text set as outlines |
| **Style** | **Light**: white, with colors that stay apart for color-blind readers and in print. **Dark**: the terminal theme's colors. **Transparent**: Light with no background |
| **Size** | A preset, below; typing **Width** or **Height** makes it **Custom** |
| **Legend** | **Line ends** names each line at its right end, the default for line and KDE charts; other charts default to a legend at the top right. Or a corner, or **Off**. A chart whose legend is off exports with it off |
| **Opacity** | Scatter: **Auto** (the default) draws points opaque up to 1,000 and fainter as there are more, down to 15% from 100,000, so a dense cloud shows where it is densest; or **100%**, **50%**, **20%** |
| **Point size** | Scatter: **Small**, **Medium** (the default) or **Large**, 1.6, 2.4 or 3.6 pt |
| **Line width** | Line, colored or not: **Thin**, **Normal** (the default) or **Bold**, 1, 1.5 or 2.5 pt; the legend's swatches follow |
| **Y from zero** | Line: **On** includes zero in the Y axis. It starts as the chart's **Y from zero** option. Bars always start at zero |
| **Title**, **Description** | Over the chart. The description starts as the plot's title row, which says how the chart is made: `Mean by month, colored by carrier`; empty when it has none. The Y column is named over the plot's left edge, as on screen |
| **Notes** | Under the chart, after what the chart says about its rows (a sample, values a range left out) |
| **Source**, **Byline** | The last line: `Source: …` and the byline. Source starts from the catalog entry when the dataset came from one: its name, publisher and license |
| **Recipe** | **Include** (the default, from [`chart.export_recipe`](../reference/settings.md#chart)) writes how the chart was made into the file; **Omit** writes no datui metadata at all |

| Size | Pixels | Prints at |
|---|---|---|
| Slide 16:9 | 1920 × 1080 | 10 × 5.6 in |
| Document | 1600 × 1000 | 10 × 6.25 in |
| Square | 1200 × 1200 | 8 × 8 in |
| Single column | 1050 × 788 | 3.5 × 2.6 in, 300 dpi |
| Double column | 2100 × 1300 | 7 × 4.3 in, 300 dpi |
| Custom | 16 to 8,192 a side | the last preset's resolution |

The recipe is a JSON document that reads back as a [view](views.md):

| Key | Holds |
|---|---|
| `datui` | The version that wrote it |
| `source` | The path, or the URL without its user, password, query string or fragment |
| `table` | The table of a file of tables, when it is one |
| `rows` | What the chart read: the sample with its seed, or `every row` |
| `settings` | The view's: the sample's scope, method, size, seed and how it was drawn, with the view it was drawn through; the query, filters, sort, column layout and types, reshape; the chart, its options and its export settings, the dialog's words included |

The image is the same either way. The recipe can carry paths, bucket names and
query values, so choose **Omit** before sharing a file that should not show
them.

| Format | Recipe in |
|---|---|
| PNG | An `iTXt` chunk, keyword `datui-recipe` |
| SVG | A `<metadata>` element, the root's first child |
| PDF | The document info, `/DatuiRecipe` |

```bash,expect=screen
datui -c chart.export_recipe=false
```

Text is set in IBM Plex Sans, bundled with datui, so a chart comes out the same
on every machine; a character it lacks falls back to a system font. Its size
follows the page: smaller on a journal column, larger on a slide. A bar chart
exports up to its first 100 bars and counts the rest.

## Colors

Series take `chart_1` through `chart_10` from the
[theme](../reference/settings.md#colors), in order; bars and histograms take
`chart_1`, Other `dimmed`, and the grid `chart_grid`. The **Dark** export takes
the same slots, its background `background`, and its text `text_primary` and
`text_secondary`.

Two series never share a color. A slot the theme gives the same color as an
earlier one is skipped. On a 256- or 16-color terminal, or under `NO_COLOR`,
slots are compared as the terminal shows them, and a chart draws one series per
distinct color. So a 16-color terminal draws fewer series (`top 4 of 16 by
rows`), with Other for the rest when it is on. A **Dark** export skips a
repeated slot too, and otherwise always has all ten.
