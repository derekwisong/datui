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
toggles a series and <kbd>Enter</kbd> closes the picker.
The chart contains the 342 rows with both measurements present.

## Chart types

| Tab | Plots | Columns | Options |
|---|---|---|---|
| **XY** | Line, scatter or bar | One numeric or temporal X, up to seven numeric Y | Y from zero, log scale, legend |
| **Histogram** | Counts per bin | One numeric column | Bins |
| **Box Plot** | Quartiles and outliers | One numeric column | |
| **KDE** | A smoothed density curve | One numeric column | Bandwidth |
| **Heatmap** | Density of two variables | Numeric X and Y | Bins |

Every type has a **Limit Rows** option at the bottom of the sidebar: how many
rows are used to build the chart. The default comes from `row_limit` in the
[`[chart]` config section](../reference/settings.md#charts) and is 10,000. This is a row cap, not random sampling. Filter or aggregate
first when the chart needs to represent a larger period or dataset.

## Keys

| Key | Action |
|---|---|
| <kbd>1</kbd>–<kbd>5</kbd> | Switch chart type directly, from anywhere (<kbd>[</kbd> <kbd>]</kbd> cycle) |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the option rows |
| <kbd>Enter</kbd> <kbd>Space</kbd> | Open a column row's picker, toggle an option, or cycle the plot style |
| <kbd>←</kbd> <kbd>→</kbd> | Cycle the plot style, or adjust bins, bandwidth or the row limit (<kbd>+</kbd> <kbd>-</kbd> too, <kbd>PgUp</kbd> <kbd>PgDn</kbd> for bigger steps on the limit) |
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
missing, and you are asked before an existing file is overwritten.

## Colors

Series take `chart_series_color_1` through `chart_series_color_7` from the
[theme](../reference/settings.md#colors).
