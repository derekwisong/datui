# Charting

Press <kbd>c</kbd> to chart the current view. Tabs across the top show the
chart type — switch it from anywhere with <kbd>1</kbd>–<kbd>5</kbd> or
<kbd>[</kbd> <kbd>]</kbd> — and the sidebar holds the active type's columns
and options. <kbd>Esc</kbd> returns to the table.

![Charting Demo](../demos/10-charting.gif)

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
[`[chart]` config section](configuration.md#charts) and is 10,000.

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
<kbd>Enter</kbd> chooses, <kbd>Space</kbd> toggles a Y series in or out, and
<kbd>Esc</kbd> backs out of the picker alone.

## Export

<kbd>e</kbd> in the chart view opens the export dialog. Choose **PNG** or
**EPS**, type a path, and press <kbd>Enter</kbd>. The extension is added when
missing, and you are asked before an existing file is overwritten.

## Colors

Series take `chart_series_color_1` through `chart_series_color_7` from the
[theme](configuration.md#colors).
