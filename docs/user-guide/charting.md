# Charting

Press <kbd>c</kbd> to chart the current view. Tabs across the top pick the
chart type; a sidebar picks columns and options. <kbd>Esc</kbd> returns to the
table.

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
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move focus: tab bar, then each sidebar field |
| <kbd>←</kbd> <kbd>→</kbd> | On the tab bar, change chart type. On plot style, cycle line, scatter, bar. On bins, bandwidth or limit, adjust |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move through a column list |
| <kbd>Enter</kbd> <kbd>Space</kbd> | Select a column, or toggle a Y series or an option |
| <kbd>+</kbd> <kbd>-</kbd> | Adjust bins, bandwidth or the row limit |
| <kbd>e</kbd> | Export the chart |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> | Back to the table |

Column lists have a search box: type part of a name to narrow them.

## Export

<kbd>e</kbd> in the chart view opens the export dialog. Choose **PNG** or
**EPS**, type a path, and press <kbd>Enter</kbd>. The extension is added when
missing, and you are asked before an existing file is overwritten.

## Colors

Series take `chart_series_color_1` through `chart_series_color_7` from the
[theme](configuration.md#colors).
