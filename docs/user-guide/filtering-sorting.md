# Filtering and Sorting

Press <kbd>s</kbd> to open the **Sort & Filter** sidebar. It has two tabs.
<kbd>←</kbd> <kbd>→</kbd> switch tabs when the tab bar has focus;
<kbd>Tab</kbd> and <kbd>Shift</kbd>+<kbd>Tab</kbd> move focus through the form
to **Apply**, **Cancel** and **Clear**.

| Button | Does |
|---|---|
| **Apply** | Run the changes and close |
| **Cancel** or <kbd>Esc</kbd> | Close without changing anything |
| **Clear** | Reset the current tab |

Filters and sort from this sidebar apply to the loaded table. To filter the
result of a query, put the condition in the query's `where` clause.

## Sort tab

![Sorting Demo](../demos/06-sorting.gif)

One row per column. Type to narrow the list, then:

| Key | Action |
|---|---|
| <kbd>Space</kbd> | Sort by this column; press again to remove it |
| <kbd>1</kbd> to <kbd>9</kbd> | Put this column at that position in the sort order |
| <kbd>[</kbd> <kbd>]</kbd> | Move this column earlier or later in the sort order |
| <kbd>+</kbd> <kbd>-</kbd> | Move this column left or right in the table |
| <kbd>L</kbd> | Freeze this column and every column above it on the left |
| <kbd>v</kbd> | Hide or show this column |
| <kbd>Ctrl</kbd>+<kbd>Enter</kbd> | Apply from anywhere on this tab |

Ascending or descending is a single switch for the whole sort. Nulls go last
either way, as they do in pandas, DuckDB and spreadsheets.

Back in the main view, <kbd>r</kbd> reverses the sort and <kbd>R</kbd> resets
everything: query, filters, sort, column order and frozen columns.

## Filter tab

![Filtering Demo](../demos/07-filtering.gif)

A filter is a column, an operator and a value. <kbd>Enter</kbd> adds it to the
list; add as many as you need and join them with **AND** or **OR**.

| Operator | Meaning |
|---|---|
| `=` `!=` | equal, not equal |
| `<` `>` `<=` `>=` | less, greater, or equal |
| `contains` `!contains` | text contains, or does not contain, the value |

The value is parsed as the column's type, so `> 1000` on a number column is a
numeric comparison. Filters stay in place while you chart, analyze or export,
and are saved in [templates](templates.md).

## From the query prompt

For anything more involved, the `where` clause of a
[query](querying-data.md#filtering-rows) takes expressions, `OR` groups,
null tests and date arithmetic.
