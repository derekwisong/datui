# Filtering and Sorting

Press <kbd>s</kbd> to open the **Sort & Filter** sidebar. It has two tabs,
**Columns** and **Filters**; <kbd>←</kbd> <kbd>→</kbd> switch them, and
<kbd>Tab</kbd> moves between the tab bar and the body.

| Key | Does |
|---|---|
| <kbd>Enter</kbd> | Apply everything staged and close (on the Filters tab it adds or edits; <kbd>a</kbd> applies there) |
| <kbd>Esc</kbd> | Close without changing anything |
| <kbd>C</kbd> | Clear the current tab's staged state |

Filters and sort from this sidebar apply to the loaded table. To filter the
result of a query, put the condition in the query's `where` clause. While a
filter or query narrows the view, the bottom bar reads `Rows: 417 of 1,000`.
Canceling the sidebar discards whatever was staged; reopening it shows what
is actually applied.

## Columns tab

![Sorting Demo](../demos/06-sorting.gif)

One row per column, with its lock, its place and direction in the sort
(`1▲`, `2▼`), and a `⊘` when it is hidden. Each sorted column's header in the
table carries its own direction mark (▲/▼), so the sort and <kbd>r</kbd>
reversing it are visible at a glance. Type in the find field to narrow the
list, then:

| Key | Action |
|---|---|
| <kbd>Space</kbd> | Cycle this column's sort: none, ascending, descending |
| <kbd>Del</kbd> | Remove this column from the sort |
| <kbd>1</kbd> to <kbd>9</kbd> | Put this column at that position in the sort order; <kbd>0</kbd> removes it |
| <kbd>[</kbd> <kbd>]</kbd> | Move this column earlier or later in the sort order |
| <kbd>+</kbd> <kbd>-</kbd> | Move this column left or right in the table |
| <kbd>L</kbd> | Freeze this column and every column above it on the left |
| <kbd>v</kbd> | Hide or show this column (it stays in the list, dimmed) |

Every column carries its own direction, so `salary` can run descending while
`start_date` runs ascending. Nulls go last either way, as they do in pandas,
DuckDB and spreadsheets.

Back in the main view, <kbd>r</kbd> reverses every direction at once and
<kbd>R</kbd> resets everything: query, filters, sort, column order and frozen
columns.

## Filters tab

![Filtering Demo](../demos/07-filtering.gif)

Each filter is one row: column, operator, value, and how it joins the row
above (**and**/**or**). The cursor walks the rows plus a trailing
`add filter…` row.

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Edit this row, or start a new filter on the add row |
| <kbd>Space</kbd> | Toggle and/or on this row |
| <kbd>d</kbd> <kbd>Del</kbd> | Delete this row |

Editing walks three steps on the row: pick the column (type to narrow,
<kbd>↑</kbd> <kbd>↓</kbd> move, <kbd>Enter</kbd> chooses), pick the operator
the same way, then type the value — <kbd>Enter</kbd> saves the row, and
<kbd>Esc</kbd> abandons the edit and only the edit.

| Operator | Meaning |
|---|---|
| `=` `!=` | equal, not equal |
| `<` `>` `<=` `>=` | less, greater, or equal |
| `contains` `!contains` | text contains, or does not contain, the value |

The value is parsed as the column's type, so `> 1000` on a number column is a
numeric comparison. Filters stay in place while you chart, analyze or export,
and are saved in [views](views.md).

## From the query prompt

For anything more involved, the `where` clause of a
[query](querying-data.md#filtering-rows) takes expressions, `OR` groups,
null tests and date arithmetic.
