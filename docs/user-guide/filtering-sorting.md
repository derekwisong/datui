# Sort, filter and arrange columns

Press <kbd>s</kbd> to open the **Sort & Filter** sidebar. It has two tabs,
**Columns** and **Filters**; <kbd>←</kbd> <kbd>→</kbd> switch them, and
<kbd>Tab</kbd> moves between the tab bar and the body.

## Filter rows and sort a column

With the [quick-start penguin data](../getting-started/quick-start.md):

1. Press <kbd>s</kbd> and select **Filters**.
2. Add a filter: `body_mass_g`, `>`, `5000`. Press <kbd>Enter</kbd> to save the row, then <kbd>a</kbd> to apply.
3. Open <kbd>s</kbd> again. On **Columns**, select `body_mass_g` and press <kbd>Space</kbd> twice for descending order.
4. Press <kbd>Enter</kbd> to apply. The 61 matching penguins appear heaviest first.

## Apply or cancel changes

| Key | Does |
|---|---|
| <kbd>Enter</kbd> | Apply everything staged and close (on the Filters tab it adds or edits; <kbd>a</kbd> applies there, outside the row editor) |
| <kbd>Ctrl</kbd>+<kbd>Enter</kbd> or <kbd>Ctrl</kbd>+<kbd>J</kbd> | Apply from anywhere, including mid-edit — the row in progress is saved. <kbd>Ctrl</kbd>+<kbd>Enter</kbd> needs a terminal that tells it from <kbd>Enter</kbd>; <kbd>Ctrl</kbd>+<kbd>J</kbd> works on every terminal |
| <kbd>Esc</kbd> | Close without changing anything |
| <kbd>C</kbd> | Clear the current tab's staged state (with the body focused) |

Sidebar filters and sort apply to the current query or reshape result. Running
a new query clears them, so apply the query first and the sidebar settings
afterward. The bottom bar shows matching and total row counts, such as `417 of 1,000`.
Large totals are abbreviated; **Info** shows the exact total.
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
| <kbd>1</kbd> to <kbd>9</kbd> | Put this column at that position in the sort order; <kbd>0</kbd> removes it. A digit past the end of the order says so on the status line |
| <kbd>[</kbd> <kbd>]</kbd> | Move this column earlier or later in the sort order |
| <kbd>+</kbd> <kbd>-</kbd> | Move this column left or right in the table |
| <kbd>L</kbd> | Freeze this column and every column above it on the left |
| <kbd>v</kbd> | Hide or show this column (it stays in the list, dimmed) |

Every column carries its own direction, so `salary` can run descending while
`start_date` runs ascending. Nulls go last in either direction.

Back in the main view, <kbd>r</kbd> reverses every direction at once and
<kbd>R</kbd> resets everything: query, filters, sort, column order and hidden
columns, frozen columns, pivot/melt, drill-down and the applied view.

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
<kbd>Esc</kbd> abandons the edit and only the edit. That sequence works on
every terminal: finish the row with <kbd>Enter</kbd>, then press
<kbd>a</kbd> to apply.

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
