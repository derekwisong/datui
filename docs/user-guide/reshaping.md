# Pivot and Melt

Press <kbd>p</kbd> to reshape the current view between long and wide. The
sidebar has a **Pivot** tab and a **Melt** tab; <kbd>←</kbd> <kbd>→</kbd>
switch between them. Each field is one row; <kbd>Space</kbd> (or just typing)
opens a picker scoped to that row, and the full spec —
`department × job_title → avg(salary)` — is echoed above the footer as it
takes shape. <kbd>Enter</kbd> applies it from anywhere in the form.

| | From | To |
|---|---|---|
| **Pivot** (long to wide) | `id, date, key, value` | `id, date, key_A, key_B, key_C` |
| **Melt** (wide to long) | `id, Q1, Q2, Q3` | `id, variable, value` |

Both work on the table as currently queried, filtered and sorted.

## Pivot

![Pivot Demo](../demos/04-pivot.gif)

1. **Index**: the columns that stay as rows. Type to search, <kbd>Space</kbd> to select. Order matters.
2. **Columns**: the column whose distinct values become new column names.
3. **Values**: the column that fills the new cells.
4. **Aggregate**: how to combine several values per cell. `last` (default), `first`, `min`, `max`, `avg`, `med`, `std` or `count`. A string value column allows only `first` and `last`.

New columns are named after the pivot column's values, sorted alphabetically.

Pivoting reads every affected row into memory to discover the column names.
On a large table, query or filter first.

## Melt

![Melt Demo](../demos/05-melt.gif)

1. **Index**: the identifier columns to keep, selected as for pivot.
2. **Strategy**, deciding the value columns. Its own row follows the choice:
   - **All except index**: every other column.
   - **By pattern**: a regex over column names, such as `Q[1-4]_2024` or `metric_.*`, typed on a **Pattern** row.
   - **By type**: every numeric, string, datetime or boolean column, picked on a **Type** row.
   - **Explicit list**: pick them with <kbd>Space</kbd> on a **Columns** row.
3. **Variable name** and **Value name**: the two output columns. Default `variable` and `value`.

The spec line counts what the strategy resolves — `melt 48 value columns by
pattern "q_.*"` — so a pattern that matches nothing is visible before it runs.

## Keys

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the rows |
| <kbd>←</kbd> <kbd>→</kbd> | Switch tab, or move the cursor in a text field |
| <kbd>Space</kbd> or typing | Open the focused row's picker, narrowed by what you type |
| <kbd>↑</kbd> <kbd>↓</kbd> in the picker | Move; <kbd>Space</kbd> chooses, or toggles where several can be chosen |
| <kbd>Enter</kbd> | In the picker, choose; otherwise apply, from anywhere |
| <kbd>Esc</kbd> | Close the picker; pressed again, close without applying |
| <kbd>?</kbd> | Help |

## Views

A [view](views.md) saved after a pivot or melt stores the reshape. When
it is applied, the order is query, filters, sort, pivot or melt, column order.
