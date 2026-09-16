# Pivot and Melt

Press <kbd>p</kbd> to reshape the current view between long and wide. The
dialog has a **Pivot** tab and a **Melt** tab; <kbd>←</kbd> <kbd>→</kbd> switch
between them when the tab bar has focus.

| | From | To |
|---|---|---|
| **Pivot** (long to wide) | `id, date, key, value` | `id, date, key_A, key_B, key_C` |
| **Melt** (wide to long) | `id, Q1, Q2, Q3` | `id, variable, value` |

Both work on the table as currently queried, filtered and sorted.

## Pivot

![Pivot Demo](../demos/04-pivot.gif)

1. **Index columns**: the columns that stay as rows. Type to search, <kbd>Space</kbd> to select. Order matters.
2. **Pivot column**: the column whose distinct values become new column names.
3. **Value column**: the column that fills the new cells.
4. **Aggregation**: how to combine several values per cell. `last` (default), `first`, `min`, `max`, `avg`, `med`, `std` or `count`. A string value column allows only `first` and `last`.

New columns are named after the pivot column's values, sorted alphabetically.

Pivoting reads every affected row into memory to discover the column names.
On a large table, query or filter first.

## Melt

![Melt Demo](../demos/05-melt.gif)

1. **Index columns**: the identifier columns to keep, selected as for pivot.
2. **Value columns**, by one of four strategies:
   - **All except index**: every other column.
   - **By pattern**: a regex over column names, such as `Q[1-4]_2024` or `metric_.*`.
   - **By type**: every numeric, string, datetime or boolean column.
   - **Explicit list**: pick them with <kbd>Space</kbd>.
3. **Variable name** and **Value name**: the two output columns. Default `variable` and `value`.

## Keys

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move focus: tab bar, fields, **Apply**, **Cancel**, **Clear** |
| <kbd>←</kbd> <kbd>→</kbd> | Switch tab, or move the cursor in a text field |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move through a list |
| <kbd>Space</kbd> | Select or deselect in a multi-select list |
| <kbd>Enter</kbd> | Press the focused button |
| <kbd>Esc</kbd> | Close without applying |
| <kbd>?</kbd> | Help |

## Templates

A [template](templates.md) saved after a pivot or melt stores the reshape. When
it is applied, the order is query, filters, sort, pivot or melt, column order.
