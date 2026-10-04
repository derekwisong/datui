# Sort, filter and arrange columns

<kbd>s</kbd> opens the **Sort & Filter** sidebar, where you sort, filter,
hide, move, freeze and size columns.

It has two tabs, **Columns** and **Filters**; <kbd>←</kbd> <kbd>→</kbd> switch
them, and <kbd>Tab</kbd> moves between the tab bar and the body.

## Filter, sort and hide columns

Open **Food nutrition (fast food)** from **Public datasets**: 515 menu items
from eight chains, nutrients per item. Find the chicken dishes with at least
40 g of protein, heaviest first:

1. Press <kbd>/</kbd>, then <kbd>Ctrl</kbd>+<kbd>T</kbd> for **Text**. Type
   `chicken` and press <kbd>Enter</kbd>: 178 of 515.
2. Press <kbd>s</kbd>, <kbd>→</kbd> for **Filters**, <kbd>Tab</kbd> into the
   list and <kbd>Enter</kbd> on `add filter…`. Type `protein` and press
   <kbd>Enter</kbd>, `>=` and <kbd>Enter</kbd>, `40` and <kbd>Enter</kbd>.
3. Press <kbd>Shift</kbd>+<kbd>Tab</kbd>, <kbd>←</kbd> for **Columns** and
   <kbd>Tab</kbd> into the find field. Type `calories`, press <kbd>↓</kbd> to
   reach the row, then <kbd>Space</kbd> twice for descending.
4. Press <kbd>Shift</kbd>+<kbd>Tab</kbd> back to the find field and
   <kbd>Ctrl</kbd>+<kbd>U</kbd> to clear it. Type `vit`, then <kbd>↓</kbd>
   <kbd>v</kbd> <kbd>↓</kbd> <kbd>v</kbd> to hide `vit_a` and `vit_c`.
5. Press <kbd>Enter</kbd> to apply.

The table shows 30 of 515, led by McDonald's 20 piece Buttermilk Crispy
Chicken Tenders at 2,430 calories. The `calories` header carries `▼`.

## Filter on a cell

At the table, <kbd>+</kbd> and <kbd>-</kbd> filter on the cell under the
cursor, where the row cursor and the [column cursor](#move-across-a-wide-table)
cross.

| Key | Does |
|---|---|
| <kbd>+</kbd> | Keep the rows whose value in this column is the cell's; on a null cell, keep the nulls |
| <kbd>-</kbd> | Drop those rows; on a null cell, drop the nulls |

Each press adds a row to the [Filters tab](#filters-tab), joined to the others
with **and**, so you can edit or delete it there and <kbd>R</kbd> clears it. A
float matches the number as the table draws it: on a cell drawn `0.3`, `+`
keeps every row drawn `0.3`, `0.1 + 0.2` among them. A list, struct or binary
cell has no value to compare; the bar says so.

## Apply or cancel changes

| Key | Does |
|---|---|
| <kbd>Enter</kbd> | Apply everything staged and close (on the Filters tab it adds or edits; <kbd>a</kbd> applies there, outside the row editor) |
| <kbd>Ctrl</kbd>+<kbd>Enter</kbd> or <kbd>Ctrl</kbd>+<kbd>J</kbd> | Apply from anywhere, including mid-edit — the row in progress is saved. <kbd>Ctrl</kbd>+<kbd>Enter</kbd> needs a terminal that tells it from <kbd>Enter</kbd>; <kbd>Ctrl</kbd>+<kbd>J</kbd> works on every terminal |
| <kbd>Esc</kbd> | Close without changing anything |
| <kbd>C</kbd> | Clear the current tab's staged state (with the body focused) |

Sidebar filters and sort apply to the current query or reshape result. Running
a new query clears them, so apply the query first and the sidebar settings
afterward. The bottom bar shows matching and total row counts, such as `30 of 515`.
Large totals are abbreviated; **Info** shows the exact total.
Canceling the sidebar discards whatever was staged; reopening it shows what
is actually applied.

## Columns tab

![Sorting columns in the sidebar](../demos/06-sorting.gif)

One row per column, with its lock, its place and direction in the sort
(`1▲`, `2▼`), a width set by hand, and a `⊘` when it is hidden. Each sorted column's header in the
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
| <kbd>v</kbd> | Hide or show this column (it keeps its place in the list, dimmed) |
| <kbd>&lt;</kbd> <kbd>&gt;</kbd> (<kbd>,</kbd> <kbd>.</kbd>) | Make this column 4 cells narrower or wider |
| <kbd>f</kbd> | Fit this column to the rows on screen; the list shows `fit` until you apply |
| <kbd>w</kbd> | Back to the automatic width |

### Column widths

Each column keeps the width it was first drawn at, so paging, scrolling,
reordering, hiding and opening a sidebar move nothing. A longer value on a
later page ends in `…` (ASCII `...`).

| Column | Automatic width |
|---|---|
| Text, lists, structs and names | The first page's widest, at most two fifths of the window: 32 cells at 80 columns, 48 at 120 (16 to 64) |
| Numbers, dates, times, flags | The widest value seen so far, never cut; paging does not narrow it |

A query, a pivot or melt, drilling down or back up, a new sort, <kbd>r</kbd>
and a new filter change the rows, so automatic widths are learned again from
the first page they show.

At the table, the [column cursor](#move-across-a-wide-table)'s column takes
<kbd>&lt;</kbd> <kbd>&gt;</kbd> (4 cells narrower or wider), <kbd>=</kbd> (fit
to the rows on screen) and <kbd>w</kbd> (automatic) at once, so you see each
change as you make it. The Sort tab stages the same keys for any column until
<kbd>Enter</kbd>.

A width set with <kbd>&lt;</kbd> <kbd>&gt;</kbd>, <kbd>=</kbd> or <kbd>f</kbd> is kept through
paging, resizing and reordering until <kbd>w</kbd>, <kbd>C</kbd> or
<kbd>R</kbd>. Text gets exactly that width; a number column is never narrower
than its numbers. Applying only width changes leaves the table on the page
you were on.

The space between columns is the `display.cell_padding` setting:
`"comfortable"` (2 cells, the default), `"compact"` (1) or a number. See the
[settings reference](../reference/settings.md#display).

### Frozen columns

Frozen columns stay at the left edge, left of a `│`, while the rest scroll.
When the window is too narrow for all of them beside a usable scrolling
column, the separator turns dashed (`┆`, ASCII `:`) and the frozen columns
that do not fit scroll after it, so every column stays reachable. The freeze
is kept: a wider window shows them all frozen again. A value or name cut
short at the edge of its column ends in `…` (ASCII `...`).

Every column carries its own direction, so `calories` can run descending while
`restaurant` runs ascending. Nulls go last in either direction.

Back in the main view, <kbd>r</kbd> reverses every direction at once and
<kbd>R</kbd> resets everything: query, filters, sort, column order, hidden
columns and widths, frozen columns, pivot/melt, drill-down and the applied view.

## Move across a wide table

The table has a column cursor as well as a row cursor: the cursor's column is
tinted from header to last row, and the cell where it crosses the current row
stands out from both. On a 16-color terminal, or with `NO_COLOR`, its header
and that cell are drawn reversed.

| Key | Moves |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | The cursor one column. The columns scroll only when it would leave the screen |
| <kbd>[</kbd> <kbd>]</kbd> or <kbd>Shift</kbd>+<kbd>←</kbd> <kbd>→</kbd> | A page of columns; the cursor goes to its first column |
| <kbd>{</kbd> <kbd>}</kbd> | The cursor to the first column, or to the last on the last page |
| <kbd>g</kbd> | The cursor to a column you name: type to narrow the list, <kbd>Enter</kbd> goes |

- <kbd>]</kbd> starts the next page at the first column not shown whole, so
  a column cut at the edge is read whole there. A column wider than the
  window still moves one at a time.
- The last page is full: it ends with the last column.
- <kbd>[</kbd> right after <kbd>]</kbd> goes back to the page it left;
  otherwise it ends the page before with the column left of the first one
  shown.
- <kbd>g</kbd> leaves a column already whole on screen where it is; another
  becomes the first after the frozen ones, or lands on the last page.
- Frozen columns stay put, and the cursor walks them too: <kbd>h</kbd> from
  the first scrolling column goes to the last frozen one, and <kbd>l</kbd>
  back goes to the first scrolling column, scrolling back to it.
- At the last page, <kbd>]</kbd> takes the cursor to the last column; at the
  first, <kbd>[</kbd> takes it to the page's first column, then the first.
- The cursor stays on its column when columns are hidden, moved or frozen in
  the sidebar; when its own column is hidden, the column that takes its place
  takes the cursor.
- Hidden columns are not listed by <kbd>g</kbd>.

<kbd>H</kbd> and <kbd>L</kbd> move the cursor's column itself one place left
or right, the cursor with it: the same column order the Columns tab's
<kbd>+</kbd> <kbd>-</kbd> set, and <kbd>R</kbd> puts it back. A frozen column
moves among the frozen ones, and a scrolling column among the scrolling ones.

The keys that act on one column act on the cursor's:

| Key | On the cursor's column |
|---|---|
| <kbd>F</kbd> | [Value counts](value-counts.md) |
| <kbd>s</kbd> | The sidebar opens with its Columns cursor there, and a new filter starts on it |
| <kbd>y</kbd> | The Cell scope [copies](copying.md) its value in the current row |
| <kbd>Space</kbd> | The [inspector](inspecting-rows.md) opens on its field |
| <kbd>f</kbd> | <kbd>Ctrl</kbd>+<kbd>L</kbd> in the prompt [finds](finding.md) in it alone; a match moves the cursor to its column |

The bottom bar says where the cursor is: `col 43 of 300` (`col 43/300` on a
bar under 100 cells). It counts the columns the table shows, frozen first;
hidden columns are not counted.

## Filters tab

![Adding filters in the sidebar](../demos/07-filtering.gif)

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
| `is null` `not null` | the value is null, or is not; these take no value |

The value is parsed as the column's type, so `> 1000` on a number column is a
numeric comparison. On a float column, `=` and `!=` compare to the sixth
decimal place, where the table rounds a float, or to the last digit you write
past it: `= 0.3` holds `0.1 + 0.2`, which the table draws as `0.3`. In
exponent notation the last digit written counts: `= 1.2346e7` holds
12,345,800. A date, time, duration or decimal
column compares `=` and `!=` with its text, such as `2024-01-01`. Filters stay in place while you chart, analyze or export,
and are saved in [views](views.md).

## From the query prompt

For anything more involved, a SQL `WHERE` or the `where` clause of a
[q query](../reference/query-syntax.md#where-clause--and-) takes
expressions, `OR` groups and date arithmetic.
