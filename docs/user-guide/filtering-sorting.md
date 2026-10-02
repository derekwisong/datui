# Sort, filter and arrange columns

Press <kbd>s</kbd> to open the **Sort & Filter** sidebar. It has two tabs,
**Columns** and **Filters**; <kbd>←</kbd> <kbd>→</kbd> switch them, and
<kbd>Tab</kbd> moves between the tab bar and the body.

## Filter, sort and hide columns

Open **Food nutrition (fast food)** from **Public datasets**: 515 menu items
from eight chains, nutrients per item. Find the chicken dishes with at least
40 g of protein, heaviest first:

1. Press <kbd>/</kbd>, then <kbd>Ctrl</kbd>+<kbd>T</kbd> for **Search**. Type
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

![Sorting Demo](../demos/06-sorting.gif)

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

A width set with <kbd>&lt;</kbd> <kbd>&gt;</kbd> or <kbd>f</kbd> is kept through
paging, resizing and reordering until <kbd>w</kbd>, <kbd>C</kbd> or
<kbd>R</kbd>. Text gets exactly that width; a number column is never narrower
than its numbers. Applying only width changes leaves the table on the page
you were on.

The space between columns is the `table_cell_padding` setting:
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

| Key | Moves |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | One column |
| <kbd>[</kbd> <kbd>]</kbd> or <kbd>Shift</kbd>+<kbd>←</kbd> <kbd>→</kbd> | A page of columns |
| <kbd>{</kbd> <kbd>}</kbd> | To the first column, or to the last page |
| <kbd>g</kbd> | To a column you name: type to narrow the list, <kbd>Enter</kbd> goes |

- <kbd>]</kbd> starts the next page at the first column not shown whole, so
  a column cut at the edge is read whole there. A column wider than the
  window still moves one at a time.
- The last page is full: it ends with the last column.
- <kbd>[</kbd> right after <kbd>]</kbd> goes back to the page it left;
  otherwise it ends the page before with the column left of the first one
  shown.
- <kbd>g</kbd> leaves a column already whole on screen where it is; another
  becomes the first after the frozen ones, or lands on the last page.
- Frozen columns stay put; hidden ones are not listed by <kbd>g</kbd>.
- The current column is underlined in the header: the one <kbd>g</kbd> went
  to, until the columns scroll, and otherwise the first column past any
  frozen ones. [Value counts](value-counts.md) (<kbd>F</kbd>) count it.

While some columns are off screen, the bottom bar names the ones on it:
`cols 41-47 of 300` (`cols 41-47/300` on a bar under 100 cells). It counts
the columns the table shows, frozen first; hidden columns are not counted.

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

For anything more involved, a SQL `WHERE` or the `where` clause of a
[q-style query](../reference/query-syntax.md#where-clause--and-) takes
expressions, `OR` groups, null tests and date arithmetic.
