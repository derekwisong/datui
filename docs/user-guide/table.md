# The table

A dataset opens in the table. <kbd>?</kbd> lists its keys;
[Keyboard shortcuts](../reference/keyboard-shortcuts.md) lists every screen's.

| Key | Moves |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | The row cursor |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | The [column cursor](filtering-sorting.md#move-across-a-wide-table) |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> (<kbd>Ctrl</kbd>+<kbd>B</kbd> <kbd>Ctrl</kbd>+<kbd>F</kbd>) | A page |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> <kbd>Ctrl</kbd>+<kbd>D</kbd> | Half a page |
| <kbd>Home</kbd> <kbd>End</kbd> (<kbd>G</kbd>) | The first or last row |
| <kbd>:</kbd> | A row by number |
| <kbd>[</kbd> <kbd>]</kbd> | Sort by the cursor's column, ascending or descending |
| <kbd>/</kbd> | [Find](finding.md) |

Only the table's own keys act there: a letter with <kbd>Ctrl</kbd> or
<kbd>Alt</kbd> held does nothing, beyond the paging keys above.

## The footer

Under a thin rule at the bottom of every screen, one line says where you are and
what is in effect, in pipeline order, with the cursor's place at the right:

```text
 weather/daily.parquet › query › prcp > 0 · date ▼        41,208 / 1,204,331  ? keys
```

| Part | Says |
|---|---|
| `weather/daily.parquet` | The dataset: its file and the directory it is in |
| `query` | A query is in effect; <kbd>:</kbd> shows its text |
| `prcp > 0 · date ▼` | The filters, then the sort; `2 filters · sorted` where the line is short |
| `41,208 / 1,204,331` | The cursor's row of the view's rows, led by `col 3/40` when the table is wider than the screen |
| `? keys` | Help: every key of the screen |

At rest only `? keys` is offered. A mode shows its two or three keys while it
is active: after the column cursor moves, `+/- Filter  [/] Sort  F Counts`; with
a find in effect, `n/N Next  Esc Clear`; while following a file, `t Pause`; on a
`by` view, `Enter Drill`. A message, such as `Copied 3 rows`, takes the room of
the dataset's name for a moment and never adds a line. On a narrow terminal the
line gives up, in order: the dataset's name (cut in the middle first), the
filters and sort (counted), background work (its spinner alone), the query, and
the position (`41,208 / 1.2M`). The mode's keys and help stay.

The footer grows, up to three lines, only for something ongoing: a prompt being
typed ([find](finding.md), the command line) or a job with a count, such as a
find reading past the rows on hand (`rows 1,200,000 / 3,475,226` with a bar and
`Esc Stop`). It takes the rows from the bottom of the table; the top stays put.

## Go to a row

<kbd>:</kbd> opens the [command line](querying-data.md); digits alone make it
`row:`, and <kbd>Enter</kbd> goes there (`0` is the top).

## Sort by a column

<kbd>[</kbd> sorts by the cursor's column ascending, <kbd>]</kbd> descending,
replacing the sort in effect; the same key again on that column takes the sort
away. The header carries `▲` or `▼`, and the footer names the sort. The
[Sort & Filter](filtering-sorting.md) sidebar adds secondary sorts.

## Empty cells and marked columns

An empty cell shows which kind of empty it is: `∅` (ASCII `~`) for a null,
`·` (ASCII `.`) for a column the row's file does not have, `≠` (ASCII `!`)
for a column its file holds in another type. A column name marked `*` is not
in every file, or the files disagree on its type; the Info panel's Schema tab
says where the schema came from. [Files that disagree](open-files.md#files-that-disagree)
has the details.

Long values, control characters and column widths:
[Column widths](filtering-sorting.md#column-widths) and
[In the table](inspecting-rows.md#in-the-table).

## The mouse

| Mouse | Does |
|---|---|
| Click | Puts the cursor on the cell; on a header, the column cursor on its column |
| Double-click | <kbd>Enter</kbd> on the row |
| Wheel | <kbd>↑</kbd> <kbd>↓</kbd>, three rows a notch; the same in help, the inspector and the sidebars |
| <kbd>Shift</kbd>+wheel, or a sideways wheel | <kbd>←</kbd> <kbd>→</kbd>: the column cursor |
| Drag a header onto another column | Moves the column there, as <kbd>H</kbd> <kbd>L</kbd> do |
| Drag the gap right of a header | Sets the column's width, as <kbd>&lt;</kbd> <kbd>&gt;</kbd> do |
| Right-click a cell | A menu of the cell's keys: filter, counts, sort, copy, inspect |
| Click a key in the footer | Presses it; the filters press <kbd>s</kbd>, `query` <kbd>:</kbd> |

Dialogs, tabs and the cell's menu: [The mouse](mouse.md).
<kbd>Shift</kbd>+drag selects text in most terminals. `mouse = false` under
`[display]`, or `--mouse=false`, leaves the mouse to the terminal:
[Mouse and text selection](configuration.md#mouse-and-text-selection).

## Keys typed while datui works

While a load, query or other read runs, the footer shows a spinner, and
keys typed meanwhile are held and replayed in order once the work is done.

| Key | While busy |
|---|---|
| <kbd>Ctrl</kbd>+<kbd>Q</kbd>, <kbd>Ctrl</kbd>+<kbd>C</kbd> | Quit at once |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | Goes home at once, abandoning a load |
| At the plain table: <kbd>q</kbd> <kbd>Q</kbd>, the column cursor (<kbd>←</kbd> <kbd>→</kbd>, <kbd>h</kbd> <kbd>l</kbd>, <kbd>Shift</kbd>+<kbd>←</kbd> <kbd>→</kbd>, <kbd>{</kbd> <kbd>}</kbd>), <kbd>#</kbd>, <kbd>,</kbd>, <kbd>D</kbd>, the width keys (<kbd>&lt;</kbd> <kbd>&gt;</kbd> <kbd>=</kbd> <kbd>w</kbd>), <kbd>?</kbd> and <kbd>F1</kbd> | Act at once |
| <kbd>↑</kbd> <kbd>↓</kbd> (<kbd>j</kbd> <kbd>k</kbd>) | Act at once inside the rows already read, while all that is awaited is more rows |
| A bare <kbd>Esc</kbd>, or an <kbd>Enter</kbd> that would drill | Dropped |
| An <kbd>Enter</kbd> that would inspect | Held as <kbd>Space</kbd> |
| <kbd>Esc</kbd> while a view is applied | Stops it; the table stays as it was |
| <kbd>Esc</kbd> while a find reads | Stops it and the <kbd>n</kbd> <kbd>N</kbd> typed behind it; the cursor stays put |
| Anything else | Held |

At most 32 keys are held, and held keys are dropped with the screen they were
typed at. At the loading screen nothing is held: the keys above act, the rest
are dropped. The mouse is never held: the wheel across, the footer's keys
and a width drag act as their keys do, and the wheel down, a click on the
table, a dropped header and the cell's menu are dropped.
