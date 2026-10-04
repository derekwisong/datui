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

Only the table's own keys act there: a letter with <kbd>Ctrl</kbd> or
<kbd>Alt</kbd> held does nothing, beyond the paging keys above.

## Go to a row

<kbd>:</kbd> opens a prompt for a row number; <kbd>Enter</kbd> goes there, and
`0` is the top. Only digits are read: <kbd>Enter</kbd> on anything else closes
the prompt without moving. The prompt keeps no history.

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
| Click a chip in the control bar | Presses its key |

<kbd>Shift</kbd>+drag selects text in most terminals. `mouse = false` under
`[display]`, or `--mouse=false`, leaves the mouse to the terminal:
[Mouse and text selection](configuration.md#mouse-and-text-selection).

## Keys typed while datui works

While a load, query or other read runs, the control bar shows a spinner, and
keys typed meanwhile are held and replayed in order once the work is done.

| Key | While busy |
|---|---|
| <kbd>Ctrl</kbd>+<kbd>Q</kbd>, <kbd>Ctrl</kbd>+<kbd>C</kbd> | Quit at once |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | Goes home at once, abandoning a load |
| At the plain table: <kbd>q</kbd> <kbd>Q</kbd>, the column cursor (<kbd>←</kbd> <kbd>→</kbd>, <kbd>h</kbd> <kbd>l</kbd>, <kbd>[</kbd> <kbd>]</kbd>, <kbd>{</kbd> <kbd>}</kbd>), <kbd>#</kbd>, <kbd>,</kbd>, <kbd>D</kbd>, the width keys (<kbd>&lt;</kbd> <kbd>&gt;</kbd> <kbd>=</kbd> <kbd>w</kbd>), <kbd>?</kbd> and <kbd>F1</kbd> | Act at once |
| <kbd>↑</kbd> <kbd>↓</kbd> (<kbd>j</kbd> <kbd>k</kbd>) | Act at once inside the rows already read, while all that is awaited is more rows |
| A bare <kbd>Esc</kbd>, or an <kbd>Enter</kbd> that would drill | Dropped |
| An <kbd>Enter</kbd> that would inspect | Held as <kbd>Space</kbd> |
| <kbd>Esc</kbd> while a view is applied | Stops it; the table stays as it was |
| <kbd>Esc</kbd> while a find reads | Stops it and the <kbd>n</kbd> <kbd>N</kbd> typed behind it; the cursor stays put |
| Anything else | Held |

At most 32 keys are held, and held keys are dropped with the screen they were
typed at. At the loading screen nothing is held: the keys above act, the rest
are dropped. The mouse is never held: the wheel across and the busy bar's
chips act as their keys do, and the wheel down and a click on the table are
dropped.
