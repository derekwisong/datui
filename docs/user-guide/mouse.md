# The mouse

The mouse is a shortcut for the keys. Each click or drag does what a key does,
so everything works without a mouse.

## At the table

| Mouse | Does |
|---|---|
| Click a cell | Puts the cursor on it |
| Click a header | Puts the column cursor on its column |
| Double-click a cell | <kbd>Enter</kbd> on the row |
| Double-click a header | Sorts by its column, as <kbd>[</kbd> <kbd>]</kbd> do: ascending, then descending, then off. The header carries the direction |
| Double-click the gap right of a header | Fits the column to the rows on screen and moves the column cursor there, as <kbd>=</kbd> does; it never sorts |
| Drag a header onto another column | Moves the column there, as <kbd>H</kbd> <kbd>L</kbd> do; a rule on the header marks where it lands. Let go outside the columns, or press a key, and nothing moves |
| Drag the gap right of a header | Sets the column's width by hand, as <kbd>&lt;</kbd> <kbd>&gt;</kbd> do, from 4 to 240 cells. For a column cut off at the right edge, drag from its last header cell |
| Right-click a cell | The cursor goes there and the cell's menu opens |
| Wheel | <kbd>↑</kbd> <kbd>↓</kbd>, three rows a notch |
| <kbd>Shift</kbd>+wheel, or a sideways wheel | <kbd>←</kbd> <kbd>→</kbd>: the column cursor |

## The cell's menu

```text
╭───────────────────────────────╮
│ ▎+      Filter to this value  │
│  -      Filter out this value │
│  F      Value counts          │
│  [      Sort ascending        │
│  ]      Sort descending       │
│  y      Copy                  │
│  Space  Inspect row           │
╰───────────────────────────────╯
```

Each line names its key, and running it presses that key on the cell.

| Key or mouse | Does |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> (<kbd>k</kbd> <kbd>j</kbd>) | Move between the lines |
| <kbd>Enter</kbd>, or a click on a line | Close the menu and press the line's key |
| <kbd>Esc</kbd>, or a click elsewhere | Close the menu |
| Any other key | Close the menu, then act as typed |

## In dialogs and sidebars

| Mouse | Does |
|---|---|
| Click a field | Focuses it and acts as <kbd>Space</kbd>: a checkbox flips, a choice steps, a picker opens, a button runs; a text field takes the cursor |
| Click a row of a list (a sort, a filter, a column in Sort & Filter) | Focuses it; a second click acts as <kbd>Space</kbd> |
| Right-click a choice | Steps it back, as <kbd>←</kbd> does; on any other field, only focuses it |
| Click a line of an open picker | Chooses it (flips it, in a list of checkboxes) |
| Click a value in a list beside its field (export formats, compression) | Chooses it |
| Click a tab | Switches to it |
| Wheel over an open picker | Moves through its lines |
| Click a key in a dialog's footer (`Esc Cancel`) | Presses it; over help, an error or a question, only its footer's keys take clicks |
| Click outside a dialog | Nothing |

## In the footer

| Click | Does |
|---|---|
| A key | Presses it |
| `? keys` | Opens the help |
| The filters and sort | <kbd>s</kbd>: the Sort & Filter sidebar |
| `query` | <kbd>:</kbd>: the command line, on the query's text |

## While datui works

The mouse is never held for later. While datui is busy, a click acts if its
key would act at once, and is dropped if its key would wait
([Keys typed while datui works](table.md#keys-typed-while-datui-works)).
So a width drag works at a busy table, as <kbd>&lt;</kbd> <kbd>&gt;</kbd> do,
but a dropped header, a menu line or a click on a field is dropped.

## Selecting text

<kbd>Shift</kbd>+drag selects text in most terminals. To leave the mouse to
the terminal, set `mouse = false` under `[display]`, or run with
`--mouse=false`: [Mouse and text selection](configuration.md#mouse-and-text-selection).
