# Find in the table

<kbd>/</kbd> (or <kbd>f</kbd>) finds a cell that matches text, a regex or
letters in order, and moves the cursor to it without changing the rows.
<kbd>Ctrl</kbd>+<kbd>G</kbd> keeps only the rows that match.

Type what to find. As you type, matching cells in the rows already loaded
light up, and the prompt says how many are on screen (`3 on screen`); this
reads no more data. Press <kbd>Enter</kbd> and the cursor, row and column, goes
to the first matching cell at or after its row. The cell stays highlighted
until the cursor leaves it. Find reads the view as it stands (query, filters,
sort, the columns shown) and never changes it.

| Key | Action |
|---|---|
| <kbd>/</kbd> or <kbd>f</kbd> | Find. The prompt holds the last pattern, selected, so typing replaces it |
| <kbd>n</kbd> | Next match after the cursor's cell, left to right then down |
| <kbd>N</kbd> | Previous match before the cursor's cell |
| <kbd>Esc</kbd> | While a find reads, stop it and the <kbd>n</kbd> <kbd>N</kbd> typed behind it; the cursor stays where it was. Otherwise clear the find |

<kbd>n</kbd> and <kbd>N</kbd> start from the cursor's cell, as in vim: move
the column cursor along a found row and <kbd>n</kbd> finds the next match to
its right. After the last match <kbd>n</kbd> wraps to the first, and the
footer says `Wrapped to the top`; <kbd>N</kbd> wraps the other way. Each
<kbd>n</kbd> typed while a find is reading runs in turn, so five presses move
five matches. <kbd>Esc</kbd> stops them all.

## In the prompt

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Go to the first match at or after the cursor's row; on an empty field, clear the find |
| <kbd>Ctrl</kbd>+<kbd>G</kbd> | Keep only the rows that match, as a filter |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | Regex on or off |
| <kbd>Ctrl</kbd>+<kbd>T</kbd> | Letters in order on or off |
| <kbd>Ctrl</kbd>+<kbd>L</kbd> | Only the [column cursor](filtering-sorting.md#move-across-a-wide-table)'s column, or every column shown |
| <kbd>↑</kbd> <kbd>↓</kbd> | Earlier patterns |
| <kbd>Esc</kbd> | Cancel |

Case is ignored unless the pattern has a capital letter: `oslo` finds `Oslo`,
`Oslo` does not find `oslo`. In a regex, an escape such as `\S` is not a
capital. Each value is matched as text, so `05-17` finds a date and `2.5` a
number; binary and nested columns are skipped.

| Match | Pattern | Finds |
|---|---|---|
| Text | `chicken` | `Crispy Chicken Sandwich` |
| Regex (<kbd>Ctrl</kbd>+<kbd>R</kbd>) | `^Chick` | `Chick-n-Strips`, not `Crispy Chicken` |
| Letters in order (<kbd>Ctrl</kbd>+<kbd>T</kbd>) | `chkn` | `Chick-n-Strips`, `Crispy Chicken`: the letters need not be adjacent; spaces in the pattern are ignored |

## Keep the matches

<kbd>Ctrl</kbd>+<kbd>G</kbd> in the prompt keeps only the rows with a match, as
a filter. The footer shows it (`has "chicken"`, `has /^Chick/`,
`name has letters "chkn"` when limited to a column), and so does the
[Sort & Filter](filtering-sorting.md) sidebar, where you remove it like any
other filter; <kbd>R</kbd> resets the view. The find stays in effect, so
<kbd>n</kbd> steps through the matches in the rows kept.

## The footer

While a find is in effect the footer offers <kbd>n</kbd>/<kbd>N</kbd> and
<kbd>Esc</kbd>, and its status line shows the pattern: `find "chicken"`, or
`find /^ch/` for a regex, `find ~chkn` for letters in order, with `in name` when
it is limited to a column. `match 3` follows when the finds so far stepped from
the top of the view to the cursor, so the match's number is known. Nothing
counts all the matches: a find reads only as far as the next one.

## Large data

A find starts in the rows already read. Past them, it reads on through the
view a window at a time, and the footer counts the rows read on a line of its
own (`rows 1,200,000 / 3,475,226`, with a bar when the view's rows are
counted). <kbd>Esc</kbd> or <kbd>Ctrl</kbd>+<kbd>O</kbd> stops it. A filtered
view, a CSV or an NDJSON file cannot skip to a window, so there <kbd>N</kbd>
reads the rows before the cursor in one pass, and <kbd>n</kbd> from deep in the
view reads windows at least as large as the rows above them. A find follows
the order on screen, even in a sorted view or a SQL result, so <kbd>n</kbd>
never skips or repeats a row.
