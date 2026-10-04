# Find in the table

<kbd>/</kbd> (or <kbd>f</kbd>) moves the cursor to a cell holding text, a regex
or letters in order, without changing the rows; <kbd>Ctrl</kbd>+<kbd>G</kbd>
keeps only the rows that match.

Type what to find. As you type, the matching cells among the rows on hand light
up, and the prompt says how many are on screen (`3 on screen`); nothing is read
for that. Press <kbd>Enter</kbd>: the cursor, column cursor and all, goes to the
first matching cell at or after its row, highlighted until the cursor leaves it.
Find reads the view as it stands (query, filters, sort, the columns shown) and
never changes it.

| Key | Action |
|---|---|
| <kbd>/</kbd> or <kbd>f</kbd> | Find. The prompt holds the last pattern, selected, so typing replaces it |
| <kbd>n</kbd> | Next match after the cursor's cell, left to right then down |
| <kbd>N</kbd> | Previous match before the cursor's cell |
| <kbd>Esc</kbd> | While a find reads, stop it and the <kbd>n</kbd> <kbd>N</kbd> typed behind it; the cursor stays where it was. Otherwise clear the find |

<kbd>n</kbd> and <kbd>N</kbd> start from the cursor's cell, as in vim: move
the column cursor along a found row and <kbd>n</kbd> finds the next match to
its right. Past the last match <kbd>n</kbd> comes round to the first, and the
footer says `Wrapped to the top`; <kbd>N</kbd> comes round the other way. Each
<kbd>n</kbd> typed while a find reads runs in turn: five move five matches.
<kbd>Esc</kbd> stops them all.

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

Case is ignored until the pattern has a capital letter: `oslo` finds `Oslo`,
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
a filter: the footer shows it (`has "chicken"`, `has /^Chick/`,
`name has letters "chkn"` when limited to a column), and so does the
[Sort & Filter](filtering-sorting.md) sidebar, where it is removed like any
other filter; <kbd>R</kbd> resets the view. The find stays in effect, so
<kbd>n</kbd> walks the matches among the rows kept.

## The footer

While a find is in effect the footer offers <kbd>n</kbd>/<kbd>N</kbd> and
<kbd>Esc</kbd>, and its status line shows the pattern: `find "chicken"`, or
`find /^ch/` for a regex, `find ~chkn` for letters in order, with `in name` when
it is limited to a column. `match 3` follows when the finds so far walked from
the top of the view to the cursor. Nothing counts every match: a find reads only
as far as the next one.

## Large data

A find starts in the rows already read. Past them it reads the view on, a
window at a time, and the footer counts the rows read on a line of its own
(`rows 1,200,000 / 3,475,226`, with a bar when the view's rows are counted);
<kbd>Esc</kbd> or <kbd>Ctrl</kbd>+<kbd>O</kbd> stops it. A filtered view, a CSV
or NDJSON cannot skip to a window, so <kbd>N</kbd> there reads the rows before
the cursor in one pass, and <kbd>n</kbd> from deep in the view reads windows no
smaller than the rows above them. The order of a sorted view, a SQL result
included, is the order on screen, so <kbd>n</kbd> never skips or repeats a row.
