# Find in the table

Press <kbd>f</kbd>, type what to find, press <kbd>Enter</kbd>. The cursor goes
to the first cell at or after it that holds the text, and that cell is
highlighted. The view is searched as it stands — query, filters, sort, the
columns shown — and is never changed.

| Key | Action |
|---|---|
| <kbd>f</kbd> | Find. The prompt holds the last pattern, selected, so typing replaces it |
| <kbd>n</kbd> | Next match, cell by cell, left to right then down |
| <kbd>N</kbd> | Previous match |
| <kbd>Esc</kbd> | While a find reads, stop it; the cursor stays where it was |

Past the last match <kbd>n</kbd> comes round to the first, and the bar says
`Wrapped to the top`; <kbd>N</kbd> comes round the other way.

## In the prompt

| Key | Action |
|---|---|
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | Regex on or off |
| <kbd>Ctrl</kbd>+<kbd>L</kbd> | Only the current column, or every column shown. The current column is the found cell's, or the first one on screen |
| <kbd>↑</kbd> <kbd>↓</kbd> | Earlier patterns |
| <kbd>Enter</kbd> | Find |
| <kbd>Esc</kbd> | Cancel |

Case is ignored until the pattern has a capital letter: `oslo` finds `Oslo`,
`Oslo` does not find `oslo`. In a regex, an escape such as `\S` is not a
capital. Each value is matched as text, so `05-17` finds a date and `2.5` a
number; binary and nested columns are skipped.

## The bar

While a find is in effect the bar shows its pattern: `find "chicken"`, or
`find /^ch/` for a regex, with `in name` when it is limited to a column.
`match 3` follows when the finds so far walked from the top of the view to
the cursor. Nothing counts every match: a find reads only as far as the next
one.

## Large data

A find starts in the rows already read. Past them it reads the view on, a
window at a time, and the bar shows the rows read; <kbd>Esc</kbd> or
<kbd>Ctrl</kbd>+<kbd>O</kbd> stops it. The order of a sorted view, a SQL
result included, is the order on screen, so <kbd>n</kbd> never skips or
repeats a row.

[Search](querying-data.md#search) in the query prompt filters rows instead.
