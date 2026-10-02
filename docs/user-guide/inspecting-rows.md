# Inspect a row

Press <kbd>Space</kbd> at the table to see every field of the current row,
and the focused field's whole value. <kbd>Esc</kbd> or <kbd>Space</kbd>
closes it.

<kbd>Enter</kbd> opens it too, except on a row of a `by` query or a SQL
`GROUP BY`, where <kbd>Enter</kbd>
[drills into the group](querying-data.md#drill-into-a-group-by) and <kbd>Space</kbd> inspects.
The bottom bar's first chip names the key: `Enter Inspect` or `Space Inspect`.

The title counts the rows: `Row 3 of 60`. Inside a drill-down it counts the
group's rows and names the group, `Row 3 of 20 · region=north`, since the
inspector covers the breadcrumb.

When every field fits above a few lines of value, the list shows them all;
a longer list takes half the height and scrolls. The footer offers only the
keys that act on the focused field: <kbd>e</kbd> on text and bytes,
<kbd>PgUp</kbd> <kbd>PgDn</kbd> when the value runs past the pane,
<kbd>Home</kbd> <kbd>End</kbd> when the list does.

The table cuts long text at the edge of its column and rounds floats to its
preview. The inspector shows what is stored:

| Value | Shown as |
|---|---|
| Float | The shortest decimal that reads back to the stored value: `1000000.125`, not the table's `1.0000e6`. `-0.0`, `NaN`, `inf` and `-inf` as they are |
| Integer | Every digit. When the table groups digits, a line under it says what the table shows |
| Datetime | Every digit of its unit and the zone's offset: `2024-01-02 04:04:05.000120 +01:00`. One past the calendar's range shows its stored number: `-9223372036854775807 us since 1970-01-01 UTC` |
| Text | Whole, wrapped, a line per line break; its length, line count and any spaces at either end are named on the rule above it |
| Empty text | `""` in the list; `empty string` in the pane, or `""` escaped |
| Empty bytes | `0 bytes · empty` in the list; `empty binary` in the pane |
| Null | `∅ null`; `· absent` or `≠ conflicting` where the dataset's files differ |
| List, struct | One item per line, text quoted |
| Binary | A hex dump, or `b"..."` escaped. The table's type row says `binary` |

Exact means the value as Polars stored it, not the spelling in a CSV file: a
`1.50` read from CSV is the float `1.5`.

## Keys

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move between fields |
| <kbd>Home</kbd> <kbd>End</kbd> | First and last field |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Previous and next row; the table's cursor moves with it |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Scroll a long value |
| <kbd>Enter</kbd> | Show more of a long value (16 KB of text at a time), or read a field the table's rows do not hold. The last line counts the whole value: `… 65,521 more lines` for a 1 MiB hex dump, `… 205 more lines, then 2,079,786 chars` for long text |
| <kbd>y</kbd> | Copy the focused field's exact value |
| <kbd>e</kbd> | Show text or bytes escaped (`\n`, `\t`, `\\`, quotes) or as itself |
| <kbd>/</kbd> | Find a field by name: type to narrow; <kbd>Enter</kbd> keeps the list narrowed, <kbd>Esc</kbd> clears it |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |
| <kbd>Esc</kbd> <kbd>Space</kbd> | Close; <kbd>Esc</kbd> clears a find first |

Escaped text tells a line break (`\n`) from a backslash followed by `n`
(`\\n`), and shows invisible characters such as a no-break space as `\u{a0}`.

## Copying a field

<kbd>y</kbd> copies the whole value through the same clipboard as the
[copy dialog](copying.md), whatever the pane shows of it: numbers exact, lists
and structs as JSON, binary as base64, a null as nothing. A value over an
`osc52` clipboard's `osc52_limit_kb` is refused before it is formatted.

## Hidden and binary columns

The table reads only the columns it shows, and reads a binary column as a
`‹binary›` placeholder. In the inspector, columns hidden in
[Sort & Filter](filtering-sorting.md) are listed after the others with a `⊘`
mark, and they and binary columns read `not read`. <kbd>Enter</kbd> on one reads
those fields for this row only, in the background. The read also checks the
row's other fields: if a query's sort with ties put another row there on
reading again, the pane says so instead of showing that row's values. A sort
from Sort & Filter or a SQL `ORDER BY` keeps tied rows in order, and a SQL
result comes back in one order, so a second read finds the same row.

## In the table

The table keeps a row on one line. A line break in a value shows as `¶`, a
tab as `»` and another control character or a direction mark (such as
U+202E, which would turn the rest of the row around) as `¤` (`$`, `>` and `?`
in an ASCII terminal), so `line1\nline2` reads `line1¶line2` instead of
`line1line2`.

A date or datetime past the calendar's range, such as a sentinel of
`i64::MIN + 1` microseconds, shows its stored number:
`-9223372036854775807 us since 1970-01-01 UTC`, `2147483647 days since
1970-01-01`. So do Describe, Data Quality and copies.
