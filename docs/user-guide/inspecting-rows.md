# Inspect a row

Press <kbd>Space</kbd> at the table to see every field of the current row,
and the focused field's whole value. It opens on the field of the column
cursor's column. <kbd>Esc</kbd> or <kbd>Space</kbd> closes it.

<kbd>Enter</kbd>, or a double-click on the row, opens it too, except on a row of a `by` query or a SQL
`GROUP BY`, where <kbd>Enter</kbd>
[drills down into the group](querying-data.md#drill-down-a-group-by) and <kbd>Space</kbd> inspects.
The bottom bar's first chip says what <kbd>Enter</kbd> does: `Enter Inspect`,
or `Enter Drill` where it drills. On a narrow terminal it yields to
`? Help`.

The title counts the rows: `Row 3 of 60`. Inside a drill-down it counts the
group's rows and names the group, `Row 3 of 20 · region=north`, since the
inspector covers the breadcrumb.

The list takes the rows its fields need and the value the rest; when they do
not all fit, the value keeps the lines it needs, up to half the height, and
the list scrolls, with `… 5 above` and `… 12 more` at its ends. At 140
columns and wider the fields and the value sit side by side at full height,
and the fields flow into as many columns as fit: a row with more fields than
the screen has rows narrows the value to make room for them. At 240 columns
and wider, a row with a binary field gives the value room for a hex dump of
32 bytes a line. The fields' rule counts
them, `14 · 2 null · 1 empty`, and the footer offers only the keys that act
now.

The table cuts long text at the edge of its column and rounds floats to its
preview. The inspector shows what is stored:

| Value | Shown as |
|---|---|
| Float | The shortest decimal that reads back to the stored value: `1000000.125`, not the table's `1.0000e6`. `-0.0`, `NaN`, `inf` and `-inf` as they are |
| Integer | Every digit. When the table groups digits, a line under it says what the table shows |
| Datetime | Every digit of its unit and the zone's offset: `2024-01-02 04:04:05.000120 +01:00`. One past the calendar's range shows its stored number: `-9223372036854775807 us since 1970-01-01 UTC` |
| Text | Whole, wrapped between words, a line per line break; its length, line count and any spaces at either end are named on the rule above it. Over 1 KB, the list's preview ends with its size: `… · 2.1 MB` |
| JSON text | Indented, with `json` on the rule. <kbd>e</kbd> shows it raw or escaped |
| Empty text | `""` in the list; `empty string` in the pane, or `""` escaped |
| Empty bytes | `0 bytes · empty` in the list; `empty binary` in the pane |
| Null | `∅ null`; `· absent` or `≠ conflicting` where the dataset's files differ |
| List, struct | One item per line, text quoted |
| Binary | Its size, `1.0 MB (1,048,576 bytes)`, and what the first bytes say it is: PNG, JPEG or GIF with its dimensions, PDF, gzip, zstd, zip, Parquet, Arrow. UTF-8 bytes read as text; others as a hex dump |

Exact means the value as Polars stored it, not the spelling in a CSV file: a
`1.50` read from CSV is the float `1.5`.

## Keys

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move between fields |
| <kbd>Home</kbd> <kbd>End</kbd> | First and last field |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A page of fields |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Previous and next row; the table's cursor moves with it |
| <kbd>Tab</kbd> | Move into the value ([below](#read-a-long-value)) |
| <kbd>Enter</kbd> | On a row of a `by` query or `GROUP BY`, drill down to its rows, as at the table. Else open a struct, a list or JSON text ([below](#drill-down-into-nested-values)), or read a field the table's rows do not hold |
| <kbd>r</kbd> | On a group's row, read a field the table's rows do not hold |
| <kbd>y</kbd> | Copy the focused value as its view shows it |
| <kbd>Y</kbd> | Copy the whole row as one JSON object |
| <kbd>e</kbd> | The value's next view, where it has more than one ([below](#views)) |
| <kbd>w</kbd> | Word wrap or hard wrap for long text |
| <kbd>o</kbd> | Open the value in another program ([below](#open-a-value-elsewhere)) |
| <kbd>f</kbd> | Only the fields with a value; while comparing, only the fields that differ |
| <kbd>s</kbd> | Order the fields: the table's order, A-Z, or filled first |
| <kbd>c</kbd> | Compare with the next row, or the pinned one ([below](#compare-two-rows)) |
| <kbd>m</kbd> | Pin this row for Compare; again on the same row to unpin |
| <kbd>/</kbd> | Find a field by name, then by value: type to narrow; <kbd>Enter</kbd> keeps the list narrowed, <kbd>Esc</kbd> clears it |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |
| <kbd>Esc</kbd> <kbd>Space</kbd> | Close; <kbd>Esc</kbd> clears a find first |

## Read a long value

<kbd>Tab</kbd> moves the focus, and the `▎` rail, into the value. The list
keeps a few rows around the focused field, and the value takes the rest.

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Scroll a line |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Scroll a page |
| <kbd>Home</kbd> <kbd>End</kbd> | The top and the end |
| <kbd>/</kbd> | Find in the value; <kbd>n</kbd> <kbd>N</kbd> go to the next and the last place. Lowercase text ignores case |
| <kbd>e</kbd> <kbd>w</kbd> <kbd>y</kbd> <kbd>o</kbd> | As in the fields |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Previous and next row |
| <kbd>Esc</kbd> <kbd>Tab</kbd> | Back to the fields; <kbd>Esc</kbd> clears a find first |

Only what is on screen is wrapped, so the end of a 2 MiB string or a 1 MiB
binary is as near as its start: <kbd>Tab</kbd> <kbd>End</kbd>. The rule says
where the pane is: `lines 41-73 of 4,000 · 1%` for text,
`0x0-0x1ff of 0x100000` for bytes.

Word wrap breaks between words and after `/ & ? , ; | -`, so a URL wraps at its
separators. A long run with no spaces, such as base64, fills each row instead.
<kbd>w</kbd> switches to hard wrap, at the pane's edge, for every value until
pressed again.

## Views

<kbd>e</kbd> cycles the views that apply to the value, and the rule names the
one shown. The footer offers <kbd>e</kbd> only where there is more than one.

| Value | Views |
|---|---|
| Text that parses as JSON (up to 1 MiB) | JSON (indented), Raw, Escaped |
| Other text | Raw, Escaped |
| UTF-8 bytes | Text, Hex, Escaped |
| gzip or zstd holding text | Hex, Text (the first 64 KB decompressed), Escaped |
| Other bytes | Hex, Escaped |

JSON text over 64 KB is indented in the background; until then it shows raw,
with `json, indenting...` on the rule. gzip and zstd are decompressed in the
background when their Text view is chosen, with `decompressing...` on the rule
until then; bytes that turn out to hold no text go back to Hex, and the rule
says `not text`. Escaped text tells a line break (`\n`)
from a backslash followed by `n` (`\\n`), and shows invisible characters such
as a no-break space as `\u{a0}`.

## Compare two rows

<kbd>c</kbd> adds a column with the next row's values, and `Δ` (`*` in an ASCII
terminal) marks each field whose values differ. The rule counts them,
`14 · 5 differ`, and the title names the other row: `Row 2 of 60 · compare
with 3`. At 240 columns and wider the row before is shown as well, in row
order (previous, this, next), each named over its column, and the title says
`compare with 1 and 3`; a field is marked when it differs from either.
<kbd>f</kbd> then lists only the fields that differ.

<kbd>m</kbd> pins the current row: Compare then shows it beside each row you
move to with <kbd>←</kbd> <kbd>→</kbd>, and the title says `compare with pinned 3`.
<kbd>m</kbd> on the pinned row lets it go.

## Drill down into nested values

<kbd>Enter</kbd> on a struct, a list or an array opens it one level down:
a struct's fields, or a list's items as `[0]`, `[1]`, … with their types and
previews. Text that holds a JSON object or array opens the same way, its keys
in the document's order. The footer says `Enter Open` where it applies.

| Key | Action |
|---|---|
| <kbd>Enter</kbd> or <kbd>→</kbd> <kbd>l</kbd> | Open the focused item |
| <kbd>Esc</kbd> or <kbd>←</kbd> <kbd>h</kbd> | Up a level; at the row, <kbd>Esc</kbd> closes |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd>, <kbd>Home</kbd> <kbd>End</kbd> | Move between items |
| <kbd>y</kbd> | Copy the focused item; a JSON object or array as indented JSON |
| <kbd>Space</kbd> | Close |

The title is the path from the row: `Row 42 of 60 › customer › address`.
On a narrow terminal the middle steps give way to `…`. A list of structs,
or a JSON array of objects, shows as a table with a column per field (the
first object's keys); `+3` after the header counts the columns that do not
fit, and the focused item's whole value is under the table.

| Limit | |
|---|---|
| Items | Only the items on screen are read: a list of a million items opens at once, and `End` reaches the last |
| JSON text | Up to 64 KB is parsed on the key; longer text in the background, with the spinner. Text over 4 MiB is not opened; <kbd>Tab</kbd> reads it in the value |
| Depth | JSON nested deeper than 128 levels does not open |

Text that does not parse stays where it is, and the bottom bar says why:
`Not JSON: key must be a string at line 1 column 2`. From then on
<kbd>Enter</kbd> leaves it as text, read in the value like any other.

## Copy a field or the row

<kbd>y</kbd> copies the focused value through the same clipboard as the
[copy dialog](copying.md), as its view shows it: numbers exact, lists and
structs as JSON, indented JSON in the JSON view, the text bytes hold in their
Text view, other bytes as base64 (the footer says `y Copy base64`), a null as
nothing.

<kbd>Y</kbd> copies the whole row as one JSON object, field names as keys and
values exact, without leaving the inspector. Hidden and binary fields are
included once read; otherwise the message counts them:
`Copied row 3: 12 fields, 2 not read`.

A copy over an `osc52` clipboard's `osc52_limit_kb` is refused.

## Open a value elsewhere

<kbd>o</kbd> writes the focused value, as its view shows it, to a read-only
temporary file named for the field, the row and its kind (`.json`, `.xml`,
`.txt`, `.png`, `.pdf`, `.bin`, ...), and opens it:

| Value | Opened with |
|---|---|
| Text, JSON, bytes with no known kind | `$VISUAL`, `$EDITOR` or `$PAGER`, else `less` (on Windows, the system's opener). It has the terminal until it exits, and the file is removed then |
| Images and PDFs | The system's opener: `xdg-open`, `open`, or Windows' `start`, which asks for a program when none is set. The file stays until datui quits |

A program named with arguments, such as `code --wait`, runs with them. Nothing
is read back into the dataset.

## Hidden and binary columns

The table reads only the columns it shows, and reads a binary column as a
`‹binary›` placeholder. In the inspector, columns hidden in
[Sort & Filter](filtering-sorting.md) are listed after the others with a `⊘`
mark, and they and binary columns read `not read`. <kbd>Enter</kbd> on one reads
those fields for this row, in the background. From then on, while the focus
stays on that field, each row moved to is read too, without holding the keys. The read also checks the
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
