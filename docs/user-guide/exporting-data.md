# Export data

<kbd>e</kbd> writes the view to a file: every row and column as queried,
filtered and sorted.

## Export a CSV

1. Open **Premier League (2020-21)** from **Example datasets** and run the
   [goals query](querying-data.md#dates-and-messy-text).
2. Press <kbd>e</kbd> and type `goals.csv` in **Path**. The field starts
   with a suggested name: the dataset's name plus `-export`. Typing replaces
   it, and <kbd>Enter</kbd> accepts it as it is.
3. Press <kbd>Enter</kbd>. If the file exists, confirm whether to overwrite it.

The status line says `Exported to goals.csv`. A long path is shortened from
the start so the file name stays visible. The file holds a header and 380
matches, in the query's order:

```text
Round,match_date,home,away,goals
4,2020-10-04,Aston Villa,Liverpool,9
22,2021-02-02,Manchester Utd,Southampton,9
14,2020-12-20,Manchester Utd,Leeds United,8
```

`match_date` is written as a date because the query casts it. Without the
cast, a `STRPTIME` result is a datetime and exports as `2020-10-04T00:00:00.000000`.

The file contains all matching rows and displayed columns, not just the page
on screen. Numbers use their raw values, without display formatting.

A view with a [sample](sampling.md) exports only the sample's rows. To keep a
local sample of a remote table, press <kbd>S</kbd>, then <kbd>e</kbd>, and
export to `sample.parquet`. The export waits until the sample is drawn.

| Format | Extension | Options |
|---|---|---|
| CSV | `.csv` | Delimiter, header, compression |
| TSV | `.tsv` | Header, compression; the delimiter is a tab |
| PSV | `.psv` | Header, compression; the delimiter is a pipe |
| Parquet | `.parquet` | |
| JSON | `.json` | Compression; one array |
| NDJSON | `.jsonl`, `.ndjson` | Compression; one object per line |
| Arrow IPC | `.arrow`, `.ipc`, `.feather` | |
| Avro | `.avro` | |

Each export format also offers **Source file** for a dataset whose files disagree; see
[below](#source-file).

The dialog starts on the format datui read the data as, if datui can write
that format: a TSV file exports as TSV, and a Parquet part file with no
extension as Parquet. Otherwise it starts on the last format you picked.

Excel, ORC, NMEA, GPX, VCD, FIX, SDF and SQLite can be read but not written.

## Lists and structs

A `by` query, SQL `ARRAY_AGG` or a nested source file gives list, array or
struct columns. CSV, TSV and PSV have no such types, so they write each of
those cells as JSON text. The dialog says so when the view has such a column.

| Table shows | CSV cell |
|---|---|
| `[1, 2]` | `[1,2]` |
| `["a", "b"]` | `["a","b"]` |
| `{1,"a"}` | `{"x":1,"label":"a"}` |

A null list or struct is an empty field. NaN and infinity inside one are
written as `null`, since JSON has no other way to write them. Polars reads the text back with
`str.json_decode`.

Parquet, Arrow, JSON and NDJSON keep lists, arrays and structs as they are.

## Binary

CSV, TSV, PSV, JSON and NDJSON have no bytes type, so they write a binary value as
standard base64 text, inside lists and structs too: `hi` is written `aGk=`.
Polars reads it back with `str.decode("base64")`. Parquet, Arrow and Avro keep
the bytes.

## Durations

CSV has no duration type, so a CSV export writes a duration as ISO 8601 text
in seconds. JSON and NDJSON exports write the same text, and so does a duration
inside a list or struct. The text is exact in every unit, down to the
nanosecond.

| Table shows | CSV cell |
|---|---|
| `1h 2m 3s 4ms` | `PT3723.004S` |
| `-1s -500ms` | `-PT1.5S` |
| `1500ns` | `PT0.0000015S` |
| `0ms` | `P0D` |

Parquet and Arrow keep the duration type. Avro writes microseconds
([below](#avro-types)).

## Dates past the calendar

A date or datetime outside the calendar's range, such as a sentinel value of
`i64::MIN + 1` microseconds, cannot be written as a calendar date. CSV, JSON and NDJSON write
its stored number, as the table shows it:
`-9223372036854775807 us since 1970-01-01 UTC`. Parquet and Arrow keep the
value.

## Avro types

Avro keeps booleans, 32- and 64-bit integers and floats, strings, binary,
dates, millisecond and microsecond datetimes, lists and structs. Other
columns are converted, inside lists and structs too:

| Column | Avro |
|---|---|
| Array | List |
| Categorical, enum | String |
| 8- and 16-bit integer | 32-bit integer |
| 16-bit float | 32-bit float |
| Unsigned 32- and 64-bit integer | 64-bit integer; a value past its range fails the export |
| Decimal, 128-bit integer | String of the exact value, such as `327.68` |
| Nanosecond datetime | Microsecond datetime; digits past the microsecond are dropped |
| Datetime with a time zone | The same instant in UTC, without the zone |
| Time, duration | 64-bit integer of microseconds (a time counts from midnight) |
| Null | String |

An Avro name may contain only letters, digits and `_`, and may not start with
a digit. Avro export renames columns and struct fields that break these rules:
any other character becomes `_`, a leading digit gets a `_` in front, and a
name that is then already taken gets `_2`, `_3` and so on. A valid name is
never changed. The dialog warns when the view has a name to rename.

| Column | Avro field |
|---|---|
| `my col` | `my_col` |
| `2024` | `_2024` |
| `délai` | `d_lai` |
| `a-b`, beside `a_b` | `a_b_2` |

A renamed field keeps its original name as its `doc` in the file's schema.

## Keys

The dialog opens on the path and takes the keys every
[dialog](../reference/dialogs.md) takes:

| Key | Action |
|---|---|
| <kbd>↓</kbd> <kbd>↑</kbd> or <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between format, path and options |
| <kbd>←</kbd> <kbd>→</kbd> | Change the format, or the compression; in the path, move the cursor |
| <kbd>Space</kbd> | Toggle a checkbox; the next format or compression |
| <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | In the path: step through paths you exported to before |
| <kbd>Enter</kbd> | Export, from anywhere in the form. On a blank path the form says "Enter a file path." instead |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> | Close without exporting |

Typing a path with a known extension selects the matching format, and a
trailing `.gz`/`.zst`/`.bz2`/`.xz` sets the compression (`out.csv.gz` selects
CSV, gzipped). Picking a format afterward changes the typed extension to
match, so the file's name always agrees with its contents.

Overwrite confirmation defaults to **No**: <kbd>←</kbd> <kbd>→</kbd> or
<kbd>Tab</kbd> pick **Overwrite** or **No**, <kbd>Enter</kbd> confirms the one
picked, and <kbd>Esc</kbd> declines. Declining returns to the form with your
path intact. The chart's export dialog asks the same way.

The CSV **Delimiter** starts as the `--delimiter` value if you gave one,
otherwise a comma. Only its first ASCII character counts. <kbd>Tab</kbd> moves
focus, so you cannot type a tab there; pick TSV instead.

The **Format** row lists the formats, with the chosen one highlighted. The
rows under it hold that format's options, and they change as <kbd>←</kbd>
<kbd>→</kbd> change the format. When the row is too narrow for every format,
it shows only the chosen one, `‹ TSV ›`. A click on a format
chooses it.

## Overwriting

An export is written to a hidden file beside the destination, named
`.datui-XXXXXX-<name>`, and moved into place only once every byte, including
the compressed file's end, is written and synced to disk. The status line says
`Exported to` only after that move.

| Case | Result |
|---|---|
| The export fails | The old file keeps its bytes and permissions; a new export leaves no file. The hidden file is removed. The dialog comes back as you left it, with the reason under the fields: fix the path and press <kbd>Enter</kbd> |
| You confirmed the overwrite | The file is replaced whole. On Linux and macOS it keeps its permission bits |
| A file appears at the path after you pressed <kbd>Enter</kbd>, so you were never asked about overwriting it | It is left alone and the export fails |
| The file is read-only or you may not write it, or the path is a directory, a pipe or a device | The export fails before anything is written |
| The directory is not writable | The export fails, even if the file itself is writable, because the hidden file is created in that directory |
| The path is a symbolic link | The file it points to is replaced; the link stays |

The replacement is a new file: the old file's owner, ACLs, extended attributes
and hard links are not carried over, and on Windows its attributes are the
defaults. If datui is killed during an export, the hidden file can be left
behind. A power cut just after the move can leave the old file in place. On
filesystems with no atomic no-replace rename and no hard links, such as FAT and
some network mounts, a file created at the path in the instant before the move
can still be replaced. Chart and Data Quality report exports work the same way.

## Large exports

| Export | Written |
|---|---|
| CSV, TSV and PSV without compression, Parquet | Streamed: written in batches as the rows are read; the export never holds the whole view |
| Compressed CSV, TSV and PSV, JSON, NDJSON, Arrow IPC, Avro | The whole view is read into memory, then written |

Streaming needs `streaming` on in `[performance]` (the default) and a build
with the `streaming` feature. If either is missing, every export reads the
whole view first. Streaming limits what the export holds, not what the view
needs: a sort, a `by` or `GROUP BY` query, or a join
still holds its input in memory before the first row is written. The status
line counts the bytes written so far.

## Source file

For a dataset with missing columns or conflicting types, **Source file** adds
the original file path to each exported row. This helps trace empty values
back to the files that produced them.

The `∅`, `·` and `≠` [cell markers](open-files.md#files-that-disagree) all
export as null. Use the source path with the original file's schema to
distinguish their causes.

The option is offered only for those datasets, and is off by default. If the
data already has a column called `source_file`, that column is left alone and
datui adds its own at the end as `source_file_1`, or the next free number.

Charts export separately, to PNG or EPS, from the [chart view](charting.md#export).
