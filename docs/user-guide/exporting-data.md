# Export data

Press <kbd>e</kbd> to write the current view to a file: the rows and columns
as queried, filtered and sorted.

## Save a CSV

1. Open **Premier League (2020-21)** from **Public datasets** and run the
   [goals query](querying-data.md#dates-and-messy-text).
2. Press <kbd>e</kbd> and type `goals.csv` in **Path**.
3. Press <kbd>Enter</kbd>. If the file exists, confirm whether to overwrite it.

The status line says `Exported to goals.csv`. The file holds a header and 380
matches, in the query's order:

```text
Round,match_date,home,away,goals
4,2020-10-04,Aston Villa,Liverpool,9
22,2021-02-02,Manchester Utd,Southampton,9
14,2020-12-20,Manchester Utd,Leeds United,8
```

`match_date` is written as a date because the query casts it; a `STRPTIME`
result alone is a datetime and exports as `2020-10-04T00:00:00.000000`.

The file contains all matching rows and displayed columns, not just the page
on screen. Numbers use their raw values, without display formatting.

| Format | Extension | Options |
|---|---|---|
| CSV | `.csv` | Delimiter, include header |
| Parquet | `.parquet` | |
| JSON | `.json` | One array |
| NDJSON | `.jsonl`, `.ndjson` | One object per line |
| Arrow IPC | `.arrow`, `.ipc`, `.feather` | |
| Avro | `.avro` | |

Every format also offers **Source file** for a dataset whose files disagree; see
[below](#source-file).

Excel and ORC can be read but not written.

## Lists and structs

A `by` query, SQL `ARRAY_AGG` or a nested source file gives list, array or
struct columns. CSV has no such types, so a CSV export writes each of those
cells as JSON text; the dialog says so when the view has one.

| Table shows | CSV cell |
|---|---|
| `[1, 2]` | `[1,2]` |
| `["a", "b"]` | `["a","b"]` |
| `{1,"a"}` | `{"x":1,"label":"a"}` |

A null list or struct is an empty field; NaN and infinity inside one are
`null`, which JSON has no other spelling for. Polars reads the text back with
`str.json_decode`.

Parquet, Arrow, JSON and NDJSON keep lists, arrays and structs as they are.

## Binary

CSV, JSON and NDJSON have no bytes type, so they write a binary value as
standard base64 text, inside lists and structs too: `hi` is written `aGk=`.
Polars reads it back with `str.decode("base64")`. Parquet, Arrow and Avro keep
the bytes.

## Durations

CSV has no duration type, so a CSV export writes a duration as ISO 8601 text
in seconds: the text JSON and NDJSON exports write, and the same alone or inside
a list or struct. It is exact in every unit, down to the nanosecond.

| Table shows | CSV cell |
|---|---|
| `1h 2m 3s 4ms` | `PT3723.004S` |
| `-1s -500ms` | `-PT1.5S` |
| `1500ns` | `PT0.0000015S` |
| `0ms` | `P0D` |

Parquet and Arrow keep the duration type; Avro writes microseconds, below.

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

An Avro name is letters, digits and `_`, and does not start with a digit, so
Avro export renames columns and struct fields that are not: any other
character becomes `_`, a leading digit gets a `_` in front, and a name that
is then taken gets `_2`, `_3` and so on. A valid name is never changed. The
dialog says so when the view has such a name.

| Column | Avro field |
|---|---|
| `my col` | `my_col` |
| `2024` | `_2024` |
| `délai` | `d_lai` |
| `a-b`, beside `a_b` | `a_b_2` |

A renamed field keeps its original name as its `doc` in the file's schema.

## Keys

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between format, path and options |
| <kbd>↑</kbd> <kbd>↓</kbd> | Change the format, or the compression |
| <kbd>Space</kbd> | Toggle a checkbox |
| <kbd>Enter</kbd> | Export, from anywhere in the form. On a blank path the form says "Enter a file path." instead |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> | Close without exporting |

Typing a path with a known extension selects the matching format, and a
trailing `.gz`/`.zst`/`.bz2`/`.xz` sets the compression (`out.csv.gz` selects
CSV, gzipped); picking a format afterward rewrites the typed extension to
match, so the file's name and its bytes agree.

Overwrite confirmation defaults to **No**. Declining returns to the form
with your path intact.

## Overwriting

An export is written to a hidden file beside the destination, named
`.datui-XXXXXX-<name>`, and moved into place only once every byte, including
the compressed file's end, is written and synced to disk. The status line says
`Exported to` only after that move.

| Case | Result |
|---|---|
| The export fails | The old file keeps its bytes and permissions; a new export leaves no file. The hidden file is removed |
| You confirmed the overwrite | The file is replaced whole. On Linux and macOS it keeps its permission bits |
| A file appears at the path after you pressed <kbd>Enter</kbd> without being asked | It is left alone and the export fails |
| The file is read-only or you may not write it, or the path is a directory, a pipe or a device | The export fails before anything is written |
| The directory is not writable | The export fails, even where the file itself is writable: the hidden file is made in the directory |
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
| CSV without compression, Parquet | Streamed: written in batches as the rows are read; the export never holds the whole view |
| Compressed CSV, JSON, NDJSON, Arrow IPC, Avro | The whole view is read into memory, then written |

Streaming needs `polars_streaming` on in `[performance]`, the default, and a
build with the `streaming` feature; without either, every export reads the
whole view first. Streaming bounds what the export
holds, not what the view needs: a sort, a `by` or `GROUP BY` query, or a join
still holds its input in memory before the first row is written. The status
line counts the bytes written so far.

## Source file

For a dataset with missing columns or conflicting types, **Source file** adds
the original file path to each exported row. This helps trace empty values
back to the files that produced them.

The `∅`, `·` and `≠` [cell markers](loading-data.md#files-that-disagree) all
export as null. Use the source path with the original file's schema to
distinguish their causes.

The option is offered only for those datasets, and is off by default. If the
data already has a column called `source_file`, that column is left alone and
datui's is added at the end as `source_file_1`, or the next free number.

Charts export separately, to PNG or EPS, from the [chart view](charting.md#export).
