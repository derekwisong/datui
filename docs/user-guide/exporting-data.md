# Export data

Press <kbd>e</kbd> to write the current view to a file: the rows and columns
as queried, filtered and sorted.

## Save a CSV

1. Apply the query and filters to export.
2. Press <kbd>e</kbd> and enter `result.csv` in **Path**.
3. Press <kbd>Enter</kbd>. If the file exists, confirm whether to overwrite it.

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
`str.json_decode`. Binary data inside a list or struct has no JSON form, so
CSV refuses that column: hide it or pick another format.

Parquet, Arrow, JSON and NDJSON keep lists, arrays and structs as they are.

## Avro types

Avro keeps booleans, 32- and 64-bit integers and floats, strings, binary,
decimals, dates, millisecond and microsecond datetimes, lists and structs.
Other columns are converted, inside lists and structs too:

| Column | Avro |
|---|---|
| Array | List |
| Categorical, enum | String |
| 8- and 16-bit integer | 32-bit integer |
| Unsigned 32- and 64-bit integer | 64-bit integer; a value past its range fails the export |
| 128-bit integer | Decimal |
| Nanosecond datetime | Microsecond datetime |
| Datetime with a time zone | The same instant in UTC, without the zone |
| Time, duration | 64-bit integer of microseconds |
| Null | String |

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
