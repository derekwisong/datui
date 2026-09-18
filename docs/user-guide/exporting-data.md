# Exporting Data

Press <kbd>e</kbd> to write the current view to a file: the rows and columns
as queried, filtered and sorted.

![Export Demo](../demos/08-export.gif)

| Format | Extension | Options |
|---|---|---|
| CSV | `.csv` | Delimiter, include header |
| Parquet | `.parquet` | |
| JSON | `.json` | One array |
| NDJSON | `.jsonl` | One object per line |
| Arrow IPC | `.arrow` | |
| Avro | `.avro` | |

Every format also offers **Source file** for a dataset whose files disagree; see
[below](#source-file).

Excel and ORC can be read but not written.

## Keys

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between format, path, options and buttons |
| <kbd>↑</kbd> <kbd>↓</kbd> | Change the format |
| <kbd>Enter</kbd> | Toggle a checkbox, or press **Export** or **Cancel** |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> | Close without exporting |

Numbers are written as raw values, whatever the
[display formatting](configuration.md#number-formatting) shows on screen.

## Source file

A folder of Parquet files whose files disagree shows an empty cell three ways:
a null the data holds, a column the file was written without, and a column the
file keeps in another type. See
[datasets whose files differ](loading-data.md#files-that-disagree).

No file format has "absent", so an export writes all three as null. **Source
file** adds a column naming the file each row came from, which is enough to
tell them apart downstream: a row whose file never had the column is a
different thing from a row whose file had it and left it empty.

The option is offered only for those datasets, and is off by default.

Charts export separately, to PNG or EPS, from the [chart view](charting.md#export).
