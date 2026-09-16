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

Charts export separately, to PNG or EPS, from the [chart view](charting.md#export).
