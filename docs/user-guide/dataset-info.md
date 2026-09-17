# Dataset Info

Press <kbd>i</kbd> for the schema and the facts about the file. <kbd>i</kbd>
or <kbd>Esc</kbd> closes it.

![Info Panel Demo](../demos/03-info.gif)

| Tab | Shows |
|---|---|
| **Schema** | Total rows and columns, columns by type, whether the schema is stored (Parquet) or inferred (CSV, JSON), and every column with its type and, for Parquet, its codec and compression ratio |
| **Resources** | File size, the memory used by the buffered rows, the format, and for Parquet the overall compression ratio, row groups, version and writer |
| **Partitions** | For a hive-partitioned dataset, the partition columns and their values |
| **Notes** | What datui noticed about the data while reading it. Only there when something is worth saying |

## Keys

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch tab |
| <kbd>Tab</kbd> | On the Schema tab, move focus between the tab bar and the column table |
| <kbd>↑</kbd> <kbd>↓</kbd> | Scroll the column table, or move through the notes |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> <kbd>i</kbd> | Close |

The row count is for the whole dataset, not the rows on screen. The type of
each column is also shown in the table's second header row, which <kbd>D</kbd>
toggles.

## Notes

A folder of Parquet files rarely holds files that agree. Where they do not,
datui says so:

| Note | From |
|---|---|
| A column is not in every file | The footers |
| Files disagree on a column's type, beyond what widening settles | The footers |
| A column is stored as more than one type the scan can read into one | The footers |
| A file's footer could not be read, so it was left out | The footers |

Every note says what it is based on — `in all 6,541 footers`, or
`in 5,000 of 200,000 footers (sample)` — so a count never stands for files
datui has not looked at. None of them costs a read of its own: they come from
the footers the schema and the row count already needed.

A note is one sentence and the line beneath it saying what it is based on;
<kbd>↑</kbd> and <kbd>↓</kbd> move between them. When the list is taller than the
panel, the corner says how many notes are out of view. The panel begins and ends
at a note: half a note is worse than none, since a claim with no basis under it,
or a basis with no claim above it, is the misreading the basis is there to
prevent.

A note describes the dataset as opened. A query, a pivot or a drill-down builds
rows of its own, so the notes step aside; resetting or drilling back up brings
them back.

When there is something to note, the <kbd>i</kbd> key in the control bar takes
a quiet accent until you open the panel. `notes_accent = false` in the
`[display]` section of the [config](configuration.md) turns that off; the Notes
tab is still there either way. A note is an observation about the
data, not a fault in it, so there is no pop-up and no error styling.
