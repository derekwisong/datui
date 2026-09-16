# Dataset Info

Press <kbd>i</kbd> for the schema and the facts about the file. <kbd>i</kbd>
or <kbd>Esc</kbd> closes it.

![Info Panel Demo](../demos/03-info.gif)

| Tab | Shows |
|---|---|
| **Schema** | Total rows and columns, columns by type, whether the schema is stored (Parquet) or inferred (CSV, JSON), and every column with its type and, for Parquet, its codec and compression ratio |
| **Resources** | File size, the memory used by the buffered rows, the format, and for Parquet the overall compression ratio, row groups, version and writer |
| **Partitions** | For a hive-partitioned dataset, the partition columns and their values |

## Keys

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch tab |
| <kbd>Tab</kbd> | On the Schema tab, move focus between the tab bar and the column table |
| <kbd>↑</kbd> <kbd>↓</kbd> | Scroll the column table |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> <kbd>i</kbd> | Close |

The row count is for the whole dataset, not the rows on screen. The type of
each column is also shown in the table's second header row, which <kbd>D</kbd>
toggles.
