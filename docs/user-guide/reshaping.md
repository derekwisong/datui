# Pivot and melt

Press <kbd>p</kbd> to reshape the current query and filters.
**Pivot** turns values into columns; **Melt** turns columns into rows.

## Pivot

Open **US baby names (1880-2017)** from **Public datasets**: 1.9 million rows
of `year`, `sex`, `name`, `n` and `prop`. Keep three girls' names:

```sql
SELECT year, name, n FROM df WHERE sex = 'F' AND name IN ('Emma', 'Jennifer', 'Olivia')
```

376 rows. Press <kbd>p</kbd> and choose these settings on **Pivot**:

| Setting | Value | Meaning |
|---|---|---|
| Index | `year` | One row per year |
| Columns | `name` | One output column per name |
| Values | `n` | The count to put in each cell |
| Aggregate | `last` | There is one value per year and name, so any aggregate keeps it |

Use <kbd>Tab</kbd> to move through settings. <kbd>Space</kbd> opens a column
picker; type to narrow, <kbd>Space</kbd> to select and <kbd>Enter</kbd> to
close it. Press <kbd>Enter</kbd> in the form to apply.

The result has 138 rows, one per year, and the columns `year`, `Emma`,
`Jennifer` and `Olivia`. A year with no count for a name is null: Jennifer is
`∅` before 1916, and in 1917 and 1918. Chart it as three lines; see
[the examples](charting.md#examples-on-the-built-in-datasets).

Output columns are sorted alphabetically. Pivot reads the rows once, in the
background, and keeps one value per index and column pair in memory; filter
large datasets first.

### Choose an aggregate

Available functions are `last` (default), `first`, `min`, `max`, `avg`, `med`,
`std` and `count`. String values support only `first` and `last`.
Those two functions are positional and retain nulls. After sorting with nulls
last, `last` returns null for any group ending in a null.

### Count with a pivot

`count` turns rows into tallies. On **Space launches (1957-2018)**, with no
query, press <kbd>p</kbd> and set:

| Setting | Value |
|---|---|
| Index | `launch_year` |
| Columns | `category` |
| Values | `tag` |
| Aggregate | `count` |

62 rows of launches per year: `O` for those that reached orbit, `F` for
failures. 1967 has 127 and 12. Rows come in the order the years first appear
in the file; a line chart draws them in X order regardless.

## Melt

To turn the names pivot back into rows, press <kbd>p</kbd>, select **Melt**,
and set:

| Setting | Value |
|---|---|
| Index | `year` |
| Strategy | **All except index** |
| Variable name | `name` |
| Value name | `n` |

The name fields start as `variable` and `value`, selected: typing replaces
them. Apply with <kbd>Enter</kbd>. The result has 414
rows with columns `year`, `name` and `n`: the 376 counts, plus a null for each
of the 38 years Jennifer has none.

Other strategies select columns by regex (**By pattern**), data type
(**By type**) or an **Explicit list**. The form shows how many columns match
before it runs.

Press <kbd>R</kbd> from the table to clear the reshape and other view changes.

## Keys

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the rows |
| <kbd>←</kbd> <kbd>→</kbd> | Switch tab, or move the cursor in a text field |
| <kbd>Space</kbd> or typing | Open the focused row's picker, narrowed by what you type |
| <kbd>↑</kbd> <kbd>↓</kbd> in the picker | Move; <kbd>Space</kbd> chooses, or toggles where several can be chosen |
| <kbd>Enter</kbd> | In the picker, choose; otherwise apply, from anywhere |
| <kbd>Esc</kbd> | Stop a pivot being computed; close the picker; otherwise close without applying |
| <kbd>?</kbd> | Help |

## Views

A [view](views.md) saved after a pivot or melt stores the reshape and the
query, filters and sort it ran over. When it is applied, the order is that
query, filters and sort, the pivot or melt, then any SQL, filters and sort on
its result, then column order.
