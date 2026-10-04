# Pivot and melt

<kbd>p</kbd> reshapes the table: **Pivot** turns values into columns, **Melt**
turns columns into rows. It reshapes the rows the query and filters leave.

<kbd>p</kbd> opens the builder full screen: the form on the left, a live
preview of the result on the right. Below 100 columns the preview sits under
the form.

| Preview line | Says |
|---|---|
| Input | The rows the preview ran over: `all 376 rows`, or the `first 1,000 rows of 1,939,184` of a larger view, `unsorted` when the view is sorted (a small sorted view is read in its order) |
| Result | The shape, `rows × columns`. `?` is what the first rows cannot tell; a pivot's columns read `12+` until applied |
| `▲` | A pivot with 100 or more new columns, or why the reshape fails on these rows |

Under them, the first rows of the result, typed and colored as the table
draws them. Each change to the form runs the preview again in the background;
<kbd>Enter</kbd> applies the reshape to the whole view.

## Pivot

Open **US baby names (1880-2017)** from **Public datasets**: 1.9 million rows
of `year`, `sex`, `name`, `n` and `prop`. Keep three girls' names:

```sql,dataset=names,network,rows=376
SELECT year, name, n FROM df WHERE sex = 'F' AND name IN ('Emma', 'Jennifer', 'Olivia')
```

376 rows. Press <kbd>p</kbd> and choose these settings on **Pivot**:

| Setting | Value | Meaning |
|---|---|---|
| Index | `year` | One row per year |
| Columns | `name` | One output column per name |
| Values | `n` | The count to put in each cell |
| Aggregate | `last` | There is one value per year and name, so any aggregate keeps it |

Use <kbd>↓</kbd> to move through settings. <kbd>Space</kbd> opens a column
picker; type to narrow, <kbd>Space</kbd> to select and <kbd>Enter</kbd> to
close it. <kbd>←</kbd> <kbd>→</kbd> step **Aggregate**. The builder, at 100
columns:

```text
╭Pivot & Melt──────────────────────────────────────────────────────────────────────────────────────╮
│  Pivot │ Melt                                    Preview ─────────────────────────────────────── │
│                                                  Input    all 376 rows                           │
│  Index:          year                            Result   138 rows × 4 columns                   │
│  Columns:        name                                                                            │
│ ▎Values:         n                                                                               │
│  Aggregate:      last                            year  Emma  Jennifer  Olivia                    │
│                                                   i64   i64       i64     i64                    │
│                                                  1880  2003         ∅      44                    │
│                                                  1881  2034         ∅      51                    │
│                                                  1882  2303         ∅      52                    │
│                                                  1883  2367         ∅      46                    │
│ year × name → last(n)                            1884  2587         ∅      54                    │
│                                                                                                  │
│  Enter  Apply   Space  Open   Tab  Next   Esc  Close                                             │
╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
```

Press <kbd>Enter</kbd> to apply.

The result has 138 rows, one per year, and the columns `year`, `Emma`,
`Jennifer` and `Olivia`. A year with no count for a name is null: Jennifer is
`∅` before 1916, and in 1917 and 1918. Chart it as three lines; see
[the examples](charting.md#examples-on-the-built-in-datasets).

Output columns are sorted alphabetically. Pivot reads the rows once, in the
background, and keeps one value per index and column pair in memory; filter
large datasets first. A pivot that would make more than 10,000 columns is
refused, with the count it would make; so is a [view](views.md) whose pivot would.

A date or datetime past the calendar's range, such as a sentinel of
`i64::MIN + 1` microseconds, names its column by its stored number, as the
table shows it: `-9223372036854775807 us since 1970-01-01 UTC`.

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
(**By type**) or an **Explicit list**. The form shows how many columns match,
and the preview the rows they make, before it runs.

Melting dates together with text makes the values text. A date past the
calendar's range becomes its stored number, as in a pivot.

Press <kbd>R</kbd> from the table to clear the reshape and other view changes.

## Keys

The builder takes the keys every [dialog](../reference/dialogs.md) takes. It
opens on its first row, Pivot or Melt.

| Key | Action |
|---|---|
| <kbd>↓</kbd> <kbd>↑</kbd> or <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between the rows |
| <kbd>←</kbd> <kbd>→</kbd> | On the first row, switch Pivot and Melt; on Aggregate, Strategy or Type, the previous or next value; on Columns or Values, the previous or next column; in a text field, move the cursor |
| <kbd>Space</kbd> | On a choice, its next value; on a column row, open its picker (type to narrow) |
| <kbd>↑</kbd> <kbd>↓</kbd> in the picker | Move; <kbd>Space</kbd> chooses, or toggles where several can be chosen |
| <kbd>Enter</kbd> | In the picker, choose; otherwise apply, from anywhere |
| <kbd>Esc</kbd> | Stop a pivot being computed and keep the builder; close the picker alone, keeping the toggles made in it; otherwise close without applying |
| <kbd>?</kbd> | Help |

## Views

A [view](views.md) saved after a pivot or melt stores the reshape and the
query, filters and sort it ran over. When it is applied, the order is that
query, filters and sort, the pivot or melt, then any SQL, filters and sort on
its result, then column order.
