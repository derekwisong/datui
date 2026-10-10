# Pivot and melt

<kbd>p</kbd> reshapes the table: **Pivot** turns values into columns, **Melt**
turns columns into rows. It works on the rows left after the query and filters.

<kbd>p</kbd> opens the builder full screen: the form on the left, a live
preview of the result on the right. In a terminal narrower than 100 columns,
the preview sits under the form.

| Preview line | Says |
|---|---|
| Input | The rows the preview ran over: `all 376 rows`, or the `first 1,000 rows of 1,939,184` of a larger view, plus `unsorted` when that larger view is sorted (a small sorted view is read in its order) |
| Result | The shape, `rows × columns`. `?` marks what the first rows cannot tell; a pivot's columns read `12+` until applied |
| `▲` | A pivot with 100 or more new columns, or why the reshape fails on these rows |

Under these lines are the first rows of the result, typed and colored as in
the table. Each change to the form runs the preview again in the background;
<kbd>Enter</kbd> applies the reshape to the whole view.

## Pivot

Open **US baby names (1880-2017)** from **Example datasets**: 1.9 million rows
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
picker; type to narrow, <kbd>Space</kbd> to toggle a column where several
can be chosen, and <kbd>Enter</kbd> to choose or close it. <kbd>←</kbd> <kbd>→</kbd> step **Aggregate**.

![The Pivot & Melt builder: Index year, Columns name, Values n, Aggregate last; the preview reads all 376 rows in, 138 rows × 4 columns out, with Jennifer null before 1916](../demos/screenshots/pivot-builder.png)

One column per name, a row per year? The preview says 138 rows × 4 columns
before anything is applied. <kbd>p</kbd>, then **Index** `year`,
**Columns** `name`, **Values** `n`.

The status footer under the builder reads `<dataset> › pivot & melt` and names the keys
that act on the focused field, here `Enter Apply  Space Open  Esc Close`.

Press <kbd>Enter</kbd> to apply.

The result has 138 rows, one per year, and the columns `year`, `Emma`,
`Jennifer` and `Olivia`. A year with no count for a name is null: Jennifer is
`∅` before 1916, and in 1917 and 1918. Chart it as three lines; see
[the examples](charting.md#examples-on-the-built-in-datasets).

Output columns are sorted alphabetically. Pivot reads the rows once, in the
background, and keeps one value per index and column pair in memory; filter
large datasets first. A pivot that would make more than 10,000 columns is
refused, and the message gives the count. So is a [view](views.md) whose pivot
would.

When a **Columns** value is a date or datetime outside the calendar's range,
such as a sentinel of `i64::MIN + 1` microseconds, its new column is named by
the stored number, as the table shows it: `-9223372036854775807 us since 1970-01-01 UTC`.

### Choose an aggregate

Available functions are `last` (default), `first`, `min`, `max`, `avg`, `med`,
`std` and `count`. String values support only `first` and `last`.
Those two pick by position and keep nulls: after a sort with nulls last,
`last` returns null for any group that ends in a null.

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
(**By type**) or an **Explicit list**. Before you apply, the form shows how
many columns match and the preview shows the rows they make.

Melting dates together with text makes the values text. A date past the
calendar's range becomes its stored number, as in a pivot.

Press <kbd>R</kbd> from the table to clear the reshape and other view changes.

## Keys

The builder uses the same keys as every [dialog](../reference/dialogs.md). It
opens on its first row, which chooses Pivot or Melt.

| Key | Action |
|---|---|
| <kbd>↓</kbd> <kbd>↑</kbd> or <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between the rows |
| <kbd>←</kbd> <kbd>→</kbd> | On the first row, switch Pivot and Melt; on Aggregate, Strategy or Type, the previous or next value; on Columns or Values, the previous or next column; in a text field, move the cursor |
| <kbd>Space</kbd> | On a choice, its next value; on a column row, open its picker (type to narrow) |
| <kbd>↑</kbd> <kbd>↓</kbd> in the picker | Move. <kbd>Space</kbd> toggles a column where several can be chosen; elsewhere it chooses, until you type (then it types a space) |
| <kbd>Enter</kbd> | In the picker, choose; otherwise apply, from anywhere |
| <kbd>Esc</kbd> | While a pivot is computed, stop it and keep the builder; in the picker, close just the picker, undoing the toggles made since it opened (<kbd>Enter</kbd> keeps them); otherwise close without applying |
| <kbd>?</kbd> | Help |

## Views

A [view](views.md) saved after a pivot or melt stores the reshape and the
query, filters and sort it ran over. When it is applied, the steps run in this
order: that query, filters and sort; the pivot or melt; any SQL, filters and
sort on its result; then the column order.
