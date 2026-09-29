# Pivot and melt

Press <kbd>p</kbd> to reshape the current query and filters.
**Pivot** turns values into columns; **Melt** turns columns into rows.

## Pivot

With the [quick-start penguin data](../getting-started/quick-start.md),
run this query to keep the three needed columns and exclude missing sex values:

```text
select species, sex, body_mass_g where not null sex
```

Press <kbd>p</kbd> and choose these settings on **Pivot**:

| Setting | Value | Meaning |
|---|---|---|
| Index | `species` | One row per species |
| Columns | `sex` | One output column per sex |
| Values | `body_mass_g` | Values to aggregate |
| Aggregate | `avg` | Mean body mass for each species/sex pair |

Use <kbd>Tab</kbd> to move through settings. <kbd>Space</kbd> opens a column
picker; type to narrow, select columns and press <kbd>Enter</kbd> to close it.
Press <kbd>Enter</kbd> in the form to apply.

The result has three rows. Values below are rounded to one decimal:

| species | female | male |
|---|---:|---:|
| Adelie | 3368.8 | 4043.5 |
| Chinstrap | 3527.2 | 3939.0 |
| Gentoo | 4679.7 | 5484.8 |

Output columns are sorted alphabetically. Pivot reads every affected row into
memory to discover the columns; filter large datasets first.

### Choose an aggregate

Available functions are `last` (default), `first`, `min`, `max`, `avg`, `med`,
`std` and `count`. String values support only `first` and `last`.
Those two functions are positional and retain nulls. After sorting with nulls
last, `last` returns null for any group ending in a null.

## Melt

To turn the pivot above back into rows, press <kbd>p</kbd>, select **Melt**,
and set:

| Setting | Value |
|---|---|
| Index | `species` |
| Strategy | **All except index** |
| Variable name | `sex` |
| Value name | `mean_mass_g` |

Apply with <kbd>Enter</kbd>. The result has six rows with columns `species`,
`sex` and `mean_mass_g`. This reshapes the averages; it does not recover the
original individual penguins.

Other strategies select columns by regex (**By pattern**), data type
(**By type**) or an **Explicit list**. The form shows how many columns match
before it runs. Default output names are `variable` and `value`.

Press <kbd>R</kbd> from the table to clear the reshape and other view changes.

## Keys

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the rows |
| <kbd>←</kbd> <kbd>→</kbd> | Switch tab, or move the cursor in a text field |
| <kbd>Space</kbd> or typing | Open the focused row's picker, narrowed by what you type |
| <kbd>↑</kbd> <kbd>↓</kbd> in the picker | Move; <kbd>Space</kbd> chooses, or toggles where several can be chosen |
| <kbd>Enter</kbd> | In the picker, choose; otherwise apply, from anywhere |
| <kbd>Esc</kbd> | Close the picker; pressed again, close without applying |
| <kbd>?</kbd> | Help |

## Views

A [view](views.md) saved after a pivot or melt stores the reshape. When
it is applied, the order is query, filters, sort, pivot or melt, column order.
