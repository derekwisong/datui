# Querying Data

Press <kbd>/</kbd> to open the query prompt. It has three tabs; <kbd>Tab</kbd>
and <kbd>Shift</kbd>+<kbd>Tab</kbd> move between them and into the input.

| Tab | What you type | Example |
|---|---|---|
| **Query** | Datui's own short language, described below | `select name, salary where salary > 100000 by dept` |
| **SQL** | Standard SQL. The table is called `df` | `SELECT dept, COUNT(*) AS n FROM df GROUP BY dept ORDER BY n DESC` |
| **Fuzzy** | Words to look for in any text column | `smith london` |

<kbd>Enter</kbd> runs the query, <kbd>Esc</kbd> cancels, <kbd>↑</kbd> <kbd>↓</kbd>
walk the history of the current tab. Submit an empty query to return to the
full table. <kbd>R</kbd> in the main view does the same and also clears filters
and sort.

![Querying Demo](../demos/02-querying.gif)

The rest of this page is about the **Query** tab. The complete grammar is in
the [Query Syntax reference](../reference/query-syntax.md).

## Choosing columns

```
select a, b, c              # these columns
select                      # every column
select total: price * qty   # a computed column, named with :
select col["First Name"]    # a name with spaces
```

## Filtering rows

```
select where a > 10                      # comparison: = != < > <= >=
select where a > 10, b < 2               # comma is AND
select where a > 10 | a < 5              # pipe is OR
select where (a > 10 | a < 5), b = 2     # parentheses group
select where name = "Smith"              # strings in double quotes
select where null email                  # null tests
select where not null email
select where city.contains["York"]       # string accessors
```

## Arithmetic

`+`, `-`, `*` and `%` for divide. Expressions bind **right to left**: `a * b + c`
is `a * (b + c)`. Use parentheses when in doubt.

```
select margin: (price - cost) % price where qty > 0
```

## Dates and times

Date and Datetime columns have dot accessors, and date literals are written
`YYYY.MM.DD`.

```
select order_date.year, order_date.month, total
select where created_at.date >= 2024.01.01, created_at.dow = 1
select day: ts.format["%Y-%m-%d"]
```

Accessors include `date`, `time`, `year`, `month`, `week`, `day`, `dow`,
`month_start`, `month_end` and `format["..."]`. The
[reference](../reference/query-syntax.md#date-and-datetime-accessors) has the
full list.

## Grouping and aggregating

```
select name, salary by department                         # group: drill into a row with Enter
select avg salary, max salary, count name by department   # aggregate
select total: sum[price * qty] by region, year            # computed aggregates
```

A `by` clause without aggregates gives one row per group. Press <kbd>Enter</kbd>
on a group to see its rows, <kbd>Esc</kbd> to come back. With aggregates
(`avg`, `sum`, `min`, `max`, `count`, `std`, `med`, `first`, `last`) you get one
summary row per group. Brackets around the argument are optional.

## Fuzzy search

Type on the **Fuzzy** tab and press <kbd>Enter</kbd>. A row matches when its
text columns contain the characters of every word, in order but not necessarily
adjacent, so `smth` finds `Smith`. Matching is case-insensitive.

## Saving a query

The active tab's query is saved with a [template](templates.md), along with
filters and sort, so it can be replayed on the next file of the same shape.
