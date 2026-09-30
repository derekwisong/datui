# Queries and search

Press <kbd>/</kbd> to open the query prompt. It opens on **SQL**, or on the
active query's mode when you edit one.
<kbd>Ctrl</kbd>+<kbd>T</kbd> switches mode without leaving the input;
<kbd>Shift</kbd>+<kbd>Tab</kbd> moves to the tab bar, where <kbd>←</kbd>
<kbd>→</kbd> switch too.

| Mode | What you type | Example |
|---|---|---|
| **SQL** | SQL over the table named `df` | `SELECT dept, COUNT(*) AS n FROM df GROUP BY dept ORDER BY n DESC` |
| **Search** | Words; each word's letters in order, in any text column | `smth london` |
| **q-style** | Datui's short language, a subset of q, described below | `select name, salary by dept where salary > 100000` |

<kbd>Enter</kbd> runs the query, <kbd>Esc</kbd> cancels, <kbd>↑</kbd> <kbd>↓</kbd>
walk the history of the current mode. Submit an empty query to return to the
full table. Running a query — or clearing one — starts a fresh view: sidebar
filters, sort, frozen columns and pivot/melt are dropped. Apply sidebar settings after the query.

The prompt stays open until the query's first rows are in. A query that fails
on the data is not applied: the reason shows under it, the table keeps what it
showed, and the query is still there to fix.

To open on another mode, set it in the [config](../reference/settings.md#query-views-debug):

```toml
[query]
default_mode = "q-style"   # "sql" (default), "search" or "q-style"
```

A build without SQL has no SQL tab and opens on Search instead.

## Run a query

With the [quick-start penguin data](../getting-started/quick-start.md), enter
this on the **SQL** tab:

```sql
SELECT species, AVG(body_mass_g) AS mean_mass_g
FROM df
GROUP BY species
```

It returns three species averages. Gentoo has the highest, about 5,076 g.

## Write SQL

| Key | In the SQL input |
|---|---|
| <kbd>Tab</kbd> | Complete the column name, or `df`, being typed. Press again for the next match |
| <kbd>Alt</kbd>+<kbd>Enter</kbd> | Start a new line |
| <kbd>Enter</kbd> | Run the statement |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move between lines; past the first or last, walk the history |

- The columns of `df` and their types are listed under the input, narrowed to
  the name being typed. Names with spaces complete in double quotes:
  `"Team 1"`.
- A long statement wraps onto up to four lines instead of scrolling sideways.
- An empty input shows an example to start from: `SELECT * FROM df WHERE ...`.

When a statement fails on a value that will not convert, the reason names
the column, the values that failed and SQL that gets past them:

| Failure | Try |
|---|---|
| `STRPTIME` meets text that does not match the format | Trim it first: `STRPTIME(SUBSTR(Date, 1, 15), '%a %b %d %Y')`, or `REPLACE` |
| `CAST` meets text that is not a number | `TRY_CAST(col AS INT)`, which reads it as null |
| `CAST(col AS DATE)` meets a date not written `YYYY-MM-DD` | `STRPTIME(col, '%d/%m/%Y')` with the format it is written in |

On a table of up to 100,000 rows the count is exact a moment later; on a
larger one it says "at least", from the rows read before the run stopped.

## Search

Type words on the **Search** tab and press <kbd>Enter</kbd>. A row matches
when, for every word, one of its text columns contains that word's characters
in order, not necessarily adjacent, so `smth` finds `Smith`. Matching is
case-insensitive. Reopen the prompt to see how many rows matched.

## q-style

q-style is a scoped option for people who know q: a subset of the q
language, not a complete q or q-sql, and it **evaluates right to left**. The
penguin query above reads:

```text
select mean_mass_g: avg body_mass_g by species
```

The examples below use q-style. See [Query syntax](../reference/query-syntax.md)
for the complete grammar and [the demo](../demos.md#querying) for a recording.

### Choosing columns

```
select a, b, c              # these columns
select                      # every column
select total: price * qty   # a computed column, named with :
select col["First Name"]    # a name with spaces
```

### Filtering rows

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

### Arithmetic

`+`, `-`, `*` and `/` for divide; `%` also divides. Expressions bind **right to left**:
`a * b + c` is `a * (b + c)`. Use parentheses when in doubt. Comparisons bind
the same way, so `(a + b) * 2 > 100` parses as `(a + b) * (2 > 100)` — put
the comparison first: `100 < (a + b) * 2`.

```
select margin: (price - cost) / price where qty > 0
```

### Dates and times

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

### Grouping and aggregating

```
select name, salary by department                         # group
select avg salary, max salary, count name by department   # aggregate
select total: sum[price * qty] by region, year            # computed aggregates
```

A `by` clause without aggregates gives one row per group. With aggregates
(`avg`, `sum`, `min`, `max`, `count`, `std`, `med`, `first`, `last`) you get one
summary row per group. Brackets around the argument are optional. An unaliased
aggregate of a column is named `fn_column` (`avg salary` → `avg_salary`); name
it yourself with `:`.

Press <kbd>Enter</kbd> on a group to see its rows, <kbd>Esc</kbd> to come back.
A group without aggregates shows the columns you selected; an aggregated one shows
every column of the rows behind it, after the query's `where`, key columns first.
The cursor, frozen columns and column order come back with <kbd>Esc</kbd>.

## Saving a query

The active query is saved with a [view](views.md), in its own mode, along with
filters and sort, so it can be replayed on the next file of the same shape.
