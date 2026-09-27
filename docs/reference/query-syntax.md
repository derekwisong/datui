# Query Syntax

The grammar of the Query tab of the query prompt. For a walkthrough with
examples, see [Querying Data](../user-guide/querying-data.md).

## Structure of a query

```
select [columns] [by group_columns] [where conditions]
```

| Clause | Role |
|---|---|
| `select` | Required. Alone it means all columns; otherwise a comma-separated list of column expressions |
| `by` | Optional. Grouping and aggregation |
| `where` | Optional. Filtering |

Clause order is fixed and each clause appears at most once. The parser splits
on the keywords `where` and `by`, respecting parentheses and brackets.

## The `:` assignment (aliasing)

`name : expression` names an expression. The left side is the new column or
group name, an identifier (`total`) or `col["name with spaces"]`; the right
side is any expression (column reference, literal, arithmetic, function call).

```
select a, b, sum_ab: a + b
select renamed: col["Original Name"]
by region_name: region, total: sales + tax
```

Assignment works in both select and by. In by it defines computed group keys
or renames.

## Columns with spaces in their names

Identifiers cannot contain spaces. For columns (or aliases) with spaces, use
`col["..."]` with a quoted string, or `col[identifier]` for a name without
spaces. The same syntax works in select, by and where.

```
select col["First Name"], col["Last Name"]
select no_spaces: col["name with spaces"]
```

## Right-to-left expression parsing

There is no operator precedence. Expressions are parsed right-to-left: the
leftmost binary operator is the root, and everything to its right is parsed
first as a unit.

- `a + b * c` → `a + (b * c)`
- `a * b + c` → `a * (b + c)`, not `(a * b) + c`

Put the operation you want done first on the right, or use `()` to override
grouping:

```
select (a + b) * c
select a, b where (x > 1) | (y < 0)
```

Parentheses also matter for `,` and `|` in where: splitting on comma and pipe
respects nesting, so you can wrap ORs in `()` and combine them with commas.
See [Where clause](#where-clause--and-).

## Select clause

- `select` — all columns, no expressions
- `select a, b, c` — those columns or expressions, in order, separated by `,`
- `select a, b: x + y, c` — columns and aliased expressions

## By clause (grouping and aggregation)

- `by col1, col2` — group by those columns; non-group columns become list columns, and the UI supports drill-down
- `by region, total: sales + tax` — group by a column and a computed expression
- `select avg salary, min id by department` — aggregations per group

By uses the same comma-separated list and `name : expression` rules as
select. Aggregation functions (`avg`, `min`, `max`, `count`, `sum`, `std`,
`med`) can be written `fn[expr]` or `fn expr`; brackets are optional.

An unaliased aggregate of a single column is named `{fn}_{column}`, so
`select avg salary, max salary by department` yields `avg_salary` and
`max_salary`; an explicit alias (`total: sum[price]`) overrides it.

## Where clause: `,` and `|`

The where clause combines conditions with two separators:

- `,` — AND. Each comma-separated segment is one ANDed condition.
- `|` — OR. Within one segment, `|` separates alternatives that are ORed.

The where part is split on `,` first (respecting `()` and `[]`), then each
segment on `|`, so `,` has broader scope than `|`:

| Written | Means |
|---|---|
| `where a > 10, b < 2` | `(a > 10) AND (b < 2)` |
| `where a > 10 \| a < 5` | `(a > 10) OR (a < 5)` |
| `where a > 10 \| a < 5, b = 2` | `(a > 10 OR a < 5) AND (b = 2)` |
| `A, B \| C` | `A AND (B OR C)` |
| `A \| B, C \| D` | `(A OR B) AND (C OR D)` |

For more complex logic, wrap OR subexpressions in `()` — parentheses keep `|`
inside one AND term — and separate the groups with `,`.

## Operators and literals

| Kind | Syntax |
|---|---|
| Arithmetic | `+` `-` `*` `/` `%` (`/` and `%` both divide; `%` is not modulo) |
| Equal, not equal | `=`, `!=`, `<>` (same as `!=`) |
| Ordering | `<` `>` `<=` `>=` |
| Coalesce | `^` — first non-null, left to right; `a^b^c` = coalesce(a, b, c), binding right-to-left as `a^(b^c)` |
| Numbers | `42`, `3.14` |
| Strings | `"hello"`, `\"` for an embedded quote |
| Date literals | `2021.01.01` (YYYY.MM.DD) |
| Timestamp literals | `2021.01.15T14:30:00.123456` (YYYY.MM.DDTHH:MM:SS[.fff...]); fractional-second digits set precision: 1–3 = ms, 4–6 = μs, 7–9 = ns |

Either side of a comparison can be a column, a literal or an expression:
`where a = 10`, `where created_at.date > other_date_col`.

## Date and datetime accessors

For columns of type Date or Datetime (with or without timezone), dot notation
extracts components: `column_ref.accessor`.

| Accessor | Result | Description |
|---|---|---|
| `date` | Date | Date part (year-month-day); Datetime only |
| `time` | Time | Time part (Polars Time type); Datetime only |
| `year` | Int32 | Year |
| `month` | Int8 | Month (1–12) |
| `week` | Int8 | Week number |
| `day` | Int8 | Day of month (1–31) |
| `dow` | Int8 | Day of week (1=Monday … 7=Sunday, ISO) |
| `month_start` | Date/Datetime | First day of month, at midnight for Datetime |
| `month_end` | Date/Datetime | Last day of month |
| `format["fmt"]` | String | Format as string (chrono strftime, e.g. `"%Y-%m"`) |

### String accessors

Apply to String columns:

| Accessor | Result | Description |
|---|---|---|
| `len` | Int32 | Character length |
| `upper` | String | Uppercase |
| `lower` | String | Lowercase |
| `starts_with["x"]` | Boolean | True if the string starts with `x` |
| `ends_with["x"]` | Boolean | True if the string ends with `x` |
| `contains["x"]` | Boolean | True if the string contains `x` |

An accessor result is automatically aliased to `{column}_{accessor}`, so
`timestamp.date` becomes `timestamp_date`.

### Examples

```
select event_date: timestamp.date
select col["Created At"].date, col["Created At"].year
select name, event_time.time
select order_date, order_date.month, order_date.dow by order_date.year
select a: coln^cola^colb
select name.len, name.upper, dt_col.format["%Y-%m"]
select where created_at.date > other_date_col
select where dt_col.date > 2021.01.01
select where ts_col > 2021.01.15T14:30:00.123456
select where event_ts.month = 12, event_ts.dow = 1
select where city_name.ends_with["lanta"]
select where null col1
select where not null col1
```

## Functions

Functions are used for aggregation (typically in select with by) and for
logic in where. Write `fn[expr]` or `fn expr`; brackets are optional.

### Aggregation functions

| Function | Aliases | Description | Example |
|---|---|---|---|
| `avg` | `mean` | Average | `select avg[price] by category` |
| `min` | — | Minimum | `select min[qty] by region` |
| `max` | — | Maximum | `select max[amount] by id` |
| `count` | — | Count of non-null values | `select count[id] by status` |
| `sum` | — | Sum | `select sum[amount] by year` |
| `first` | — | First value in group | `select first[value] by group` |
| `last` | — | Last value in group | `select last[value] by group` |
| `std` | `stddev` | Standard deviation | `select std[score] by group` |
| `med` | `median` | Median | `select med[price] by type` |
| `len` | `length` | String length (chars) | `select len[name] by category` |

### Logic functions

| Function | Description | Example |
|---|---|---|
| `not` | Logical negation | `where not[a = b]`, `where not x > 10` |
| `null` | Is null | `where null col1`, `where null[col1]` |
| `not null` | Is not null | `where not null col1` |

### Scalar functions

| Function | Description | Example |
|---|---|---|
| `len` / `length` | String length | `select len[name]`, `where len[name] > 5` |
| `upper` | Uppercase string | `select upper[name]`, `where upper[city] = "ATLANTA"` |
| `lower` | Lowercase string | `select lower[name]` |
| `abs` | Absolute value | `select abs[x]` |
| `floor` | Numeric floor | `select floor[price]` |
| `ceil` / `ceiling` | Numeric ceiling | `select ceil[score]` |
