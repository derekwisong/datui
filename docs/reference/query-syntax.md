# Query Syntax

The grammar of the q-style mode of the query prompt: a subset of the q
language, evaluated right to left. For a walkthrough with
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

Use clauses in the order shown, at most once each. Misplaced or repeated
clauses and extra tokens after an expression are errors.

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
- `select distinct carrier, origin` — only the distinct rows of the result;
  `select distinct` alone drops duplicate rows. A column named `distinct` is
  `col["distinct"]`

## By clause (grouping and aggregation)

- `by col1, col2` — group by those columns; non-group columns become list columns, and the UI supports drill-down
- `by region, total: sales + tax` — group by a column and a computed expression
- `select avg salary, min id by department` — aggregations per group; <kbd>Enter</kbd> on a row drills into the rows behind it

By uses the same comma-separated list and `name : expression` rules as
select. Aggregation functions (`avg`, `min`, `max`, `count`, `sum`, `std`,
`med`, `nunique`, `var`, `dev`) can be written `fn[expr]` or `fn expr`;
brackets are optional. `wavg` goes between its operands: `w wavg x`.

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
| Arithmetic | `+` `-` `*` `/` `%` (`/` and `%` both divide; `%` is not modulo, `mod` is) |
| Equal, not equal | `=`, `!=`, `<>` (same as `!=`) |
| Ordering | `<` `>` `<=` `>=` |
| Coalesce | `^` — first non-null, left to right; `a^b^c` = coalesce(a, b, c), binding right-to-left as `a^(b^c)` |
| Numbers | `42`, `3.14` |
| Strings | `"hello"`, `\"` for an embedded quote |
| Date literals | `2021.01.01` (YYYY.MM.DD) |
| Timestamp literals | `2021.01.15T14:30:00.123456` (YYYY.MM.DDTHH:MM:SS[.fff...]); fractional-second digits set precision: 1–3 = ms, 4–6 = μs, 7–9 = ns |

Either side of a comparison can be a column, a literal or an expression:
`where a = 10`, `where created_at.date > other_date_col`.

## Word operators

q's infix words, parsed right-to-left like every other operator.

| Operator | Result | Example |
|---|---|---|
| `x in [a, b, c]` | True where `x` equals one of the values; the right side is a bracketed list | `select total: sum n by name where name in ["Emma", "Jennifer", "Olivia"]` |
| `x like "pattern"` | True where the whole value matches: `*` is any run of characters, `?` one character; case-sensitive | `select restaurant, item where item like "*Chicken*"` |
| `size xbar x` | `x` rounded down to a multiple of `size`, for buckets; a whole-number size keeps integers integral | `select trips: count fare_amount by b: 5 xbar fare_amount` |
| `x mod n` | Remainder, with the sign of `n` (`-7 mod 3` is `2`) | `select dep_time, minute: dep_time mod 100` |
| `w wavg x` | Average of `x` weighted by `w`, an aggregate; pairs where either is null are skipped | `select delay: distance wavg arr_delay by carrier` |

Because evaluation is right-to-left, `x mod 2 in [1]` is `x mod (2 in [1])`.
Write `(x mod 2) in [1]` or `1 = x mod 2`. `not name in ["Mary"]` negates the
whole test.

The words are operators only between two operands. A column named `in` or
`mod` still works on its own or at the start of an expression, and
`col["in"]` always does.

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
| `quarter` | Int8 | Quarter (1–4) |
| `day` | Int8 | Day of month (1–31) |
| `doy` | Int16 | Day of year (1–366) |
| `dow` | Int8 | Day of week (1=Monday … 7=Sunday, ISO) |
| `hour` | Int8 | Hour (0–23); Datetime and Time |
| `minute` | Int8 | Minute (0–59); Datetime and Time |
| `second` | Int8 | Second (0–59); Datetime and Time |
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
| `part[sep, n]` | String | Split on `sep` and take piece `n`, counting from 0; negative counts from the end; past the last piece is null |
| `slice[start, len]` | String | `len` characters from `start` (0-based; negative counts from the end); without `len`, to the end |
| `replace[from, to]` | String | Every `from` replaced with `to`, literally |
| `strip` | String | Leading and trailing whitespace removed |
| `to_date["fmt"]` | Date | Parse with a chrono format such as `"%Y%m%d"`; without a format, Polars infers it |
| `to_datetime["fmt"]` | Datetime | As `to_date`, for date and time: `"%Y-%m-%d %H:%M"` |

`part`, `slice`, `replace`, `strip`, `to_date` and `to_datetime` also work on
number and date columns, read as their text: NOAA's `DATE` parses whether it
was read as `20240101` text or as an integer. A value that does not parse
becomes null.

### Number and conversion accessors

| Accessor | Result | Description |
|---|---|---|
| `round[n]` | Number | Round to `n` decimals, halves away from zero; `round` alone rounds to a whole number |
| `int` | Int64 | Convert; text that is not a whole number becomes null |
| `float` | Float64 | Convert; text that is not a number becomes null |
| `str` | String | Convert to text |

Accessors chain left to right: `FT.part["–", 0].int`. Arguments are
literals, quoted text or numbers, and a wrong number of them is an error
naming the accessor. To apply an accessor to an aggregate or an expression,
wrap it in parentheses: `(avg dep_delay).round[1]`.

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
| `std` | `stddev`, `dev` | Standard deviation (sample) | `select dev dep_delay by origin` |
| `var` | — | Variance (sample) | `select var dep_delay by origin` |
| `nunique` | — | Count of distinct values | `select stations: nunique ID by ELEMENT` |
| `wavg` | — | Weighted average, written `w wavg x` | `select delay: distance wavg arr_delay by carrier` |
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
| `sqrt` | Square root | `select sd: sqrt var dep_delay by origin` |
| `log` | Natural logarithm | `select year, log_n: (log n).round[2] where name = "Emma"` |
| `exp` | e raised to the value | `select exp[x]` |

`var`, `dev` and `std` divide by n − 1, where q's `var` and `dev` divide by n.

## Examples on the built-in datasets

Each runs as written on the public dataset of that name on the home screen.

| Dataset | Query |
|---|---|
| NYC yellow taxis | `select trips: count VendorID by tpep_pickup_datetime.hour` |
| NYC yellow taxis | `select trips: count fare_amount by b: 5 xbar fare_amount where fare_amount > 0, fare_amount < 100` |
| Premier League | `select home: FT.part["–", 0].int, away: FT.part["–", 1].int` |
| Premier League | `select d: Date.replace["(P)", ""].to_date["%a %b %d %Y"]` |
| Premier League | `select matches: count Round by m: Date.replace["(P)", ""].to_date["%a %b %d %Y"].month` |
| NOAA weather (`by_year/YEAR=2024`) | `select day: DATE.to_date["%Y%m%d"], high: DATA_VALUE / 10 where ID = "USW00094728", ELEMENT = "TMAX"` |
| NOAA weather (`by_year/YEAR=2024`) | `select stations: nunique ID by ELEMENT` |
| NOAA weather (`by_year/YEAR=2024`) | `select stations: nunique ID by country: ID.slice[0, 2] where ELEMENT = "TMAX"` |
| Baby names | `select total: sum n by name where name in ["Emma", "Jennifer", "Olivia"]` |
| Baby names | `select total: sum n by decade: 10 xbar year where name = "Jennifer"` |
| NYC flights | `select mean_delay: (avg dep_delay).round[1] by hour` |
| NYC flights | `select distinct carrier, origin` |
| NYC flights | `select planes: nunique tailnum by carrier` |
| Food nutrition | `select items: count item by restaurant where item like "*Chicken*"` |

