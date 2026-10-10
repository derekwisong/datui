# Query syntax

The grammar of q, typed on the command line (<kbd>:</kbd>, then `q:`). datui's
q is a subset of the q language and evaluates right to left. [Query data](../user-guide/querying-data.md)
walks through it. Every example below runs on the public dataset its block
names; open that dataset from **Example datasets** on the home screen.

q and kdb+ are trademarks of KX Systems. datui is not affiliated with or endorsed by KX.

## Structure of a query

Each part in brackets is optional; replace it with a list of expressions:

```q,template
select [columns] [by group_columns] [where conditions]
```

| Clause | Role |
|---|---|
| `select` | Required. Alone it means all columns; otherwise a comma-separated list of column expressions |
| `by` | Optional. Grouping and aggregation |
| `from df` | Optional. The table on screen, as in q and like SQL's `FROM df`; no other name |
| `where` | Optional. Filtering |

Use clauses in the order shown, at most once each. Misplaced or repeated
clauses and extra tokens after an expression are errors.

## The `:` assignment (aliasing)

`name : expression` names an expression. The left side is the new column or
group name, an identifier (`total`) or `col["name with spaces"]`; the right
side is any expression (column reference, literal, arithmetic, function call).

```q,dataset=flights,network
select carrier, flight, gain: dep_delay - arr_delay
select route: dest, flight
select flights: count flight by airline: carrier, long: distance > 1000
```

Assignment works in both select and by. In by it defines computed group keys
or renames.

## Columns with spaces in their names

Identifiers cannot contain spaces. For columns (or aliases) with spaces, use
`col["..."]` with a quoted string, or `col[identifier]` for a name without
spaces. The same syntax works in select, by and where.

```q,dataset=football,network
select col["Team 1"], col["Team 2"], FT
select home: col["Team 1"]
```

A column named like a function (`count`, `log`, `var`) is read as the
function when something follows it, so `select log + 1` is `log(+1)`. Write
`col["log"] + 1`.

## Right-to-left expression parsing

There is no operator precedence. Expressions are parsed right-to-left: the
leftmost binary operator is the root, and everything to its right is parsed
first as a unit.

- `a + b * c` → `a + (b * c)`
- `a * b + c` → `a * (b + c)`, not `(a * b) + c`
- `(a + b) * 2 > 100` → `(a + b) * (2 > 100)`; write the comparison first,
  `100 < (a + b) * 2`
- `a > 5 & b < 3` → `a > (5 & (b < 3))`; parenthesize the left comparison,
  `(a > 5) & b < 3`

Put the operation you want done first on the right, or use `()` to override
grouping:

```q,dataset=flights,network
select gain: (dep_delay - arr_delay) * 60
select carrier, flight where (dep_delay > 60) | arr_delay > 60
```

## And, or

`&` (or the word `and`) and `|` (or `or`) are operators like `+` or `>`: no
precedence, right to left, in select, by and where. Only the rightmost
comparison can go without parentheses.

| Written | Means |
|---|---|
| `(a > 5) & b < 3` | `(a > 5)` and `(b < 3)` |
| `a > 5 & b < 3` | `a > (5 & (b < 3))` |
| `(a > 5) \| (b < 3) & c = 1` | `(a > 5)` or (`(b < 3)` and `(c = 1)`) |
| `not (a > 5) & b < 3` | not (`(a > 5)` and `(b < 3)`) |

On two booleans they are and and or. On numbers they are q's lesser and
greater: `&` is the smaller of the two, `|` the larger, row by row. A boolean
with a number counts as 0 or 1 in the number's type, and a whole number next
to an integer column is an integer, so `a & 5` stays an integer. As in q, any
value exceeds a null: `&` with a null is null, and `|` with a null is the
other side.

| Operands | `x & y` | `x \| y` | With a null |
|---|---|---|---|
| Two booleans | And | Or | `false & null` is null; `false \| null` is `false` |
| Numbers, or a boolean with a number | Smaller | Larger | `null & 3` is null; `null \| 3` is `3` |
| Two dates or times, or two strings | Earlier, or first in order | Later, or last in order | As for numbers |
| A string or a date with anything else | Error | Error | |

In a where condition, a null drops the row either way.

```q,dataset=flights,network
select carrier, flight where (origin = "JFK") & dep_delay > 60
select carrier, flight where (dep_delay > 60) or (arr_delay > 60) and distance > 1000
select flight, late: 0 | dep_delay, capped: 120 & arr_delay
```

`0 | dep_delay` floors early departures at 0, and a cancelled flight's null
delay becomes 0 too; `120 & arr_delay` caps delays at two hours and keeps a
missing delay null.

`not` of a number is whether it is 0: `not x` is `x = 0`.

`&&` and `||` are errors: write `&` or `|`.

## Select clause

- `select` — all columns, no expressions
- `select a, b, c` — those columns or expressions, in order, separated by `,`
- `select a, b: x + y, c` — columns and aliased expressions
- `select distinct carrier, origin` — only the distinct rows of the result;
  `select distinct` alone drops duplicate rows. A column named `distinct` is
  `col["distinct"]`, or plain `distinct` before `,` `:` `.` or an operator

## By clause (grouping and aggregation)

- `by origin, dest` — group by those columns; the other columns become list columns, and <kbd>Enter</kbd> on a row drills down
- `by carrier, long: distance > 1000` — group by a column and a computed expression
- `select avg dep_delay, min dep_delay by carrier` — aggregations per group; <kbd>Enter</kbd> on a row drills down to the rows behind it

By uses the same comma-separated list and `name : expression` rules as
select. Aggregation functions (`avg`, `min`, `max`, `count`, `sum`, `std`,
`med`, `nunique`, `var`, `dev`) can be written `fn[expr]` or `fn expr`;
brackets are optional. `wavg` goes between its operands: `w wavg x`.

An unaliased aggregate of a single column is named `{fn}_{column}`, so
`select avg dep_delay, max dep_delay by carrier` yields `avg_dep_delay` and
`max_dep_delay`; an explicit alias (`total: sum[distance]`) overrides it.

## Where clause

The where clause is a list of conditions separated by `,`. Each one filters
the rows the ones before it kept, in order, so an aggregate in a later
condition is over those rows only. Within a condition, combine tests with `&`
and `|` ([above](#and-or)).

| Written | Means |
|---|---|
| `where a > 10, b < 2` | Rows with `a > 10`, then of those, rows with `b < 2` |
| `where (a > 10) \| a < 5` | `(a > 10)` or `(a < 5)` |
| `where a > 10 \| a < 5` | `a > (10 \| (a < 5))`, which is `a > 10` |
| `where (a > 10) \| a < 5, b = 2` | (`(a > 10)` or `(a < 5)`), then `b = 2` |

Flights later than average, then of those, longer than their average:

```q,dataset=flights,network
select carrier, flight, dep_delay, distance where dep_delay > avg dep_delay, distance > avg distance
```

The second `avg distance` is over the late flights only. Written as one
condition, `(dep_delay > avg dep_delay) & distance > avg distance`, both
averages are over every row.

A `,` inside parentheses is an error: in q it joins lists, which datui does
not support. Combine the conditions with `&` or `|` instead.
A `,` with no condition on one side, as in `where a > 1,` or `,,`, is an
error too.

The where clause takes conditions only: no `name: expression` assignment.

## Operators and literals

| Kind | Syntax |
|---|---|
| Arithmetic | `+` `-` `*` `/` `%` (`/` and `%` both divide; `%` is not modulo, `mod` is) |
| Equal, not equal | `=`, `!=`, `<>` (same as `!=`) |
| Ordering | `<` `>` `<=` `>=` |
| And, or | `&` `and`, `\|` `or`: and/or on booleans, smaller/larger on numbers; see [And, or](#and-or) |
| Coalesce | `^` — first non-null, left to right; `a^b^c` = coalesce(a, b, c), binding right-to-left as `a^(b^c)` |
| Numbers | `42`, `3.14` |
| Strings | `"hello"`, `\"` for an embedded quote |
| Date literals | `2021.01.01` (YYYY.MM.DD) |
| Timestamp literals | `2021.01.15T14:30:00.123456` (YYYY.MM.DDTHH:MM:SS[.fff...]); fractional-second digits set precision: 1–3 = ms, 4–6 = μs, 7–9 = ns |

A timestamp literal compared with a column that has a time zone is read as a
clock time in that zone. A clock time repeated when clocks fall back means its
first instant.

Quoted text is a string, never a date: `where d = "2024.01.01"` on a date
column is an error that names the literal to write, here `2024.01.01`. Time and
duration columns have no literal; compare `t.hour`, `t.minute` or `t.second`
with a number.

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

The words are operators only between two operands. A column named `in`,
`mod`, `and` or `or` still works on its own or at the start of an expression,
and `col["in"]` always does.

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

`time_hour` in NYC flights is a UTC datetime:

```q,dataset=flights,network
select day: time_hour.date
select time_hour.date, time_hour.year
select flight, time_hour.time
select time_hour, time_hour.month, time_hour.dow by time_hour.year
select delay: arr_delay^dep_delay
select tailnum.len, tailnum.upper, time_hour.format["%Y-%m"]
select where time_hour.date > 2013.06.30
select where time_hour.month = 12, time_hour.dow = 1
select where dest.ends_with["A"]
select where null dep_time
select where not null dep_time
```

`tpep_pickup_datetime` in NYC yellow taxis is a datetime with no time zone:

```q,dataset=taxis,network
select where tpep_pickup_datetime > 2025.01.15T14:30:00.123456
```

## Functions

Functions are used for aggregation (typically in select with by) and for
logic in where. Write `fn[expr]` or `fn expr`; brackets are optional.

### Aggregation functions

| Function | Aliases | Description | Example |
|---|---|---|---|
| `avg` | `mean` | Average | `select avg[dep_delay] by carrier` |
| `min` | — | Minimum | `select min[dep_delay] by origin` |
| `max` | — | Maximum | `select max[distance] by carrier` |
| `count` | — | Count of non-null values | `select count[dep_time] by origin` |
| `sum` | — | Sum | `select sum[distance] by month` |
| `first` | — | First value in group | `select first[dep_time] by day` |
| `last` | — | Last value in group | `select last[dep_time] by day` |
| `std` | `stddev`, `dev` | Standard deviation (sample) | `select dev dep_delay by origin` |
| `var` | — | Variance (sample) | `select var dep_delay by origin` |
| `nunique` | — | Count of distinct values | `select planes: nunique tailnum by carrier` |
| `wavg` | — | Weighted average, written `w wavg x` | `select delay: distance wavg arr_delay by carrier` |
| `med` | `median` | Median | `select med[air_time] by dest` |
| `len` | `length` | String length (chars) | `select len[tailnum]` |

### Logic functions

| Function | Description | Example |
|---|---|---|
| `not` | Logical negation; of a number, whether it is 0 | `where not[origin = "JFK"]`, `where not dep_delay > 10` |
| `null` | Is null | `where null dep_time`, `where null[dep_time]` |
| `not null` | Is not null | `where not null dep_time` |

### Scalar functions

| Function | Description | Example |
|---|---|---|
| `len` / `length` | String length | `select len[tailnum]`, `where len[tailnum] > 5` |
| `upper` | Uppercase string | `select upper[tailnum]`, `where lower[origin] = "jfk"` |
| `lower` | Lowercase string | `select lower[carrier]` |
| `abs` | Absolute value | `select abs[dep_delay]` |
| `floor` | Numeric floor | `select floor[distance % 100]` |
| `ceil` / `ceiling` | Numeric ceiling | `select ceil[distance % 100]` |
| `sqrt` | Square root | `select sd: sqrt var dep_delay by origin` |
| `log` | Natural logarithm | `select year, log_n: (log n).round[2] where name = "Emma"` |
| `exp` | e raised to the value | `select exp[1]` |

`var`, `dev` and `std` divide by n − 1, where q's `var` and `dev` divide by n.

## Examples on the built-in datasets

NYC yellow taxis:

```q,dataset=taxis,network
select trips: count VendorID by tpep_pickup_datetime.hour
select trips: count fare_amount by b: 5 xbar fare_amount where fare_amount > 0, fare_amount < 100
```

Premier League:

```q,dataset=football,network
select home: FT.part["–", 0].int, away: FT.part["–", 1].int
select d: Date.replace["(P)", ""].strip.to_date["%a %b %d %Y"]
select matches: count Round by m: Date.replace["(P)", ""].strip.to_date["%a %b %d %Y"].month
```

US baby names:

```q,dataset=names,network
select total: sum n by name where name in ["Emma", "Jennifer", "Olivia"]
select total: sum n by decade: 10 xbar year where name = "Jennifer"
select year, log_n: (log n).round[2] where name = "Emma"
```

NYC flights:

```q,dataset=flights,network
select mean_delay: (avg dep_delay).round[1] by hour
select distinct carrier, origin
select planes: nunique tailnum by carrier
select delay: distance wavg arr_delay by carrier
select dep_time, minute: dep_time mod 100
select sd: sqrt var dep_delay by origin
```

Food nutrition:

```q,dataset=food,network
select items: count item by restaurant where item like "*Chicken*"
select restaurant, item where item like "*Chicken*"
```

Palmer penguins:

```q,dataset=penguins,network
select mean_mass_g: avg body_mass_g by species
select species, island, bill_ratio: (bill_length_mm % bill_depth_mm).round[2] where not null bill_length_mm
```
