# Query Syntax Reference

This document details the syntax of the Datui query language. For examples and typical workflows, see [Querying Data](../user-guide/querying-data.md).

## Structure of a query

A query has the form:

```
select [columns] [by group_columns] [where conditions]
```

- **`select`** — Required. Starts every query. May be followed by nothing (select all columns), or a comma‑separated list of column expressions.
- **`by`** — Optional. Grouping and aggregation. Everything after `by` up to `where` (if present) is the group specification.
- **`where`** — Optional. Filtering. Everything after `where` is the filter expression.

Clause order is fixed: `select` → `by` → `where`. The parser splits on the keywords `where` and `by` (respecting parentheses and brackets), so you cannot reorder or repeat clauses.

---

## The `:` assignment (aliasing)

Use **`:`** to name an expression. The form is **`name : expression`**:

- **Left of `:`** — The new column or group name. Must be an identifier (e.g. `total`) or `col["name with spaces"]`.
- **Right of `:`** — The expression (column reference, literal, arithmetic, function call, etc.).

Examples:

```
select a, b, sum_ab: a + b
select renamed: col["Original Name"]
by region_name: region, total: sales + tax
```

Assignment is supported in both the **select** and **by** clauses. In **by**, it defines computed group keys or renames (e.g. `region_name: region`, `total: sales + tax`). Expressions may use column references, arithmetic, literals, and function calls.

---

## Columns with spaces in their names

Identifiers cannot contain spaces. For columns (or aliases) with spaces, use **`col["..."]`** or **`col[identifier]`**:

```
select col["First Name"], col["Last Name"]
select no_spaces: col["name with spaces"]
```

Inside the brackets use either a **quoted string** (`"name with spaces"`) or a single **identifier** (no spaces). The same syntax works in **select**, **by**, and **where**.

---

## Right‑to‑left expression parsing

Expressions are parsed **right‑to‑left**: the **leftmost** binary operator is the root, and the **right** subexpression is parsed first (so it effectively binds tighter).

### What this means

- **`a + b * c`** → parsed as **`a + (b * c)`** (multiplication binds tighter).
- **`a * b + c`** → parsed as **`a * (b + c)`** (`*` is leftmost; the right subexpression `b + c` is parsed as a unit).

So “higher‑precedence”‑style grouping happens when you put those operations **on the right**. You can often avoid parentheses by ordering:

```
select x, y: a * b + c    →  a * (b + c)
select x, y: a + b * c    →  a + (b * c)
```

### Using parentheses

Use **`()`** to override grouping:

```
select (a + b) * c
select a, b where (x > 1) | (y < 0)
```

Parentheses also matter for **`,`** and **`|`** in **where**: splitting on comma and pipe respects nesting, so you can wrap ORs (or more complex logic) in `()` and then combine with commas. See [Where clause: `,` and `|`](#where-clause--and-).

---

## Select clause

- **`select`** alone — All columns, no expressions.
- **`select a, b, c`** — Those columns (and/or expressions), in order.
- **`select a, b: x + y, c`** — Columns and aliased expressions.

Columns are separated by **`,`**. Each item is either a plain expression or **`alias : expression`**. The same expression rules (arithmetic, `col[]`, functions, etc.) apply everywhere.

---

## By clause (grouping and aggregation)

- **`by col1, col2`** — Group by those columns. Non‑group columns become list columns; the UI supports drill‑down.
- **`by region, total: sales + tax`** — Group by `region` and a computed expression (e.g. arithmetic).
- **`select avg salary, min id by department`** — Aggregations per group.

**By** uses the same comma‑separated list and **`name : expression`** rules as **select**. Aggregation functions (**`avg`**, **`min`**, **`max`**, **`count`**, **`sum`**, **`std`**, **`med`**) can be written as **`fn[expr]`** or **`fn expr`** (brackets optional).

---

## Where clause: `,` and `|`

The **where** clause combines conditions using two separators:

- **`,`** — **AND**. Each comma‑separated segment is one ANDed condition.
- **`|`** — **OR**. Within a single comma‑separated segment, **`|`** separates alternatives that are ORed together.

Parsing order:

1. Split the **where** part on **`,`** (respecting `()` and `[]`). Each segment is ANDed.
2. Within each segment, split on **`|`**. The pieces are ORed.

So:

- **`where a > 10, b < 2`** → `(a > 10) AND (b < 2)`.
- **`where a > 10 | a < 5`** → `(a > 10) OR (a < 5)`.
- **`where a > 10 | a < 5, b = 2`** → `(a > 10 OR a < 5) AND (b = 2)`.

### Interaction of `,` and `|`

**,** has broader scope than **|**: it splits the where clause into top‑level AND terms. **|** only groups within one of those terms. So:

- **`A, B | C`** = **`A AND (B OR C)`**.
- **`A | B, C | D`** = **`(A OR B) AND (C OR D)`**.

To express **(A or B) and (C or D)**, use **`(A)|(B), (C)|(D)`**. Parentheses keep **`|`** inside each AND term. For more complex logic, use **`()`** to group OR subexpressions, then separate those groups with **`,`**.

---

## Comparison operators

In **where** conditions, each comparison operator compares a left-hand expression (often a column) to a right-hand expression (column, literal, or expression).

| Operator | Meaning | Example |
|----------|---------|---------|
| **`=`** | Equal | `where a = 10` |
| **`!=`** | Not equal | `where a != 0` |
| **`<>`** | Not equal (same as **`!=`**) | `where a <> 0` |
| **`<`** | Less than | `where a < 100` |
| **`>`** | Greater than | `where a > 5` |
| **`<=`** | Less than or equal | `where a <= 10` |
| **`>=`** | Greater than or equal | `where a >= 1` |

---

## Operators and literals

- **Arithmetic**: **`+`** **`-`** **`*`** **`%`** ( **`%`** is division, not modulo).
- **Comparison**: See [Comparison operators (where clauses)](#comparison-operators) above.
- **Literals**: Numbers (**`42`**, **`3.14`**), strings (**`"hello"`**, **`\"`** for embedded quotes), date literals (**`2021.01.01`** in YYYY.MM.DD format), timestamp literals (**`2021.01.15T14:30:00.123456`** in YYYY.MM.DDTHH:MM:SS[.fff...] format; fractional seconds set precision: 1–3 digits = ms, 4–6 = μs, 7–9 = ns).
- **Coalesce**: **`^`** — first non-null from left to right. `a^b^c` = coalesce(a, b, c). Right-to-left binding: `a^b^c` = `a^(b^c)`.

---

## Date and datetime accessors

For columns of type **Date** or **Datetime**, use **dot notation** to extract components:

```
column_ref.accessor
```

### Datetime accessors

Apply to **Datetime** columns (with or without timezone):

| Accessor | Result | Description |
|----------|--------|-------------|
| **`date`** | Date | Date part (year-month-day) |
| **`time`** | Time | Time part (Polars Time type) |
| **`year`** | Int32 | Year |
| **`month`** | Int8 | Month (1–12) |
| **`week`** | Int8 | Week number |
| **`day`** | Int8 | Day of month (1–31) |
| **`dow`** | Int8 | Day of week (1=Monday … 7=Sunday, ISO) |
| **`month_start`** | Datetime/Date | First day of month at midnight |
| **`month_end`** | Datetime/Date | Last day of month |
| **`format["fmt"]`** | String | Format as string (chrono strftime, e.g. `"%Y-%m"`) |

### Date accessors

Apply to **Date** columns:

| Accessor | Result | Description |
|----------|--------|-------------|
| **`year`** | Int32 | Year |
| **`month`** | Int8 | Month (1–12) |
| **`week`** | Int8 | Week number |
| **`day`** | Int8 | Day of month |
| **`dow`** | Int8 | Day of week (1=Monday … 7=Sunday) |
| **`month_start`** | Date | First day of month |
| **`month_end`** | Date | Last day of month |
| **`format["fmt"]`** | String | Format as string (chrono strftime) |

### String accessors

Apply to **String** columns:

| Accessor | Result | Description |
|----------|--------|-------------|
| **`len`** | Int32 | Character length |
| **`upper`** | String | Uppercase |
| **`lower`** | String | Lowercase |
| **`starts_with["x"]`** | Boolean | True if string starts with `x` |
| **`ends_with["x"]`** | Boolean | True if string ends with `x` |
| **`contains["x"]`** | Boolean | True if string contains `x` |

### Accessor aliases

When you use an accessor on a column, the result is automatically aliased to **`{column}_{accessor}`** (e.g. `timestamp.date` → `timestamp_date`). This avoids duplicate column names and keeps results clear.

### Syntax examples

**Select:**

```
select event_date: timestamp.date
select col["Created At"].date, col["Created At"].year
select name, event_time.time
select order_date, order_date.month, order_date.dow by order_date.year
```

**Where (with date literal YYYY.MM.DD, timestamp literal):**

```
select where created_at.date > other_date_col
select where dt_col.date > 2021.01.01
select where ts_col > 2021.01.15T14:30:00.123456
select where event_ts.month = 12, event_ts.dow = 1
select where city_name.ends_with["lanta"]
select where null col1
select where not null col1
```

**Select (coalesce, string accessors, format):**

```
select a: coln^cola^colb
select name.len, name.upper, dt_col.format["%Y-%m"]
```

---

## Functions

Functions are used for aggregation (typically in **select** with **by**) and for logic in **where**. Write **`fn[expr]`** or **`fn expr`**; brackets are optional.

### Aggregation functions

| Function | Aliases | Description | Example |
|----------|---------|-------------|---------|
| **`avg`** | `mean` | Average | `select avg[price] by category` |
| **`min`** | — | Minimum | `select min[qty] by region` |
| **`max`** | — | Maximum | `select max[amount] by id` |
| **`count`** | — | Count of non-null values | `select count[id] by status` |
| **`sum`** | — | Sum | `select sum[amount] by year` |
| **`first`** | — | First value in group | `select first[value] by group` |
| **`last`** | — | Last value in group | `select last[value] by group` |
| **`std`** | `stddev` | Standard deviation | `select std[score] by group` |
| **`med`** | `median` | Median | `select med[price] by type` |
| **`len`** | `length` | String length (chars) | `select len[name] by category` |

### Logic functions

| Function | Description | Example |
|----------|-------------|---------|
| **`not`** | Logical negation | `where not[a = b]`, `where not x > 10` |
| **`null`** | Is null | `where null col1`, `where null[col1]` |
| **`not null`** | Is not null | `where not null col1` |

### Scalar functions

| Function | Description | Example |
|----------|-------------|---------|
| **`len`** / **`length`** | String length | `select len[name]`, `where len[name] > 5` |
| **`upper`** | Uppercase string | `select upper[name]`, `where upper[city] = "ATLANTA"` |
| **`lower`** | Lowercase string | `select lower[name]` |
| **`abs`** | Absolute value | `select abs[x]` |
| **`floor`** | Numeric floor | `select floor[price]` |
| **`ceil`** / **`ceiling`** | Numeric ceiling | `select ceil[score]` |

---

## Summary

| Topic | Detail |
|-------|--------|
| **Query shape** | `select [cols] [by groups] [where conditions]` |
| **`:`** | `name : expression` in **select** and **by** |
| **Spaces in names** | `col["name"]` or `col[identifier]` |
| **Date/datetime accessors** | `col.date`, `col.time`, `col.year`, `col.month`, `col.week`, `col.day`, `col.dow`, `col.month_start`, `col.month_end`, `col.format["fmt"]` |
| **String accessors** | `col.len`, `col.upper`, `col.lower`, `col.starts_with["x"]`, `col.ends_with["x"]`, `col.contains["x"]` |
| **Literals** | Numbers, strings, `YYYY.MM.DD`, `YYYY.MM.DDTHH:MM:SS[.fff...]` |
| **Coalesce** | `a^b^c` = first non-null of a, b, c |
| **Expressions** | Right‑to‑left; right side binds tighter; use **`()`** to override |
| **Where `,`** | AND between top‑level conditions |
| **Where `\|`** | OR within one top‑level condition |
| **Combining `,` and `\|`** | Top-level `,` = AND; `\|` = OR within a term. e.g. “A, B or C” ⇒ `A AND (B OR C)`. Use `()` to group. |
