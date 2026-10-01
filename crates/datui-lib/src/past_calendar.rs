//! Polars operations that panic on a date or datetime past the calendar's range
//! (a sentinel like `i64::MIN + 1` microseconds): a cast to text, `dt.to_string`,
//! and the date parts that go through a calendar date (`dt.date` with a zone,
//! `dt.month_start`, `dt.truncate`, ...). Each panics for the whole column, even
//! when one row holds such a value.
//!
//! datui cannot patch Polars, so it gives these operations the values they can
//! take: text is Polars' own, with a value past the calendar written as its stored
//! number ([`crate::exact::out_of_range`]), as the table shows it; a date part of
//! such a value is null, as Polars' own `dt.year` makes it. Everything here is
//! elementwise, so a streamed plan stays streamed, and costs a min and a max per
//! batch when no value is past the calendar.

use crate::exact::{calendar_without_out_of_range, stored_out_of_range};
use polars::chunked_array::cast::CastOptions;
use polars::prelude::*;

/// Whether a value of `dtype` can lie past the calendar: a date, or a datetime
/// in milliseconds or microseconds. A count of nanoseconds only spans 1677..2262.
pub fn can_leave_calendar(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Date | DataType::Datetime(TimeUnit::Milliseconds | TimeUnit::Microseconds, _)
    )
}

/// A date or datetime column as `text` writes it, with a value past the calendar,
/// on which `text` would panic, written as its stored number instead.
pub fn text_or_stored(
    series: &Series,
    text: impl Fn(&Series) -> PolarsResult<StringChunked>,
) -> PolarsResult<Series> {
    let dtype = series.dtype();
    let text = match calendar_without_out_of_range(series)? {
        // The rest written as usual, these filled in after.
        Some(shown) => {
            let stored = series.to_physical_repr().cast(&DataType::Int64)?;
            text(&shown)?
                .iter()
                .zip(stored.i64()?.iter())
                .map(|(text, v)| match text {
                    Some(text) => Some(text.to_string()),
                    None => v.and_then(|v| stored_out_of_range(dtype, v)),
                })
                .collect::<StringChunked>()
        }
        None => text(series)?,
    };
    Ok(text.with_name(series.name().clone()).into_series())
}

/// `series` cast to String as Polars casts it, with a date or datetime past the
/// calendar as its stored number.
pub fn cast_text(series: &Series, options: CastOptions) -> PolarsResult<Series> {
    if !can_leave_calendar(series.dtype()) {
        return series.cast_with_options(&DataType::String, options);
    }
    text_or_stored(series, |s| Ok(s.cast(&DataType::String)?.str()?.clone()))
}

/// Polars' `dt.to_string(format)`, with a date or datetime past the calendar as
/// its stored number.
pub fn formatted(series: &Series, format: &str) -> PolarsResult<Series> {
    if !can_leave_calendar(series.dtype()) {
        return TemporalMethods::to_string(series, format);
    }
    text_or_stored(series, |s| {
        Ok(TemporalMethods::to_string(s, format)?.str()?.clone())
    })
}

fn text_field(_: &Schema, field: &Field) -> PolarsResult<Field> {
    Ok(Field::new(field.name().clone(), DataType::String))
}

/// [`cast_text`] in a plan: `expr.cast(String)` that never panics.
pub fn text_expr(expr: Expr, options: CastOptions) -> Expr {
    expr.map_with_fmt_str(
        move |c| cast_text(c.as_materialized_series(), options).map(Column::from),
        text_field,
        "past_calendar_text",
    )
}

/// [`formatted`] in a plan: `expr.dt().to_string(format)` that never panics.
pub fn format_expr(expr: Expr, format: String) -> Expr {
    expr.map_with_fmt_str(
        move |c| formatted(c.as_materialized_series(), &format).map(Column::from),
        text_field,
        "past_calendar_format",
    )
}

/// `expr` with each date or datetime past the calendar as null, ahead of a date
/// part that would panic on it. Any other value, and any other type, as it is.
pub fn calendar_expr(expr: Expr) -> Expr {
    expr.map_with_fmt_str(
        |c| {
            Ok(calendar_without_out_of_range(c.as_materialized_series())?
                .map(Column::from)
                .unwrap_or(c))
        },
        |_, field| Ok(field.clone()),
        "past_calendar_null",
    )
}

/// Whether a date part goes through a calendar date, and so panics on a value
/// past the calendar. The rest read or relabel the stored number.
fn reads_calendar(function: &TemporalFunction) -> bool {
    !matches!(
        function,
        TemporalFunction::TimeStamp(_)
            | TemporalFunction::CastTimeUnit(_)
            | TemporalFunction::WithTimeUnit(_)
            | TemporalFunction::ConvertTimeZone(_)
    )
}

/// `expr` with every operation that would panic on a date past the calendar
/// replaced by one that does not: casts to text and `concat_str` go through
/// [`text_expr`], `dt.to_string` through [`format_expr`], and the date parts read
/// from [`calendar_expr`]. With `schema`, the one `expr` is evaluated against,
/// only operations on a date or ms/us datetime are replaced and every other plan
/// stays as Polars built it; without it, each is replaced, and the replacement
/// casts any other type as Polars would. A date that meets text in a coalesce or
/// a when/then/otherwise, which Polars casts to text itself, goes through
/// [`text_expr`] too, but only with `schema`, which says the result is text.
pub fn guard_expr(expr: Expr, schema: Option<&Schema>) -> Expr {
    let may_leave = |e: &Expr| match (e, schema) {
        (Expr::Literal(_), _) => false,
        (e, Some(schema)) => e
            .to_field(schema)
            .map_or(true, |f| can_leave_calendar(f.dtype())),
        (_, None) => true,
    };
    let as_text = |e: Expr| {
        if may_leave(&e) {
            text_expr(e, CastOptions::NonStrict)
        } else {
            e
        }
    };
    let gives_text = |e: &Expr| {
        schema.is_some_and(|schema| {
            e.to_field(schema)
                .is_ok_and(|f| f.dtype() == &DataType::String)
        })
    };
    expr.map_expr(|e| match e {
        e @ (Expr::Ternary { .. }
        | Expr::Function {
            function: FunctionExpr::Coalesce,
            ..
        }) if gives_text(&e) => match e {
            Expr::Ternary {
                predicate,
                truthy,
                falsy,
            } => Expr::Ternary {
                predicate,
                truthy: Arc::new(as_text(Arc::unwrap_or_clone(truthy))),
                falsy: Arc::new(as_text(Arc::unwrap_or_clone(falsy))),
            },
            Expr::Function { input, function } => Expr::Function {
                input: input.into_iter().map(as_text).collect(),
                function,
            },
            e => e,
        },
        Expr::Cast {
            expr,
            dtype,
            options,
        } if dtype.as_literal() == Some(&DataType::String) && may_leave(&expr) => {
            text_expr(Arc::unwrap_or_clone(expr), options)
        }
        Expr::Function {
            mut input,
            function: FunctionExpr::TemporalExpr(function),
        } if input.first().is_some_and(may_leave) => match function {
            TemporalFunction::ToString(format) => format_expr(input.swap_remove(0), format),
            function => {
                if reads_calendar(&function) {
                    input[0] = calendar_expr(input[0].clone());
                }
                Expr::Function {
                    input,
                    function: FunctionExpr::TemporalExpr(function),
                }
            }
        },
        // `concat_str` exists only with SQL, which is what makes one.
        #[cfg(feature = "sql")]
        Expr::Function {
            input,
            function:
                function @ FunctionExpr::StringExpr(
                    StringFunction::ConcatHorizontal { .. } | StringFunction::ConcatVertical { .. },
                ),
        } => Expr::Function {
            input: input.into_iter().map(as_text).collect(),
            function,
        },
        e => e,
    })
}

/// Whether [`guard_expr`] could replace anything in `expr`.
#[cfg(feature = "sql")]
fn guards(expr: &Expr) -> bool {
    expr.into_iter().any(|e| match e {
        Expr::Cast { dtype, .. } => dtype.as_literal() == Some(&DataType::String),
        Expr::Ternary { .. } => true,
        Expr::Function { function, .. } => matches!(
            function,
            FunctionExpr::TemporalExpr(_)
                | FunctionExpr::Coalesce
                | FunctionExpr::StringExpr(
                    StringFunction::ConcatHorizontal { .. } | StringFunction::ConcatVertical { .. }
                )
        ),
        _ => false,
    })
}

/// Whether a node of `plan` holds an expression [`guard_expr`] could replace.
#[cfg(feature = "sql")]
fn holds_guards(plan: &DslPlan) -> bool {
    plan.into_iter().any(|node| match node {
        DslPlan::Filter { predicate, .. } => guards(predicate),
        DslPlan::Select { expr, .. } => expr.iter().any(guards),
        DslPlan::HStack { exprs, .. } => exprs.iter().any(guards),
        DslPlan::Sort { by_column, .. } => by_column.iter().any(guards),
        DslPlan::GroupBy {
            keys,
            predicates,
            aggs,
            ..
        } => keys.iter().chain(predicates).chain(aggs).any(guards),
        DslPlan::Join {
            left_on,
            right_on,
            predicates,
            ..
        } => left_on.iter().chain(right_on).chain(predicates).any(guards),
        // Stacked on text, a date is cast to text by Polars.
        DslPlan::Union { args, .. } => args.to_supertypes,
        _ => false,
    })
}

/// `input` of a union whose columns are `union`, with each date the union stacks on
/// text, which Polars would cast to text itself, through [`text_expr`] first. A
/// diagonal union matches columns by name, any other by position.
#[cfg(feature = "sql")]
fn union_text(input: &mut DslPlan, union: &Schema, diagonal: bool) {
    let Ok(own) = LazyFrame::from(input.clone()).collect_schema() else {
        return;
    };
    let texts: Vec<Expr> = own
        .iter()
        .enumerate()
        .filter(|(i, (name, dtype))| {
            let stacked = if diagonal {
                union.get(name.as_str())
            } else {
                union.get_at_index(*i).map(|(_, dtype)| dtype)
            };
            can_leave_calendar(dtype) && stacked == Some(&DataType::String)
        })
        .map(|(_, (name, _))| text_expr(Expr::Column(name.clone()), CastOptions::NonStrict))
        .collect();
    if !texts.is_empty() {
        *input = LazyFrame::from(input.clone())
            .with_columns(texts)
            .logical_plan;
    }
}

/// [`guard_expr`] over each expression of `plan`, against the schema of the input
/// it is evaluated on: a SQL statement's plan, whose casts and date functions are
/// Polars' own. Only the parts of the plan holding such an expression are rebuilt.
#[cfg(feature = "sql")]
pub fn guard_plan(plan: &mut DslPlan) {
    if !holds_guards(plan) {
        return;
    }
    // The schema the expressions are evaluated against. When it is unknown every
    // candidate is replaced, which is still right, only less narrow.
    let schema = |input: &DslPlan| LazyFrame::from(input.clone()).collect_schema().ok();
    let guard = |exprs: &mut Vec<Expr>, schema: Option<&Schema>| {
        for expr in exprs.iter_mut().filter(|e| guards(e)) {
            *expr = guard_expr(std::mem::take(expr), schema);
        }
    };
    match plan {
        DslPlan::Filter { input, predicate } if guards(predicate) => {
            *predicate = guard_expr(std::mem::take(predicate), schema(input).as_deref());
        }
        DslPlan::Select { input, expr, .. } if expr.iter().any(guards) => {
            guard(expr, schema(input).as_deref());
        }
        DslPlan::HStack { input, exprs, .. } if exprs.iter().any(guards) => {
            guard(exprs, schema(input).as_deref());
        }
        DslPlan::Sort {
            input, by_column, ..
        } if by_column.iter().any(guards) => {
            guard(by_column, schema(input).as_deref());
        }
        DslPlan::GroupBy {
            input,
            keys,
            predicates,
            aggs,
            ..
        } if keys.iter().chain(&*predicates).chain(&*aggs).any(guards) => {
            let schema = schema(input);
            guard(keys, schema.as_deref());
            guard(predicates, schema.as_deref());
            guard(aggs, schema.as_deref());
        }
        DslPlan::Join {
            input_left,
            input_right,
            left_on,
            right_on,
            predicates,
            ..
        } => {
            if left_on.iter().any(guards) {
                guard(left_on, schema(input_left).as_deref());
            }
            if right_on.iter().any(guards) {
                guard(right_on, schema(input_right).as_deref());
            }
            // Predicates read both sides: guarded whatever their type.
            guard(predicates, None);
        }
        DslPlan::Union { args, .. } if args.to_supertypes => {
            let union = schema(plan);
            if let (Some(union), DslPlan::Union { inputs, args }) = (union, &mut *plan) {
                for input in inputs {
                    union_text(input, &union, args.diagonal);
                }
            }
        }
        _ => {}
    }
    if let DslPlan::IR { dsl, .. } = plan {
        // A plan asked for its schema is wrapped as IR, which would run as converted:
        // guard the plan it came from, and leave the IR behind.
        let mut inner = Arc::unwrap_or_clone(dsl.clone());
        guard_plan(&mut inner);
        *plan = inner;
        return;
    }
    crate::widgets::datatable::for_each_input(plan, &mut guard_plan);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nested_json::tests::calendar;

    /// The rows of `calendar(true)` that are also in `calendar(false)`.
    const IN_RANGE: usize = 7;

    /// `series`' values, as text, past the rows in range: what the table shows.
    fn past_rows(series: &Series) -> Vec<Option<String>> {
        series
            .slice(IN_RANGE as i64, series.len() - IN_RANGE)
            .iter()
            .map(|v| (!v.is_null()).then(|| crate::exact::str_value(&v).into_owned()))
            .collect()
    }

    /// Cast to text, a date or datetime is Polars' own text, and one past the
    /// calendar its stored number, as the table shows it; a nanosecond count is
    /// always in range. `dt.to_string` is the same with its format.
    #[test]
    fn text_is_polars_own_and_a_date_past_the_calendar_its_stored_number() {
        let in_range = calendar(false);
        let past = calendar(true);
        for column in past.columns() {
            let series = column.as_materialized_series();
            let polars = in_range
                .column(column.name())
                .unwrap()
                .as_materialized_series();
            for (text, expected) in [
                (
                    cast_text(series, CastOptions::Strict).unwrap(),
                    polars.cast(&DataType::String).unwrap(),
                ),
                (
                    formatted(series, "%Y/%m/%d").unwrap(),
                    TemporalMethods::to_string(polars, "%Y/%m/%d").unwrap(),
                ),
            ] {
                let name = column.name();
                assert_eq!(text.dtype(), &DataType::String, "{name}");
                assert_eq!(text.name(), name);
                assert!(
                    text.head(Some(IN_RANGE)).equals_missing(&expected),
                    "{name}: {text:?}"
                );
                if can_leave_calendar(series.dtype()) {
                    let shown = past_rows(series);
                    assert!(
                        shown.iter().flatten().all(|s| s.contains("since")),
                        "{name}"
                    );
                    assert_eq!(past_rows(&text), shown, "{name}");
                }
            }
        }
        // Any other type is cast as Polars casts it, strictness and all.
        let numbers = Series::new("n".into(), [Some(1.5f64), None]);
        assert!(
            cast_text(&numbers, CastOptions::NonStrict)
                .unwrap()
                .equals_missing(&numbers.cast(&DataType::String).unwrap())
        );
    }

    /// A date part of a value past the calendar is null, as Polars' own `dt.year`
    /// makes it, where these panicked; values in range give what Polars gives.
    #[test]
    fn date_parts_of_a_date_past_the_calendar_are_null() {
        let in_range = calendar(false).lazy();
        let past = calendar(true).lazy();
        let schema = past.clone().collect_schema().unwrap();
        for (name, dtype) in schema.iter() {
            // A zone moves a nanosecond count at 1677 or 2262 out of its range, and
            // Polars overflows converting back: the result is out of range there.
            if matches!(dtype, DataType::Datetime(TimeUnit::Nanoseconds, Some(_))) {
                continue;
            }
            let c = || col(name.clone());
            let mut parts = vec![
                c().dt().date(),
                c().dt().ordinal_day(),
                c().dt().weekday(),
                c().dt().iso_year(),
            ];
            if !matches!(dtype, DataType::Date) {
                parts.push(c().dt().time());
            }
            // Moved past 1677 or 2262, a nanosecond count overflows in Polars too.
            if can_leave_calendar(dtype) {
                parts.extend([
                    c().dt().month_start(),
                    c().dt().month_end(),
                    c().dt().truncate(lit("1mo")),
                    c().dt().round(lit("1mo")),
                ]);
            }
            if matches!(dtype, DataType::Datetime(..)) && can_leave_calendar(dtype) {
                parts.push(c().dt().truncate(lit("1d")));
            }
            for part in parts {
                let guarded = guard_expr(part.clone(), Some(&schema));
                let out = past.clone().select([guarded]).collect().unwrap();
                let out = out.columns()[0].as_materialized_series();
                let polars = in_range.clone().select([part.clone()]).collect().unwrap();
                let case = format!("{name}: {part:?}");
                assert!(
                    out.head(Some(IN_RANGE))
                        .equals_missing(polars.columns()[0].as_materialized_series()),
                    "{case}"
                );
                if can_leave_calendar(dtype) {
                    assert_eq!(past_rows(out), [None, None], "{case}");
                }
            }
        }
    }

    /// With the schema, only an operation on a date or ms/us datetime changes;
    /// every other plan stays as Polars built it. Without one, each changes.
    #[test]
    fn only_operations_on_dates_change() {
        let schema = Schema::from_iter([
            Field::new("i".into(), DataType::Int64),
            Field::new("s".into(), DataType::String),
            Field::new("ns".into(), DataType::Datetime(TimeUnit::Nanoseconds, None)),
            Field::new("d".into(), DataType::Date),
            Field::new("t".into(), DataType::Datetime(TimeUnit::Microseconds, None)),
        ]);
        let kept = [
            col("i").cast(DataType::String),
            col("s").cast(DataType::String).str().len_chars(),
            col("ns").cast(DataType::String),
            col("ns").dt().month_start(),
            lit(1).cast(DataType::String),
            col("t").dt().timestamp(TimeUnit::Milliseconds),
            col("d").cast(DataType::Int64),
            #[cfg(feature = "sql")]
            concat_str([col("i"), col("s")], "", true),
        ];
        for expr in kept {
            assert_eq!(guard_expr(expr.clone(), Some(&schema)), expr);
        }
        let changed = [
            col("d").cast(DataType::String),
            col("t").cast(DataType::String).str().len_chars(),
            col("t").max().cast(DataType::String),
            col("t").dt().to_string("%Y"),
            col("d").dt().month_start(),
            #[cfg(feature = "sql")]
            concat_str([col("i"), col("t")], "", true),
        ];
        for expr in changed {
            assert_ne!(guard_expr(expr.clone(), Some(&schema)), expr);
            assert_ne!(guard_expr(expr.clone(), None), expr);
        }
        assert_ne!(
            guard_expr(col("i").cast(DataType::String), None),
            col("i").cast(DataType::String)
        );
        // A date met with text in a coalesce or a when/then/otherwise is cast to
        // text by Polars; known only with the schema.
        let one = || col("i").eq(lit(1));
        let kept = [
            coalesce(&[col("d"), col("t")]),
            coalesce(&[col("s"), lit("x")]),
            when(one()).then(col("d")).otherwise(col("t")),
            when(one()).then(col("ns")).otherwise(col("s")),
        ];
        for expr in kept {
            assert_eq!(guard_expr(expr.clone(), Some(&schema)), expr);
        }
        let changed = [
            coalesce(&[col("t"), lit("x")]),
            coalesce(&[col("s"), col("d")]),
            when(one()).then(col("d")).otherwise(col("s")),
            when(one()).then(lit("x")).otherwise(col("t")),
        ];
        for expr in changed {
            assert_ne!(guard_expr(expr.clone(), Some(&schema)), expr);
            assert_eq!(guard_expr(expr.clone(), None), expr);
        }
    }

    /// Met with text in a coalesce, a when/then/otherwise or a union, a date is
    /// Polars' own text in range and its stored number past it, where Polars' own
    /// cast to their common type panicked.
    #[cfg(feature = "sql")]
    #[test]
    fn a_date_met_with_text_is_its_text() {
        let frame = |past: bool| {
            let at = if past { i64::MIN + 1 } else { 1 };
            df!(
                "s" => ["a", "b"],
                "d" => [0, if past { i32::MAX } else { 1 }],
                "t" => [0, at],
            )
            .unwrap()
            .lazy()
            .with_columns([
                col("d").cast(DataType::Date),
                col("t").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
            ])
        };
        let mut ctx = polars_sql::SQLContext::new();
        for sql in [
            "SELECT COALESCE(t, 'x') AS x FROM df",
            "SELECT CASE WHEN s = 'b' THEN d ELSE s END AS x FROM df",
            "SELECT d AS x FROM df UNION ALL SELECT s FROM df",
            "SELECT s AS x FROM df UNION SELECT t FROM df",
        ] {
            for past in [false, true] {
                ctx.register("df", frame(past));
                let polars = ctx.execute(sql).unwrap();
                let mut guarded = polars.clone();
                guard_plan(&mut guarded.logical_plan);
                // A UNION's rows come in any order.
                let values = |lf: LazyFrame| {
                    let df = lf.collect().unwrap();
                    let mut values: Vec<Option<String>> = df
                        .column("x")
                        .unwrap()
                        .str()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(str::to_string))
                        .collect();
                    values.sort();
                    values
                };
                let text = values(guarded);
                if past {
                    assert!(text.iter().flatten().any(|s| s.contains("since")), "{sql}");
                } else {
                    assert_eq!(text, values(polars), "{sql}");
                }
            }
        }
    }
}
