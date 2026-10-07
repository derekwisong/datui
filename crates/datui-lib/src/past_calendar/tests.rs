use super::*;
use crate::export::nested_json::tests::calendar;

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
        // A nanosecond count at 1677 or 2262 moved past its range is null too.
        parts.extend([
            c().dt().month_start(),
            c().dt().month_end(),
            c().dt().truncate(lit("1mo")),
            c().dt().round(lit("1mo")),
        ]);
        if matches!(dtype, DataType::Datetime(..)) {
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

/// Date math on a nanosecond datetime near the ends of its range (1677-09-21,
/// 2262-04-11), where Polars overflowed (#517), is null when it could move the
/// value past them; with a zone, so are the parts read in local time. Every
/// other value gives what Polars gives it, on both engines.
#[test]
fn date_math_near_the_ends_of_the_nanosecond_range_is_null() {
    const DAY: i64 = 86_400_000_000_000;
    // In range, then the ends, 20 days before the top end and a day after the
    // bottom one.
    let stamps = [
        Some(0),
        Some(400 * DAY),
        None,
        Some(i64::MAX),
        Some(i64::MIN + 1),
        Some(i64::MAX - 20 * DAY),
        Some(i64::MIN + DAY),
    ];
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let frame = |rows: &[Option<i64>]| {
        let at = |name: &str, zone: Option<TimeZone>| {
            Series::new(name.into(), rows)
                .cast(&DataType::Datetime(TimeUnit::Nanoseconds, zone))
                .unwrap()
                .into_column()
        };
        DataFrame::new_infer_height(vec![at("n", None), at("z", paris.clone())])
            .unwrap()
            .lazy()
    };
    let schema = frame(&stamps).collect_schema().unwrap();
    let n = || col("n");
    let z = || col("z");
    // Which of the last four rows keep a value.
    let cases = [
        (n().dt().month_start(), [true, false, true, false]),
        // Through its own month's start, before the bottom end.
        (n().dt().month_end(), [false, false, false, false]),
        (n().dt().truncate(lit("1d")), [true, false, true, true]),
        (n().dt().truncate(lit("1mo")), [true, false, true, false]),
        (n().dt().round(lit("1h")), [false, false, true, true]),
        #[cfg(feature = "sql")]
        (n().dt().offset_by(lit("1d")), [false, true, true, true]),
        #[cfg(feature = "sql")]
        (n().dt().offset_by(lit("-1mo")), [true, false, true, false]),
        (n().dt().date(), [true, true, true, true]),
        (n().dt().ordinal_day(), [true, true, true, true]),
        (z().dt().month_start(), [false, false, true, false]),
        (z().dt().month_end(), [false, false, false, false]),
        #[cfg(feature = "sql")]
        (z().dt().offset_by(lit("1d")), [false, false, true, false]),
        (z().dt().date(), [false, false, true, false]),
        (z().dt().time(), [false, false, true, false]),
        (z().dt().ordinal_day(), [false, false, true, false]),
        (z().dt().iso_year(), [false, false, true, false]),
        (z().dt().datetime(), [false, false, true, false]),
        (z().dt().year(), [true, true, true, true]),
    ];
    for (part, kept) in cases {
        let guarded = guard_expr(part.clone(), Some(&schema));
        let polars_alone = |v: Option<i64>| {
            frame(&[v])
                .select([part.clone()])
                .collect()
                .unwrap()
                .columns()[0]
                .get(0)
                .unwrap()
                .into_static()
        };
        let expected: Vec<AnyValue> = stamps
            .iter()
            .enumerate()
            .map(|(i, v)| match i.checked_sub(3) {
                Some(edge) if !kept[edge] => AnyValue::Null,
                _ => polars_alone(*v),
            })
            .collect();
        for streaming in [false, true] {
            let out = crate::analysis::statistics::collect_lazy(
                frame(&stamps).select([guarded.clone()]),
                streaming,
            )
            .unwrap();
            let out: Vec<AnyValue> = out.columns()[0]
                .as_materialized_series()
                .iter()
                .map(|v| v.into_static())
                .collect();
            assert_eq!(out, expected, "{part:?} streaming: {streaming}");
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
        col("ns").dt().year(),
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
        col("ns").dt().month_start(),
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
        coalesce(&[col("t"), col("ns")]),
        coalesce(&[col("s"), lit("x")]),
        when(one()).then(col("t")).otherwise(col("ns")),
        when(one()).then(col("ns")).otherwise(col("s")),
        col("t").cast(DataType::Datetime(TimeUnit::Nanoseconds, None)),
        col("d").fill_null(col("d")),
    ];
    for expr in kept {
        assert_eq!(guard_expr(expr.clone(), Some(&schema)), expr);
    }
    let changed = [
        coalesce(&[col("t"), lit("x")]),
        coalesce(&[col("s"), col("d")]),
        when(one()).then(col("d")).otherwise(col("s")),
        when(one()).then(lit("x")).otherwise(col("t")),
        // A date met with a datetime, which Polars casts to it (#526).
        coalesce(&[col("d"), col("t")]),
        when(one()).then(col("d")).otherwise(col("t")),
        col("d").fill_null(col("t")),
        polars::lazy::dsl::max_horizontal([col("t"), col("d")]).unwrap(),
    ];
    for expr in changed {
        assert_ne!(guard_expr(expr.clone(), Some(&schema)), expr);
        assert_eq!(guard_expr(expr.clone(), None), expr);
    }
    // A date cast to a datetime, with the schema or without.
    let cast = col("d").cast(DataType::Datetime(TimeUnit::Microseconds, None));
    assert_ne!(guard_expr(cast.clone(), Some(&schema)), cast);
    assert_ne!(guard_expr(cast.clone(), None), cast);
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

/// Day counts on either side of what a nanosecond and a microsecond datetime can
/// count, the ends of a date, one in range and a null.
const DAYS: [Option<i32>; 13] = [
    Some(0),
    Some(19_737),
    Some(106_751),
    Some(106_752),
    Some(-106_751),
    Some(-106_752),
    Some(106_751_991),
    Some(106_751_992),
    Some(-106_751_991),
    Some(-106_751_992),
    Some(i32::MAX),
    Some(i32::MIN),
    None,
];

/// `d`, the dates `days`, beside µs (`t`) and ns (`tn`) datetimes with nulls, and
/// a flag `b`.
fn dates_and_datetimes(days: &[Option<i32>]) -> LazyFrame {
    let n = days.len() as i64;
    df!(
        "d" => days,
        "t" => (0..n).map(|i| (i % 3 != 0).then_some(i * 1_000_000)).collect::<Vec<_>>(),
        "b" => (0..n).map(|i| i % 2 == 0).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("d").cast(DataType::Date),
        col("t").cast(DataType::Datetime(TimeUnit::Microseconds, None)),
        col("t")
            .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
            .alias("tn"),
    ])
}

/// [`DAYS`] with each a datetime in `unit` cannot count as null.
fn countable_days(unit: TimeUnit) -> Vec<Option<i32>> {
    let per_day: i64 = match unit {
        TimeUnit::Nanoseconds => 86_400_000_000_000,
        _ => 86_400_000_000,
    };
    DAYS.iter()
        .map(|d| d.filter(|d| i64::from(*d).abs() <= i64::MAX / per_day))
        .collect()
}

/// A date cast to a datetime that cannot count it, explicitly or to the common
/// type of a coalesce, a when/then/otherwise, `fill_null` or a horizontal min or
/// max, is null, where Polars' strict cast panicked naming it (#526). Every other
/// value is what Polars gives it, on both engines.
#[test]
fn a_date_a_datetime_cannot_count_is_null_as_one() {
    use polars::lazy::dsl::{max_horizontal, min_horizontal};
    let schema = dates_and_datetimes(&DAYS).collect_schema().unwrap();
    let us = TimeUnit::Microseconds;
    let ns = TimeUnit::Nanoseconds;
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let cases = [
        (col("d").strict_cast(DataType::Datetime(us, None)), us),
        (col("d").strict_cast(DataType::Datetime(ns, None)), ns),
        (col("d").strict_cast(DataType::Datetime(us, paris)), us),
        (coalesce(&[col("d"), col("t")]), us),
        (coalesce(&[col("t"), col("d")]), us),
        (coalesce(&[col("d"), col("tn")]), ns),
        (when(col("b")).then(col("d")).otherwise(col("t")), us),
        (max_horizontal([col("d"), col("t")]).unwrap(), us),
        (min_horizontal([col("t"), col("d")]).unwrap(), us),
        (col("d").fill_null(col("t")), us),
    ];
    for (expr, unit) in cases {
        let guarded = guard_expr(expr.clone(), Some(&schema));
        let expected = dates_and_datetimes(&countable_days(unit))
            .select([expr.clone()])
            .collect()
            .unwrap();
        for streaming in [false, true] {
            let out = crate::analysis::statistics::collect_lazy(
                dates_and_datetimes(&DAYS).select([guarded.clone()]),
                streaming,
            )
            .unwrap();
            assert_eq!(out.schema(), expected.schema(), "{expr:?}");
            assert!(
                out.equals_missing(&expected),
                "{expr:?} streaming: {streaming}\n{out}\n{expected}"
            );
        }
    }
}

/// SQL's cast of a date to a timestamp, and a date met with a timestamp, are null
/// where the timestamp cannot count the date, where Polars panicked (#526).
#[cfg(feature = "sql")]
#[test]
fn a_date_a_timestamp_cannot_count_is_null_in_sql() {
    let mut ctx = polars_sql::SQLContext::new();
    for (sql, unit) in [
        (
            "SELECT CAST(d AS TIMESTAMP) AS x FROM df",
            TimeUnit::Microseconds,
        ),
        ("SELECT d::timestamp AS x FROM df", TimeUnit::Microseconds),
        (
            "SELECT CAST(d AS TIMESTAMP(9)) AS x FROM df",
            TimeUnit::Nanoseconds,
        ),
        ("SELECT COALESCE(d, t) AS x FROM df", TimeUnit::Microseconds),
        ("SELECT IFNULL(t, d) AS x FROM df", TimeUnit::Microseconds),
        ("SELECT COALESCE(d, tn) AS x FROM df", TimeUnit::Nanoseconds),
        (
            "SELECT CASE WHEN b THEN d ELSE t END AS x FROM df",
            TimeUnit::Microseconds,
        ),
        ("SELECT GREATEST(d, t) AS x FROM df", TimeUnit::Microseconds),
        ("SELECT LEAST(t, d) AS x FROM df", TimeUnit::Microseconds),
        (
            "SELECT MAX(CAST(d AS TIMESTAMP)) AS x FROM df GROUP BY b",
            TimeUnit::Microseconds,
        ),
        (
            "SELECT df.d AS x FROM df JOIN e ON CAST(df.d AS TIMESTAMP) = e.t",
            TimeUnit::Microseconds,
        ),
    ] {
        // Rows sorted: a GROUP BY's and a join's come in any order.
        let mut run = |days: &[Option<i32>], guard: bool| {
            ctx.register("df", dates_and_datetimes(days));
            ctx.register("e", dates_and_datetimes(days));
            let mut lf = ctx.execute(sql).unwrap();
            if guard {
                guard_plan(&mut lf.logical_plan);
            }
            lf.sort(["x"], SortMultipleOptions::default())
                .collect()
                .unwrap()
        };
        let expected = run(&countable_days(unit), false);
        let out = run(&DAYS, true);
        assert!(out.equals_missing(&expected), "{sql}\n{out}\n{expected}");
    }
}
