//! List, array and struct cells as JSON text, for the destinations that hold
//! one value per field: a CSV export and every clipboard copy. JSON rather than
//! the table's own rendering (`[1, 2]`, `{1,"a"}`) because it keeps struct field
//! names and reads back with any JSON parser, Polars' `str.json_decode` included.
//!
//! The text is Polars' own JSON writer, the one NDJSON export uses, so a list
//! reads the same in a CSV as in a `.jsonl` written from the same view.
//!
//! Binary has no JSON or CSV form, and Polars' JSON writer panics on it, so it
//! is written as standard base64 text wherever it sits: a CSV, JSON or NDJSON
//! export and a copy all spell the same bytes the same way.
//!
//! Polars' writers also panic on a date or datetime past the calendar's range
//! (a sentinel like `i64::MIN + 1` microseconds), so dates and datetimes are
//! given to them as the text they would write, and such a value as its stored
//! number, as the table shows it.
//!
//! A duration has no CSV form either, and is written as the JSON writer spells
//! it: ISO 8601 in seconds (`PT3723.004S`, `-PT1.5S`, `P0D`). That is exact to
//! the nanosecond in every unit, and reads the same alone in a CSV cell or a
//! copy, inside a list, and in a JSON export.

use base64::Engine as _;
use polars::prelude::*;

/// Whether a column is a list, array or struct, which a delimited writer
/// takes only as JSON text.
pub fn is_nested(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::List(_) | DataType::Array(_, _) | DataType::Struct(_)
    )
}

fn is_binary(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Binary | DataType::BinaryOffset)
}

/// Whether `dtype` is binary or has binary anywhere inside.
pub fn has_binary(dtype: &DataType) -> bool {
    match dtype {
        DataType::List(inner) | DataType::Array(inner, _) => has_binary(inner),
        DataType::Struct(fields) => fields.iter().any(|f| has_binary(f.dtype())),
        dtype => is_binary(dtype),
    }
}

fn is_calendar(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Date | DataType::Datetime(..))
}

/// Whether `dtype` is binary, a date or a datetime, or has one inside: what the
/// JSON writer is given as text.
fn has_json_text(dtype: &DataType) -> bool {
    match dtype {
        DataType::List(inner) | DataType::Array(inner, _) => has_json_text(inner),
        DataType::Struct(fields) => fields.iter().any(|f| has_json_text(f.dtype())),
        dtype => is_binary(dtype) || is_calendar(dtype),
    }
}

/// `dtype` with every binary, date and datetime leaf as a string, the type
/// [`leaves_as_json_text`] returns.
fn json_text_dtype(dtype: &DataType) -> DataType {
    match dtype {
        DataType::List(inner) => DataType::List(Box::new(json_text_dtype(inner))),
        DataType::Array(inner, width) => DataType::Array(Box::new(json_text_dtype(inner)), *width),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| Field::new(f.name().clone(), json_text_dtype(f.dtype())))
                .collect(),
        ),
        dtype if is_binary(dtype) || is_calendar(dtype) => DataType::String,
        dtype => dtype.clone(),
    }
}

/// `series` with every binary value, at any depth, as its base64 text, and every
/// date and datetime as the JSON writer's text ([`calendar_as_text`]). Lists,
/// arrays and structs keep their shape and their nulls.
pub fn leaves_as_json_text(series: &Series) -> PolarsResult<Series> {
    Ok(match series.dtype() {
        dtype if !has_json_text(dtype) => series.clone(),
        DataType::List(_) => series
            .list()?
            .apply_to_inner(&|inner| leaves_as_json_text(&inner))?
            .into_series(),
        DataType::Array(..) => series
            .array()?
            .apply_to_inner(&|inner| leaves_as_json_text(&inner))?
            .into_series(),
        DataType::Struct(_) => series
            .struct_()?
            .try_apply_fields(leaves_as_json_text)?
            .into_series(),
        dtype if is_calendar(dtype) => calendar_as_text(series, Writer::Json)?,
        _ => {
            let engine = base64::engine::general_purpose::STANDARD;
            let bytes = series.cast(&DataType::Binary)?;
            bytes
                .binary()?
                .iter()
                .map(|value| value.map(|b| engine.encode(b)))
                .collect::<StringChunked>()
                .with_name(series.name().clone())
                .into_series()
        }
    })
}

/// `lf` with every column that has binary, a date or a datetime in it as text
/// ([`leaves_as_json_text`]), in place and under its own name: what a JSON export
/// writes. Planned, not run.
pub fn lazy_for_json(mut lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let schema = lf.collect_schema()?;
    let exprs: Vec<Expr> = schema
        .iter()
        .filter(|(_, dtype)| has_json_text(dtype))
        .map(|(name, _)| {
            col(name.clone()).map(
                |c| leaves_as_json_text(c.as_materialized_series()).map(Column::from),
                |_, field| {
                    Ok(Field::new(
                        field.name().clone(),
                        json_text_dtype(field.dtype()),
                    ))
                },
            )
        })
        .collect();
    Ok(if exprs.is_empty() {
        lf
    } else {
        lf.with_columns(exprs)
    })
}

/// `value` `unit`s as ISO 8601 text, the way Polars' JSON writer spells a
/// duration (chrono's `TimeDelta` display): whole seconds and the fraction's
/// significant digits, `P0D` for zero, a leading `-` when negative. Computed
/// here rather than through chrono, whose range ends short of `i64::MIN` ms.
pub fn duration_iso(value: i64, unit: TimeUnit, out: &mut String) {
    use std::fmt::Write as _;
    let nanos_per_unit: i128 = match unit {
        TimeUnit::Nanoseconds => 1,
        TimeUnit::Microseconds => 1_000,
        TimeUnit::Milliseconds => 1_000_000,
    };
    let total = i128::from(value) * nanos_per_unit;
    if total == 0 {
        out.push_str("P0D");
        return;
    }
    if total < 0 {
        out.push('-');
    }
    let abs = total.unsigned_abs();
    let (secs, nanos) = (abs / 1_000_000_000, abs % 1_000_000_000);
    let _ = write!(out, "PT{secs}");
    if nanos > 0 {
        let (mut fraction, mut digits) = (nanos, 9);
        while fraction % 10 == 0 {
            fraction /= 10;
            digits -= 1;
        }
        let _ = write!(out, ".{fraction:0digits$}");
    }
    out.push('S');
}

/// A duration column as a String column of [`duration_iso`] text, with its nulls.
pub fn duration_as_iso(series: &Series) -> PolarsResult<Series> {
    let DataType::Duration(unit) = series.dtype() else {
        polars_bail!(InvalidOperation: "expected a duration, got {}", series.dtype());
    };
    let unit = *unit;
    Ok(series
        .to_physical_repr()
        .i64()?
        .apply_into_string_amortized(|value, out| duration_iso(value, unit, out))
        .with_name(series.name().clone())
        .into_series())
}

/// Whether a column needs to become text before a CSV writer takes it: it is
/// nested, binary, a duration, a date or a datetime.
pub fn needs_text(dtype: &DataType) -> bool {
    is_nested(dtype)
        || is_binary(dtype)
        || is_calendar(dtype)
        || matches!(dtype, DataType::Duration(_))
}

/// One column that [`needs_text`] as the String column a CSV cell or a copy
/// holds: JSON, base64, ISO 8601 or the CSV writer's own text, by its type.
fn column_as_text(column: &Column) -> PolarsResult<Column> {
    match column.dtype() {
        DataType::Duration(_) => duration_as_iso(column.as_materialized_series()).map(Column::from),
        dtype if is_calendar(dtype) => {
            calendar_as_text(column.as_materialized_series(), Writer::Csv).map(Column::from)
        }
        dtype if is_binary(dtype) => {
            leaves_as_json_text(column.as_materialized_series()).map(Column::from)
        }
        _ => column_as_json(column),
    }
}

/// The writer whose text for a date or datetime [`calendar_as_text`] writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Writer {
    Csv,
    Json,
}

/// A date or datetime column as the text `writer` writes for it, with its nulls;
/// a value past the calendar's range, on which the writers panic, as its stored
/// number ([`crate::exact::out_of_range`]), as the table shows it.
pub fn calendar_as_text(series: &Series, writer: Writer) -> PolarsResult<Series> {
    // The writers' own defaults: the CSV writer's formats, and the JSON
    // writer's chrono display (`to_rfc3339` with a zone).
    let format = match (series.dtype(), writer) {
        (DataType::Date, _) => "%Y-%m-%d",
        (DataType::Datetime(unit, zone), Writer::Csv) => match (unit, zone.is_some()) {
            (TimeUnit::Milliseconds, false) => "%FT%H:%M:%S.%3f",
            (TimeUnit::Milliseconds, true) => "%FT%H:%M:%S.%3f%z",
            (TimeUnit::Microseconds, false) => "%FT%H:%M:%S.%6f",
            (TimeUnit::Microseconds, true) => "%FT%H:%M:%S.%6f%z",
            (TimeUnit::Nanoseconds, false) => "%FT%H:%M:%S.%9f",
            (TimeUnit::Nanoseconds, true) => "%FT%H:%M:%S.%9f%z",
        },
        (DataType::Datetime(_, None), Writer::Json) => "%Y-%m-%d %H:%M:%S%.f",
        (DataType::Datetime(_, Some(_)), Writer::Json) => "%Y-%m-%dT%H:%M:%S%.f%:z",
        (dtype, _) => polars_bail!(InvalidOperation: "expected a date or datetime, got {dtype}"),
    };
    let dtype = series.dtype();
    let text = |s: &Series| match s.dtype() {
        DataType::Date => s.date()?.to_string(format),
        _ => s.datetime()?.to_string(format),
    };
    let text = match crate::exact::calendar_without_out_of_range(series)? {
        // The rest formatted as usual, these written in after.
        Some(shown) => {
            let stored = series.to_physical_repr().cast(&DataType::Int64)?;
            text(&shown)?
                .iter()
                .zip(stored.i64()?.iter())
                .map(|(text, v)| match text {
                    Some(text) => Some(text.to_string()),
                    None => v.and_then(|v| crate::exact::stored_out_of_range(dtype, v)),
                })
                .collect::<StringChunked>()
        }
        None => text(series)?,
    };
    Ok(text.with_name(series.name().clone()).into_series())
}

/// One nested column as a String column of JSON, null where the value is null.
pub fn column_as_json(column: &Column) -> PolarsResult<Column> {
    let series = leaves_as_json_text(column.as_materialized_series())?;
    let chunks = (0..series.n_chunks()).map(|i| {
        let array = series.to_arrow(i, CompatLevel::newest());
        // The writer spells a missing value `null`; the cell stays empty
        // instead, as a null does everywhere else in a CSV.
        polars_json::json::write::serialize_to_utf8(array.as_ref())
            .with_validity(array.validity().cloned())
    });
    Ok(StringChunked::from_chunk_iter(series.name().clone(), chunks).into_column())
}

/// `lf` with every column a CSV writer cannot take ([`needs_text`]) replaced
/// by its text, in place and under its own name: what a CSV export writes.
/// Planned, not run: the text is built as the rows are collected.
pub fn lazy_as_json(mut lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let schema = lf.collect_schema()?;
    let exprs: Vec<Expr> = schema
        .iter()
        .filter(|(_, dtype)| needs_text(dtype))
        .map(|(name, _)| {
            col(name.clone()).map(
                |c| column_as_text(&c),
                |_, field| Ok(Field::new(field.name().clone(), DataType::String)),
            )
        })
        .collect();
    Ok(if exprs.is_empty() {
        lf
    } else {
        lf.with_columns(exprs)
    })
}

/// [`lazy_as_json`] for frames already in memory: a copy's rows, as the CSV
/// writer takes them.
pub fn frame_as_json(df: &DataFrame) -> PolarsResult<DataFrame> {
    frame_as_text(df, needs_text)
}

/// [`frame_as_json`] with dates and datetimes kept in their own type: the cells
/// a Markdown or HTML copy writes through [`crate::exact::value_text`], which
/// spells one past the calendar as its stored number itself.
pub fn frame_as_cells(df: &DataFrame) -> PolarsResult<DataFrame> {
    frame_as_text(df, |dtype| needs_text(dtype) && !is_calendar(dtype))
}

fn frame_as_text(df: &DataFrame, converts: impl Fn(&DataType) -> bool) -> PolarsResult<DataFrame> {
    let mut out = df.clone();
    for column in df.columns() {
        if converts(column.dtype()) {
            out.with_column(column_as_text(column)?)?;
        }
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The raw values behind [`durations`], one row each.
    const DURATION_VALUES: [Option<i64>; 8] = [
        Some(3_723_004),
        None,
        Some(-1_500),
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MAX),
        Some(-i64::MAX),
    ];

    /// A duration column per unit over the same raw values, with a null, zero,
    /// negatives and the ends of the range.
    pub(crate) fn durations() -> DataFrame {
        let values = Series::new("".into(), DURATION_VALUES);
        let columns = [
            ("ms", TimeUnit::Milliseconds),
            ("us", TimeUnit::Microseconds),
            ("ns", TimeUnit::Nanoseconds),
        ]
        .map(|(name, unit)| {
            values
                .cast(&DataType::Duration(unit))
                .unwrap()
                .with_name(name.into())
                .into_column()
        });
        DataFrame::new_infer_height(columns.to_vec()).unwrap()
    }

    /// [`durations`] as text, by column; a null is None.
    pub(crate) fn duration_text() -> [(&'static str, [Option<&'static str>; 8]); 3] {
        [
            (
                "ms",
                [
                    Some("PT3723.004S"),
                    None,
                    Some("-PT1.5S"),
                    Some("P0D"),
                    Some("PT0.001S"),
                    Some("-PT0.001S"),
                    Some("PT9223372036854775.807S"),
                    Some("-PT9223372036854775.807S"),
                ],
            ),
            (
                "us",
                [
                    Some("PT3.723004S"),
                    None,
                    Some("-PT0.0015S"),
                    Some("P0D"),
                    Some("PT0.000001S"),
                    Some("-PT0.000001S"),
                    Some("PT9223372036854.775807S"),
                    Some("-PT9223372036854.775807S"),
                ],
            ),
            (
                "ns",
                [
                    Some("PT0.003723004S"),
                    None,
                    Some("-PT0.0000015S"),
                    Some("P0D"),
                    Some("PT0.000000001S"),
                    Some("-PT0.000000001S"),
                    Some("PT9223372036.854775807S"),
                    Some("-PT9223372036.854775807S"),
                ],
            ),
        ]
    }

    /// A duration is ISO 8601 text in every unit, with its nulls, the same
    /// text in memory and planned, and the same text an NDJSON export writes.
    #[test]
    fn durations_are_iso_8601_as_the_json_writer_spells_them() {
        let df = durations();
        let cells = frame_as_json(&df).unwrap();
        for (name, expected) in duration_text() {
            let text = cells.column(name).unwrap().str().unwrap();
            assert_eq!(text.iter().collect::<Vec<_>>(), expected, "{name}");
        }
        let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
        assert!(cells.equals_missing(&lazy), "{cells}\n{lazy}");

        let mut ndjson = Vec::new();
        JsonWriter::new(&mut ndjson)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut df.clone())
            .unwrap();
        let json = |name: &str, i: usize| {
            cells
                .column(name)
                .unwrap()
                .str()
                .unwrap()
                .get(i)
                .map_or("null".to_string(), |s| format!("\"{s}\""))
        };
        let rebuilt: String = (0..cells.height())
            .map(|i| {
                format!(
                    "{{\"ms\":{},\"us\":{},\"ns\":{}}}\n",
                    json("ms", i),
                    json("us", i),
                    json("ns", i)
                )
            })
            .collect();
        assert_eq!(rebuilt, String::from_utf8(ndjson).unwrap());
    }

    /// Past the end of chrono's range, where the JSON writer gives up, the
    /// text is still exact.
    #[test]
    fn the_longest_negative_duration_is_exact() {
        let mut text = String::new();
        duration_iso(i64::MIN, TimeUnit::Milliseconds, &mut text);
        assert_eq!(text, "-PT9223372036854775.808S");
        text.clear();
        duration_iso(i64::MIN, TimeUnit::Nanoseconds, &mut text);
        assert_eq!(text, "-PT9223372036.854775808S");
    }

    /// Dates and datetimes in every unit, with and without a zone, and their
    /// nulls, years before 0 and past 9999 among them; `past` adds the ends of
    /// the stored range, which no writer takes.
    pub(crate) fn calendar(past: bool) -> DataFrame {
        let mut stamps = vec![
            Some(0i64),
            None,
            Some(-1),
            Some(1_700_000_000_123),
            Some(-62_000_000_000_000),
            Some(-100_000_000_000_000),
            Some(300_000_000_000_000),
        ];
        let mut days = vec![
            Some(0i32),
            None,
            Some(-1),
            Some(19_724),
            Some(-800_000),
            Some(-1_000_000),
            Some(3_000_000),
        ];
        if past {
            stamps.extend([Some(i64::MIN + 1), Some(i64::MAX)]);
            days.extend([Some(i32::MIN), Some(i32::MAX)]);
        }
        let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
        let mut columns = vec![
            Series::new("d".into(), days)
                .cast(&DataType::Date)
                .unwrap()
                .into_column(),
        ];
        for (unit, name) in [
            (TimeUnit::Milliseconds, "ms"),
            (TimeUnit::Microseconds, "us"),
            (TimeUnit::Nanoseconds, "ns"),
        ] {
            for (zone, suffix) in [(None, ""), (paris.clone(), "_tz")] {
                columns.push(
                    Series::new(format!("{name}{suffix}").into(), &stamps)
                        .cast(&DataType::Datetime(unit, zone))
                        .unwrap()
                        .into_column(),
                );
            }
        }
        DataFrame::new_infer_height(columns).unwrap()
    }

    /// Given as text, dates and datetimes read exactly as the CSV and JSON
    /// writers write them.
    #[test]
    fn dates_as_text_are_what_the_writers_write() {
        let df = calendar(false);
        let mut csv = Vec::new();
        CsvWriter::new(&mut csv).finish(&mut df.clone()).unwrap();
        let mut as_text = Vec::new();
        CsvWriter::new(&mut as_text)
            .finish(&mut frame_as_json(&df).unwrap())
            .unwrap();
        assert_eq!(
            String::from_utf8(as_text).unwrap(),
            String::from_utf8(csv).unwrap()
        );
        let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
        assert!(frame_as_json(&df).unwrap().equals_missing(&lazy));

        let mut json = Vec::new();
        JsonWriter::new(&mut json)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut df.clone())
            .unwrap();
        let mut prepared = lazy_for_json(df.clone().lazy()).unwrap().collect().unwrap();
        assert!(
            prepared
                .columns()
                .iter()
                .all(|c| c.dtype() == &DataType::String)
        );
        let mut as_text = Vec::new();
        JsonWriter::new(&mut as_text)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut prepared)
            .unwrap();
        assert_eq!(
            String::from_utf8(as_text).unwrap(),
            String::from_utf8(json).unwrap()
        );
    }

    /// The writers panic on a date past the calendar; as text it is its stored
    /// number, alone or in a list, and the rest of its column is unchanged.
    #[test]
    fn a_date_past_the_calendar_is_written_as_its_stored_number() {
        let df = calendar(true);
        let fine = frame_as_json(&calendar(false)).unwrap();
        let cells = frame_as_json(&df).unwrap();
        assert!(cells.slice(0, fine.height()).equals_missing(&fine));
        let text = |name: &str, row: usize| {
            cells
                .column(name)
                .unwrap()
                .str()
                .unwrap()
                .get(row)
                .map(str::to_string)
        };
        let last = df.height() - 1;
        assert_eq!(
            text("d", last - 1).as_deref(),
            Some("-2147483648 days since 1970-01-01")
        );
        assert_eq!(
            text("ms_tz", last).as_deref(),
            Some("9223372036854775807 ms since 1970-01-01 UTC")
        );
        assert_eq!(
            text("us", last - 1).as_deref(),
            Some("-9223372036854775807 us since 1970-01-01 UTC")
        );
        // Every nanosecond count is a date the writer takes.
        assert_eq!(
            text("ns", last).as_deref(),
            Some("2262-04-11T23:47:16.854775807")
        );
        let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
        assert!(cells.equals_missing(&lazy));

        let mut json = Vec::new();
        JsonWriter::new(&mut json)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut lazy_for_json(df.clone().lazy()).unwrap().collect().unwrap())
            .unwrap();
        let json = String::from_utf8(json).unwrap();
        assert!(
            json.lines()
                .last()
                .unwrap()
                .contains(r#""us_tz":"9223372036854775807 us since 1970-01-01 UTC""#),
            "{json}"
        );

        let listed = df
            .clone()
            .lazy()
            .select([col("us").implode(true), col("d").implode(true)])
            .collect()
            .unwrap();
        let cells = frame_as_json(&listed).unwrap();
        let us = cells
            .column("us")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap()
            .to_string();
        assert!(
            us.starts_with(r#"["1970-01-01 00:00:00",null,"#)
                && us.ends_with(r#""-9223372036854775807 us since 1970-01-01 UTC","9223372036854775807 us since 1970-01-01 UTC"]"#),
            "{us}"
        );
    }

    fn nested() -> DataFrame {
        let ids = Series::new("ids".into(), [1i64, 2, 3]);
        let lists = Series::new(
            "xs".into(),
            [
                Some(Series::new("".into(), ["a", "b"])),
                None,
                Some(Series::new("".into(), ["say \"hi\", then go"])),
            ],
        );
        let point = StructChunked::from_series(
            "point".into(),
            3,
            [
                Series::new("x".into(), [1i64, 2, 3]),
                Series::new("y".into(), [Some("a"), None, Some("c")]),
            ]
            .iter(),
        )
        .unwrap()
        .into_series();
        let pairs = Series::new(
            "pair".into(),
            [
                Some(Series::new("".into(), [1.5f64, f64::NAN])),
                Some(Series::new("".into(), [0.0f64, 2.0])),
                None,
            ],
        )
        .cast(&DataType::Array(Box::new(DataType::Float64), 2))
        .unwrap();
        DataFrame::new_infer_height(vec![ids.into(), lists.into(), point.into(), pairs.into()])
            .unwrap()
    }

    #[test]
    fn nested_columns_become_json_and_the_rest_stay() {
        let df = frame_as_json(&nested()).unwrap();
        assert_eq!(df.column("ids").unwrap().dtype(), &DataType::Int64);
        let xs = df.column("xs").unwrap().str().unwrap().clone();
        assert_eq!(xs.get(0), Some(r#"["a","b"]"#));
        assert_eq!(xs.get(1), None, "a null list stays null");
        assert_eq!(xs.get(2), Some(r#"["say \"hi\", then go"]"#));
        let point = df.column("point").unwrap().str().unwrap().clone();
        assert_eq!(point.get(0), Some(r#"{"x":1,"y":"a"}"#));
        assert_eq!(point.get(1), Some(r#"{"x":2,"y":null}"#));
        let pair = df.column("pair").unwrap().str().unwrap().clone();
        assert_eq!(pair.get(0), Some("[1.5,null]"), "NaN has no JSON spelling");
        assert_eq!(pair.get(1), Some("[0.0,2.0]"));
        assert_eq!(pair.get(2), None);
    }

    #[test]
    fn lazy_and_in_memory_agree() {
        let eager = frame_as_json(&nested()).unwrap();
        let lazy = lazy_as_json(nested().lazy()).unwrap().collect().unwrap();
        assert!(eager.equals_missing(&lazy), "{eager}\n{lazy}");
    }

    /// Dates, datetimes and categoricals inside a struct or list read the same
    /// in a CSV cell as in an NDJSON export of the same frame.
    #[test]
    fn cells_match_an_ndjson_export() {
        let df = df!(
            "d" => [Some("2024-01-02"), None],
            "c" => [Some("a"), None],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("d").str().to_date(StrptimeOptions::default()),
            col("c").cast(DataType::from_categories(Categories::global())),
        ])
        .with_columns([col("d")
            .cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            ))
            .alias("dt")])
        .select([
            as_struct(vec![col("d"), col("dt"), col("c")]).alias("s"),
            col("c").implode(true).alias("lc"),
        ])
        .collect()
        .unwrap();
        let mut ndjson = Vec::new();
        JsonWriter::new(&mut ndjson)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut df.clone())
            .unwrap();
        let cells = frame_as_json(&df).unwrap();
        let (s, lc) = (
            cells.column("s").unwrap().str().unwrap(),
            cells.column("lc").unwrap().str().unwrap(),
        );
        let rebuilt: String = (0..cells.height())
            .map(|i| {
                format!(
                    "{{\"s\":{},\"lc\":{}}}\n",
                    s.get(i).unwrap(),
                    lc.get(i).unwrap()
                )
            })
            .collect();
        assert_eq!(rebuilt, String::from_utf8(ndjson).unwrap());
        assert!(rebuilt.contains(r#""dt":"2024-01-02T00:00:00+00:00","c":"a""#));
    }

    /// Binary is base64 at any depth, with its nulls, and the same in a copy,
    /// a CSV cell and an NDJSON export.
    #[test]
    fn binary_is_base64_everywhere() {
        let blob = Series::new("blob".into(), [Some(b"hi\xff".as_slice()), None]);
        let blobs = Series::new(
            "blobs".into(),
            [Some(Series::new("".into(), [b"x".as_slice()])), None],
        );
        let pair = Series::new(
            "pair".into(),
            [
                Some(Series::new("".into(), [b"a".as_slice(), b"b".as_slice()])),
                None,
            ],
        )
        .cast(&DataType::Array(Box::new(DataType::Binary), 2))
        .unwrap();
        let meta = StructChunked::from_series(
            "meta".into(),
            2,
            [Series::new(
                "raw".into(),
                [b"ab".as_slice(), b"".as_slice()],
            )]
            .iter(),
        )
        .unwrap()
        .with_outer_validity(Some([true, false].into_iter().collect()))
        .into_series();
        let df =
            DataFrame::new_infer_height(vec![blob.into(), blobs.into(), pair.into(), meta.into()])
                .unwrap();

        for column in df.columns() {
            let text = leaves_as_json_text(column.as_materialized_series()).unwrap();
            assert_eq!(text.dtype(), &json_text_dtype(column.dtype()));
            assert_eq!(text.null_count(), 1, "{text}");
        }

        let cells = frame_as_json(&df).unwrap();
        let cell = |name: &str| cells.column(name).unwrap().str().unwrap().get(0);
        assert_eq!(cell("blob"), Some("aGn/"));
        assert_eq!(cell("blobs"), Some(r#"["eA=="]"#));
        assert_eq!(cell("pair"), Some(r#"["YQ==","Yg=="]"#));
        assert_eq!(cell("meta"), Some(r#"{"raw":"YWI="}"#));
        let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
        assert!(cells.equals_missing(&lazy), "{cells}\n{lazy}");

        let mut ndjson = Vec::new();
        JsonWriter::new(&mut ndjson)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut lazy_for_json(df.clone().lazy()).unwrap().collect().unwrap())
            .unwrap();
        assert_eq!(
            String::from_utf8(ndjson).unwrap().lines().next(),
            Some(
                r#"{"blob":"aGn/","blobs":["eA=="],"pair":["YQ==","Yg=="],"meta":{"raw":"YWI="}}"#
            )
        );

        let copy =
            crate::clipboard::tabular_payload(&df, crate::clipboard::CopyFormat::Tsv, true, true)
                .unwrap();
        let row: Vec<&str> = copy.text.lines().nth(1).unwrap().split('\t').collect();
        assert_eq!(
            row,
            [
                "aGn/",
                r#""[""eA==""]""#,
                r#""[""YQ=="",""Yg==""]""#,
                r#""{""raw"":""YWI=""}""#
            ]
        );
    }
}
