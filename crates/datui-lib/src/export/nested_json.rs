//! List, array and struct cells as JSON text for one-value-per-field destinations (CSV
//! export, clipboard copies): JSON keeps struct field names and reads back anywhere
//! (`str.json_decode`). The text is Polars' JSON writer's, as in NDJSON export.
//!
//! Special cases where Polars' writers panic or have no form: binary is standard base64
//! everywhere; dates and ms/µs datetimes go in as their written text (values past the
//! calendar as their stored number, as the table shows; ns is always in range);
//! durations are ISO 8601 seconds as the JSON writer spells them (`PT3723.004S`,
//! `-PT1.5S`, `P0D`), exact to the nanosecond.

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

/// A date or datetime the writers can panic on. Every nanosecond count is a
/// date (1677 to 2262), so those go to the writers as they are, at no cost.
fn is_calendar(dtype: &DataType) -> bool {
    crate::past_calendar::can_leave_calendar(dtype)
}

/// Whether `dtype` is binary or [`is_calendar`], or has one inside: what the
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

/// `value` `unit`s as ISO 8601, as Polars' JSON writer (chrono's `TimeDelta`) spells it:
/// whole seconds plus significant fraction digits, `P0D` for zero, `-` when negative.
/// Computed here: chrono's range stops short of `i64::MIN` ms.
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
/// nested, binary, a duration, or a date or datetime in ms or us, which the
/// writer can panic on.
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
    crate::past_calendar::text_or_stored(series, |s| match s.dtype() {
        DataType::Date => s.date()?.to_string(format),
        _ => s.datetime()?.to_string(format),
    })
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
pub(crate) mod tests;
