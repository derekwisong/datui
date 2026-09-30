//! Columns cast to types Polars' Avro writer can hold, for an Avro export only.
//!
//! The writer knows booleans, 32- and 64-bit integers and floats, strings,
//! binary, decimals, dates, naive millisecond and microsecond datetimes, and
//! lists and structs of those. Anything else fails the whole export with
//! "not yet implemented", so each is cast to the nearest type it does know.
//! The casts are strict: a value that does not fit (a `u64` past `i64::MAX`)
//! fails the export by column name instead of turning null.

use polars::prelude::*;

/// What `dtype` is written as. `durations` keeps times and durations as
/// microsecond durations, the step before they become plain integers: a
/// direct cast would count in each type's own unit, nanoseconds for a time.
fn writable(dtype: &DataType, durations: bool) -> DataType {
    use DataType as D;
    match dtype {
        D::Int8 | D::Int16 | D::UInt8 | D::UInt16 => D::Int32,
        D::UInt32 | D::UInt64 => D::Int64,
        D::Int128 | D::UInt128 => D::Decimal(38, 0),
        D::Float16 => D::Float32,
        // A time zone goes, the instant stays: the values read as UTC.
        D::Datetime(unit, _) => D::Datetime(
            match unit {
                TimeUnit::Nanoseconds => TimeUnit::Microseconds,
                unit => *unit,
            },
            None,
        ),
        D::Time | D::Duration(_) if durations => D::Duration(TimeUnit::Microseconds),
        D::Time | D::Duration(_) => D::Int64,
        D::Categorical(..) | D::Enum(..) | D::Null => D::String,
        D::List(inner) | D::Array(inner, _) => D::List(Box::new(writable(inner, durations))),
        D::Struct(fields) => D::Struct(
            fields
                .iter()
                .map(|f| Field::new(f.name().clone(), writable(f.dtype(), durations)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// `lf` with every column Avro cannot hold cast to one it can, under its own
/// name. Planned, not run.
pub fn lazy_for_avro(mut lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let schema = lf.collect_schema()?;
    let exprs: Vec<Expr> = schema
        .iter()
        .filter_map(|(name, dtype)| {
            let target = writable(dtype, false);
            if &target == dtype {
                return None;
            }
            let step = writable(dtype, true);
            let expr = col(name.clone());
            let expr = if step == target {
                expr
            } else {
                expr.strict_cast(step)
            };
            Some(expr.strict_cast(target))
        })
        .collect();
    Ok(if exprs.is_empty() {
        lf
    } else {
        lf.with_columns(exprs)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::io::avro::{AvroReader, AvroWriter};

    fn round_trip(lf: LazyFrame) -> DataFrame {
        let mut df = lazy_for_avro(lf).unwrap().collect().unwrap();
        let mut bytes = Vec::new();
        AvroWriter::new(&mut bytes).finish(&mut df).unwrap();
        AvroReader::new(std::io::Cursor::new(bytes))
            .finish()
            .unwrap()
    }

    fn category() -> DataType {
        DataType::from_categories(Categories::global())
    }

    /// Every type the writer lacks comes back as the one it was cast to, with
    /// its values; the types it has come back untouched.
    #[test]
    fn every_type_avro_lacks_is_written() {
        let lf = df!(
            "n" => [Some(3_600_000_000i64), None],
            "s" => [Some("a"), Some("b")],
        )
        .unwrap()
        .lazy()
        .select([
            col("n").alias("kept"),
            col("n").cast(DataType::Int16).alias("i16"),
            col("n").cast(DataType::UInt32).alias("u32"),
            col("n").cast(DataType::UInt64).alias("u64"),
            col("n").cast(DataType::Int128).alias("i128"),
            col("n")
                .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                .alias("ns"),
            col("n")
                .cast(DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(TimeZone::UTC),
                ))
                .alias("utc"),
            col("n").cast(DataType::Time).alias("time"),
            col("n")
                .cast(DataType::Duration(TimeUnit::Milliseconds))
                .alias("ms"),
            col("s").cast(category()).alias("cat"),
            lit(NULL).alias("null"),
            col("n")
                .fill_null(0)
                .implode(true)
                .cast(DataType::Array(Box::new(DataType::Int64), 2))
                .alias("arr"),
            col("s").cast(category()).implode(true).alias("cats"),
            as_struct(vec![
                col("s").cast(category()),
                col("n").cast(DataType::Time),
            ])
            .alias("point"),
        ]);
        let back = round_trip(lf);
        let dtype = |name: &str| back.column(name).unwrap().dtype().clone();
        let first = |name: &str| back.column(name).unwrap().get(0).unwrap().into_static();
        assert_eq!(dtype("kept"), DataType::Int64);
        assert_eq!(dtype("i16"), DataType::Int32);
        assert_eq!(dtype("u32"), DataType::Int64);
        assert_eq!(dtype("u64"), DataType::Int64);
        assert_eq!(dtype("i128"), DataType::Decimal(38, 0));
        assert_eq!(
            first("ns"),
            AnyValue::Datetime(3_600_000, TimeUnit::Microseconds, None)
        );
        assert_eq!(
            first("utc"),
            AnyValue::Datetime(3_600_000_000, TimeUnit::Microseconds, None),
            "the UTC instant, without its zone"
        );
        assert_eq!(first("time"), AnyValue::Int64(3_600_000), "microseconds");
        assert_eq!(
            first("ms"),
            AnyValue::Int64(3_600_000_000_000),
            "microseconds, whatever the unit"
        );
        assert_eq!(dtype("cat"), DataType::String);
        assert_eq!(first("cat"), AnyValue::StringOwned("a".into()));
        assert_eq!(dtype("null"), DataType::String);
        assert_eq!(dtype("arr"), DataType::List(Box::new(DataType::Int64)));
        assert_eq!(dtype("cats"), DataType::List(Box::new(DataType::String)));
        assert_eq!(
            dtype("point"),
            DataType::Struct(vec![
                Field::new("s".into(), DataType::String),
                Field::new("n".into(), DataType::Int64),
            ])
        );
    }

    /// A value that does not fit fails by column name rather than turning null.
    #[test]
    fn an_unsigned_value_past_i64_fails_by_name() {
        let lf = df!("big" => [u64::MAX]).unwrap().lazy();
        let err = lazy_for_avro(lf)
            .unwrap()
            .collect()
            .unwrap_err()
            .to_string();
        assert!(err.contains("'big'"), "{err}");
    }
}
