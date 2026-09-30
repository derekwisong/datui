//! Columns cast to types Polars' Avro writer can hold, for an Avro export only.
//!
//! The writer knows booleans, 32- and 64-bit integers and floats, strings,
//! binary, dates, naive millisecond and microsecond datetimes, and lists and
//! structs of those. Anything else fails the whole export with "not yet
//! implemented", so each is cast to the nearest type it does know. The casts
//! are strict: a value that does not fit (a `u64` past `i64::MAX`) fails the
//! export by column name instead of turning null.
//!
//! It also writes decimals, but wrongly: it drops the sign byte of a positive
//! value whose leading byte is 0x80 or more, so every reader sees 327.68 as
//! -327.68. Decimals are written as their exact text instead.
//!
//! And it writes names as they are, but an Avro name is `[A-Za-z_][A-Za-z0-9_]*`:
//! strict readers refuse `my col` or `2024`. Columns and struct fields are
//! renamed to valid names; the writer has no way to keep the originals in the
//! schema's `aliases` or `doc`.

use polars::prelude::*;

/// The record name of an export. Polars' default is empty, which strict readers
/// refuse; its nested records are `r1`, `r2`, ..., so this never clashes.
pub const RECORD_NAME: &str = "Row";

fn is_avro_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Valid Avro names for `names`, in order. A valid name is kept; in any other,
/// each character outside `[A-Za-z0-9_]` becomes `_`, a leading digit gets a
/// `_` in front, and a name then taken gets the first free `_2`, `_3`, ...
/// Valid names are reserved first, so a column already valid is never renamed.
fn avro_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let names: Vec<&str> = names.into_iter().collect();
    let mut taken: PlHashSet<String> = names
        .iter()
        .filter(|name| is_avro_name(name))
        .map(|name| name.to_string())
        .collect();
    names
        .iter()
        .map(|&name| {
            if is_avro_name(name) {
                return name.to_string();
            }
            let mut base: String = name
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            if !base.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
                base.insert(0, '_');
            }
            let name = if taken.contains(&base) {
                (2..)
                    .map(|n| format!("{base}_{n}"))
                    .find(|candidate| !taken.contains(candidate))
                    .expect("some suffix is free")
            } else {
                base
            };
            taken.insert(name.clone());
            name
        })
        .collect()
}

/// `dtype` with the fields of every struct in it under valid Avro names.
fn avro_named(dtype: &DataType) -> DataType {
    match dtype {
        DataType::List(inner) => DataType::List(Box::new(avro_named(inner))),
        DataType::Array(inner, width) => DataType::Array(Box::new(avro_named(inner)), *width),
        DataType::Struct(fields) => {
            let names = avro_names(fields.iter().map(|f| f.name().as_str()));
            DataType::Struct(
                fields
                    .iter()
                    .zip(names)
                    .map(|(f, name)| Field::new(name.into(), avro_named(f.dtype())))
                    .collect(),
            )
        }
        dtype => dtype.clone(),
    }
}

/// Whether an Avro export renames this column or a struct field inside it.
pub fn renames(name: &str, dtype: &DataType) -> bool {
    !is_avro_name(name) || &avro_named(dtype) != dtype
}

/// `series` with its struct fields renamed as in `dtype`, by position, at any
/// depth. A cast would not do: Polars casts a struct by field name, so a
/// renamed field would come out all null.
fn rename_fields(series: &Series, dtype: &DataType) -> PolarsResult<Series> {
    Ok(match dtype {
        DataType::List(inner) => series
            .list()?
            .apply_to_inner(&|s| rename_fields(&s, inner))?
            .into_series(),
        DataType::Array(inner, _) => series
            .array()?
            .apply_to_inner(&|s| rename_fields(&s, inner))?
            .into_series(),
        DataType::Struct(fields) => {
            let mut targets = fields.iter();
            series
                .struct_()?
                .try_apply_fields(|field| {
                    let target = targets.next().expect("one target per field");
                    Ok(rename_fields(field, target.dtype())?.with_name(target.name().clone()))
                })?
                .into_series()
        }
        _ => series.clone(),
    })
}

/// What `dtype` is written as. `durations` keeps times and durations as
/// microsecond durations, the step before they become plain integers: a
/// direct cast would count in each type's own unit, nanoseconds for a time.
fn writable(dtype: &DataType, durations: bool) -> DataType {
    use DataType as D;
    match dtype {
        D::Int8 | D::Int16 | D::UInt8 | D::UInt16 => D::Int32,
        D::UInt32 | D::UInt64 => D::Int64,
        D::Int128 | D::UInt128 | D::Decimal(..) => D::String,
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

/// `lf` with every column Avro cannot hold cast to one it can, and every name
/// valid in Avro. Planned, not run.
pub fn lazy_for_avro(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let mut lf = lazy_casts(lf)?;
    let schema = lf.collect_schema()?;
    let renamed: Vec<Expr> = schema
        .iter()
        .filter(|(_, dtype)| &avro_named(dtype) != *dtype)
        .map(|(name, _)| {
            col(name.clone()).map(
                |c| {
                    let series = c.as_materialized_series();
                    rename_fields(series, &avro_named(series.dtype())).map(Column::from)
                },
                |_, field| Ok(Field::new(field.name().clone(), avro_named(field.dtype()))),
            )
        })
        .collect();
    if !renamed.is_empty() {
        lf = lf.with_columns(renamed);
    }
    let (old, new): (Vec<_>, Vec<_>) = schema
        .iter_names()
        .map(|name| name.as_str())
        .zip(avro_names(schema.iter_names().map(|name| name.as_str())))
        .filter(|(old, new)| old != new)
        .unzip();
    Ok(if old.is_empty() {
        lf
    } else {
        lf.rename(old, new, true)
    })
}

/// `lf` with every column Avro cannot hold cast to one it can, under its own
/// name.
fn lazy_casts(mut lf: LazyFrame) -> PolarsResult<LazyFrame> {
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
            lit(327.68).cast(DataType::Decimal(10, 2)).alias("dec"),
            col("n")
                .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                .alias("ns"),
            col("n")
                .cast(DataType::Datetime(
                    TimeUnit::Microseconds,
                    TimeZone::opt_try_new(Some("America/New_York")).unwrap(),
                ))
                .alias("zoned"),
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
        assert_eq!(first("i128"), AnyValue::StringOwned("3600000000".into()));
        assert_eq!(
            first("dec"),
            AnyValue::StringOwned("327.68".into()),
            "the writer's own decimal reads back as -327.68"
        );
        assert_eq!(
            first("ns"),
            AnyValue::Datetime(3_600_000, TimeUnit::Microseconds, None)
        );
        assert_eq!(
            first("zoned"),
            AnyValue::Datetime(3_600_000_000, TimeUnit::Microseconds, None),
            "the UTC instant, not the wall time in New York"
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

    /// A name Avro refuses is made valid; a valid one never moves, even when a
    /// renamed one would land on it.
    #[test]
    fn names_are_made_valid_and_unique() {
        let names = avro_names(["my col", "2024", "a-b", "a_b", "délai", "", "_2024", "ok_1"]);
        assert_eq!(
            names,
            [
                "my_col", "_2024_2", "a_b_2", "a_b", "d_lai", "_", "_2024", "ok_1"
            ]
        );
        assert!(!renames("ok_1", &DataType::Int64));
        assert!(renames("my col", &DataType::Int64));
        let fields = |name: &str| {
            DataType::List(Box::new(DataType::Struct(vec![Field::new(
                name.into(),
                DataType::Int64,
            )])))
        };
        assert!(renames("ok", &fields("x y")));
        assert!(!renames("ok", &fields("x_y")));
    }

    /// Struct fields are renamed inside a list too, with their values and
    /// nulls, and the columns under them.
    #[test]
    fn struct_fields_are_renamed_at_any_depth() {
        let lf = df!("n" => [Some(1i64), None, Some(3)])
            .unwrap()
            .lazy()
            .select([
                as_struct(vec![col("n").alias("x y"), col("n").alias("x-y")])
                    .implode(true)
                    .alias("my list"),
                when(col("n").is_null())
                    .then(lit(NULL).cast(DataType::Struct(vec![Field::new(
                        "1st".into(),
                        DataType::Int64,
                    )])))
                    .otherwise(as_struct(vec![col("n").alias("1st")]))
                    .alias("point"),
            ]);
        let back = round_trip(lf);
        assert_eq!(
            back.column("my_list").unwrap().dtype(),
            &DataType::List(Box::new(DataType::Struct(vec![
                Field::new("x_y".into(), DataType::Int64),
                Field::new("x_y_2".into(), DataType::Int64),
            ])))
        );
        let items = back
            .column("my_list")
            .unwrap()
            .list()
            .unwrap()
            .get_as_series(0)
            .unwrap();
        let x = items.struct_().unwrap().field_by_name("x_y_2").unwrap();
        assert_eq!(
            x.i64().unwrap().iter().collect::<Vec<_>>(),
            [Some(1), None, Some(3)]
        );
        let point = back.column("point").unwrap();
        assert_eq!(point.null_count(), 1, "{point:?}");
        let first = point.struct_().unwrap().field_by_name("_1st").unwrap();
        assert_eq!(first.i64().unwrap().get(2), Some(3));
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
