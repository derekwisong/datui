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
//! And it writes names as they are, with an empty record name, but an Avro name
//! is `[A-Za-z_][A-Za-z0-9_]*` and strict readers refuse the file. [`write`]
//! names the record and gives each column and struct field a valid name in the
//! file's schema, with the original as the field's `doc`. It also writes the
//! header once, where Polars' writer repeats it for every chunk, and cuts
//! blocks by size rather than one per chunk.

use std::io::Write;

use polars::prelude::*;
use polars_arrow::io::avro::avro_schema::file::CompressedBlock;
use polars_arrow::io::avro::avro_schema::schema::{Field as AvroField, Schema as AvroSchema};
use polars_arrow::io::avro::{avro_schema, write as avro_write};

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

/// Whether an Avro export renames this column or a struct field inside it.
pub fn renames(name: &str, dtype: &DataType) -> bool {
    fn inside(dtype: &DataType) -> bool {
        match dtype {
            DataType::List(inner) | DataType::Array(inner, _) => inside(inner),
            DataType::Struct(fields) => fields.iter().any(|f| renames(f.name(), f.dtype())),
            _ => false,
        }
    }
    !is_avro_name(name) || inside(dtype)
}

/// `fields` under valid Avro names at any depth, each renamed one keeping its
/// original name as its `doc`. Only the schema changes: values are written by
/// position.
fn name_fields(fields: &mut [AvroField]) {
    fn name_schema(schema: &mut AvroSchema) {
        match schema {
            AvroSchema::Union(branches) => branches.iter_mut().for_each(name_schema),
            AvroSchema::Array(items) | AvroSchema::Map(items) => name_schema(items),
            AvroSchema::Record(record) => name_fields(&mut record.fields),
            _ => {}
        }
    }
    let names = avro_names(fields.iter().map(|f| f.name.as_str()));
    for (field, name) in fields.iter_mut().zip(names) {
        if field.name != name {
            field.doc = Some(std::mem::replace(&mut field.name, name));
        }
        name_schema(&mut field.schema);
    }
}

/// A block is cut once it holds this many bytes, however the frame is chunked:
/// a reader holds a whole block in memory, and Java's refuses one past 2 GiB.
const BLOCK_BYTES: usize = 1 << 20;

/// Write `df`, prepared by [`lazy_for_avro`], as an uncompressed Avro file:
/// Polars' `AvroWriter`'s encoding, with valid names, one header, and blocks
/// of about [`BLOCK_BYTES`].
pub fn write(df: &mut DataFrame, mut writer: impl Write) -> PolarsResult<()> {
    // Serializing walks the columns' chunks together, so they must line up.
    df.align_chunks_par();
    let schema = df.schema().to_arrow(CompatLevel::oldest());
    let mut record = avro_write::to_record(&schema, RECORD_NAME.to_string())?;
    name_fields(&mut record.fields);
    // avro-schema turns any I/O error into "OutOfSpec", so it only ever writes
    // to `out` and the file's errors, such as a full disk, come from here.
    let mut out = Vec::new();
    avro_schema::write::write_metadata(&mut out, record.clone(), None)?;
    writer.write_all(&out)?;

    let mut block = CompressedBlock::default();
    let mut flush = |block: &mut CompressedBlock| -> PolarsResult<()> {
        out.clear();
        avro_schema::write::write_block(&mut out, block)?;
        writer.write_all(&out)?;
        block.data.clear();
        block.number_of_rows = 0;
        Ok(())
    };
    for chunk in df.iter_chunks(CompatLevel::oldest(), true) {
        let mut serializers: Vec<_> = chunk
            .iter()
            .zip(&record.fields)
            .map(|(array, field)| avro_write::new_serializer(array.as_ref(), &field.schema))
            .collect();
        // Row by row, as Polars' `serialize` does, cutting a block by size.
        for _ in 0..chunk.height() {
            for serializer in &mut serializers {
                let value = serializer.next().expect("a value for every row");
                block.data.extend_from_slice(value);
            }
            block.number_of_rows += 1;
            if block.data.len() >= BLOCK_BYTES {
                flush(&mut block)?;
            }
        }
    }
    if block.number_of_rows > 0 {
        flush(&mut block)?;
    }
    Ok(())
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
    use polars::io::avro::AvroReader;

    fn written(lf: LazyFrame) -> Vec<u8> {
        let mut df = lazy_for_avro(lf).unwrap().collect().unwrap();
        let mut bytes = Vec::new();
        write(&mut df, &mut bytes).unwrap();
        bytes
    }

    fn round_trip(lf: LazyFrame) -> DataFrame {
        AvroReader::new(std::io::Cursor::new(written(lf)))
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
                as_struct(vec![
                    col("n").alias("x y"),
                    (col("n") * lit(10)).alias("x-y"),
                ])
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
        let field = |name: &str| {
            let values = items.struct_().unwrap().field_by_name(name).unwrap();
            values.i64().unwrap().iter().collect::<Vec<_>>()
        };
        assert_eq!(field("x_y"), [Some(1), None, Some(3)]);
        assert_eq!(field("x_y_2"), [Some(10), None, Some(30)]);
        let point = back.column("point").unwrap();
        assert_eq!(point.null_count(), 1, "{point:?}");
        let first = point.struct_().unwrap().field_by_name("_1st").unwrap();
        assert_eq!(first.i64().unwrap().get(2), Some(3));
    }

    /// A renamed field keeps its original name as its doc, nested ones too, and
    /// a frame of several chunks is one header and a block for each.
    #[test]
    fn originals_are_docs_and_chunks_share_one_header() {
        let part = df!("my col" => [1i64], "ok" => [2i64])
            .unwrap()
            .lazy()
            .with_column(as_struct(vec![col("ok").alias("x y")]).alias("point"))
            .collect()
            .unwrap();
        let mut df = part.clone();
        df.vstack_mut(&part).unwrap();
        assert_eq!(df.first_col_n_chunks(), 2);
        let mut bytes = Vec::new();
        write(&mut df, &mut bytes).unwrap();

        let record = avro_schema::read::read_metadata(&mut std::io::Cursor::new(&bytes))
            .unwrap()
            .record;
        assert_eq!(record.name, RECORD_NAME);
        let docs = |fields: &[AvroField]| -> Vec<(String, Option<String>)> {
            fields
                .iter()
                .map(|f| (f.name.clone(), f.doc.clone()))
                .collect()
        };
        assert_eq!(
            docs(&record.fields),
            [
                ("my_col".to_string(), Some("my col".to_string())),
                ("ok".to_string(), None),
                ("point".to_string(), None),
            ]
        );
        let AvroSchema::Union(branches) = &record.fields[2].schema else {
            panic!("{:?}", record.fields[2].schema);
        };
        let AvroSchema::Record(point) = &branches[1] else {
            panic!("{branches:?}");
        };
        assert_eq!(
            docs(&point.fields),
            [("x_y".to_string(), Some("x y".to_string()))]
        );

        let back = AvroReader::new(std::io::Cursor::new(bytes))
            .finish()
            .unwrap();
        let my_col = back.column("my_col").unwrap().i64().unwrap();
        assert_eq!(my_col.iter().collect::<Vec<_>>(), [Some(1), Some(1)]);
    }

    /// Blocks are cut by size, not by chunk: a hundred one-row chunks are one
    /// block, and one chunk of about 3 MiB is three, and every row reads back.
    #[test]
    fn blocks_are_cut_by_size() {
        use avro_schema::read::fallible_streaming_iterator::FallibleStreamingIterator;
        fn blocks(df: &mut DataFrame) -> Vec<(usize, usize)> {
            let mut bytes = Vec::new();
            write(df, &mut bytes).unwrap();
            let back = AvroReader::new(std::io::Cursor::new(&bytes))
                .finish()
                .unwrap();
            assert!(back.equals(df), "{back:?}");
            let mut reader = std::io::Cursor::new(bytes);
            let marker = avro_schema::read::read_metadata(&mut reader)
                .unwrap()
                .marker;
            let mut iter = avro_schema::read::block_iterator(reader, None, marker);
            let mut sizes = Vec::new();
            while let Some(block) = iter.next().unwrap() {
                sizes.push((block.number_of_rows, block.data.len()));
            }
            sizes
        }
        let text = "x".repeat(1000);
        let row = df!("s" => [text.as_str()]).unwrap();
        let mut many = row.clone();
        for _ in 0..99 {
            many.vstack_mut(&row).unwrap();
        }
        assert_eq!(many.first_col_n_chunks(), 100);
        let sizes = blocks(&mut many);
        assert_eq!(sizes.len(), 1, "{sizes:?}");
        assert_eq!(sizes[0].0, 100);

        let mut big = df!("s" => vec![text.as_str(); 3000]).unwrap();
        assert_eq!(big.first_col_n_chunks(), 1);
        let sizes = blocks(&mut big);
        assert_eq!(sizes.len(), 3, "{sizes:?}");
        assert_eq!(sizes.iter().map(|(rows, _)| rows).sum::<usize>(), 3000);
        for (_, size) in &sizes[..2] {
            assert!(
                (BLOCK_BYTES..BLOCK_BYTES + 1100).contains(size),
                "{sizes:?}"
            );
        }
    }

    /// A failed write reports the I/O error, not avro-schema's "OutOfSpec".
    #[test]
    fn a_write_error_says_what_failed() {
        struct Full;
        impl Write for Full {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("disk full"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut df = df!("n" => [1i64]).unwrap();
        let err = write(&mut df, Full).unwrap_err().to_string();
        assert!(err.contains("disk full"), "{err}");
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
