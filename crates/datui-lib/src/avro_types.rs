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
mod tests;
