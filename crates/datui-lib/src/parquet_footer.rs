//! What a Parquet file's footer says besides its rows: its row groups, each column's
//! compression and statistics, and who wrote it.
//!
//! Polars reads the footer to open the file and keeps it to itself, so the Info panel's
//! worker reads it again when the panel first opens ([`facts`]): a few KB at the end of
//! the file, the same bytes the open read, and none of its data.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;
use polars::prelude::Schema;
use polars_parquet::parquet::metadata::FileMetadata;
use polars_parquet::parquet::read::read_metadata;
use polars_parquet::parquet::schema::types::{
    PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, PrimitiveType, TimeUnit,
};
use polars_parquet::parquet::statistics::Statistics;

use crate::model_files::MetaValue;
use crate::numfmt::group_chrome;
use crate::text_formats::{Detail, count};

/// A file's footer, as read.
pub type Footer = Arc<FileMetadata>;

/// The footer of the Parquet file at `path`. `None` on error. Blocking.
pub fn read_parquet_metadata(path: &Path) -> Option<Footer> {
    let mut f = std::fs::File::open(path).ok()?;
    read_metadata(&mut f).ok().map(Arc::new)
}

/// The facts worker's read of a Parquet file: its footer, and the Parquet tab made from
/// it.
pub(crate) fn facts(path: &Path) -> Result<crate::readers::FormatFacts> {
    let mut file = std::fs::File::open(path)?;
    let footer = Arc::new(read_metadata(&mut file)?);
    Ok(crate::readers::FormatFacts {
        detail: Some(Arc::new(detail(&footer))),
        footer: Some(footer),
    })
}

/// Row groups shown one by one; past this many, the least and most rows of one.
const LISTED_GROUPS: usize = 8;
/// The longest a text statistic is shown.
const TEXT_STAT: usize = 40;

/// The Parquet tab: rows and row groups, compression and the writer, then each column's
/// least and greatest value and nulls, as the row groups' statistics say them.
pub fn detail(meta: &FileMetadata) -> Detail {
    let middot = crate::glyphs::get().middot;
    let times = crate::glyphs::get().times;
    let groups: Vec<usize> = meta.row_groups.iter().map(|g| g.num_rows()).collect();
    let mut lines = vec![format!(
        "{} in {}",
        count(meta.num_rows as u64, "row", "rows"),
        count(groups.len() as u64, "row group", "row groups")
    )];
    if groups.len() > 1 {
        let each = if groups.len() <= LISTED_GROUPS {
            groups
                .iter()
                .map(|n| group_chrome(*n))
                .collect::<Vec<_>>()
                .join(&format!(" {middot} "))
        } else {
            let least = groups.iter().min().copied().unwrap_or(0);
            let most = groups.iter().max().copied().unwrap_or(0);
            format!("{} to {}", group_chrome(least), group_chrome(most))
        };
        lines.push(format!("Rows per group: {each}"));
    }
    let (comp, uncomp) = overall_sizes(meta);
    if comp > 0 && uncomp > 0 {
        let mut codecs: Vec<String> = Vec::new();
        for cc in meta.row_groups.iter().flat_map(|g| g.parquet_columns()) {
            let codec = format!("{:?}", cc.compression()).to_lowercase();
            if !codecs.contains(&codec) {
                codecs.push(codec);
            }
        }
        lines.push(format!(
            "Compressed: {} of {} ({:.1}{times}), {}",
            crate::widgets::info::format_bytes(comp),
            crate::widgets::info::format_bytes(uncomp),
            uncomp as f64 / comp as f64,
            codecs.join(", ")
        ));
    }
    let mut writer = format!("Format version: {}", meta.version);
    if let Some(by) = &meta.created_by {
        writer.push_str(&format!(" {middot} created by {by}"));
    }
    lines.push(writer);
    if let Some(keys) = meta.key_value_metadata.as_ref().filter(|k| !k.is_empty()) {
        let names: Vec<&str> = keys.iter().map(|k| k.key.as_str()).collect();
        lines.push(format!("Metadata: {}", names.join(", ")));
    }
    let columns = meta.schema_descr.columns();
    let list = columns
        .iter()
        .enumerate()
        .map(|(i, column)| {
            let stats = meta
                .row_groups
                .iter()
                .map(|g| {
                    let stats = g
                        .parquet_columns()
                        .get(i)
                        .and_then(|cc| cc.statistics(&meta.footer_buf))
                        .and_then(|s| s.ok());
                    (stats, g.num_rows())
                })
                .collect::<Vec<_>>();
            let said = column_stats(&column.descriptor.primitive_type, &stats);
            (column.path_in_schema.join("."), MetaValue::Text(said))
        })
        .collect();
    Detail {
        tab: crate::text_formats::tab(crate::FileFormat::Parquet),
        lines,
        list_title: "Columns",
        list,
        ..Default::default()
    }
}

/// A value a statistic holds, ordered within its column.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum Value {
    Int(i128),
    Float(f64),
    Bytes(Vec<u8>),
}

/// One column's statistics across its row groups, each with its rows (`None` where a
/// group has none): `min 1, max 90, 3 nulls`. A bound is shown only where every group
/// that holds a value has one, and in the column's logical type where datui can say it.
fn column_stats(ty: &PrimitiveType, stats: &[(Option<Statistics>, usize)]) -> String {
    let dash = crate::glyphs::get().dash;
    // Bounds from some row groups would be read as the column's.
    if stats.is_empty() || stats.iter().any(|(s, _)| s.is_none()) {
        return "no statistics".to_string();
    }
    let bounds = |s: &Statistics| -> (Option<Value>, Option<Value>, Option<i64>) {
        match s {
            Statistics::Int32(s) => (
                s.min_value.map(|v| Value::Int(v.into())),
                s.max_value.map(|v| Value::Int(v.into())),
                s.null_count,
            ),
            Statistics::Int64(s) => (
                s.min_value.map(|v| Value::Int(v.into())),
                s.max_value.map(|v| Value::Int(v.into())),
                s.null_count,
            ),
            Statistics::Float(s) => (
                s.min_value.map(|v| Value::Float(v.into())),
                s.max_value.map(|v| Value::Float(v.into())),
                s.null_count,
            ),
            Statistics::Double(s) => (
                s.min_value.map(Value::Float),
                s.max_value.map(Value::Float),
                s.null_count,
            ),
            Statistics::Boolean(s) => (
                s.min_value.map(|v| Value::Int(v.into())),
                s.max_value.map(|v| Value::Int(v.into())),
                s.null_count,
            ),
            Statistics::Binary(s) => (
                s.min_value.clone().map(Value::Bytes),
                s.max_value.clone().map(Value::Bytes),
                s.null_count,
            ),
            Statistics::FixedLen(s) => (None, None, s.null_count),
            Statistics::Int96(s) => (None, None, s.null_count),
        }
    };
    let mut min: Option<Value> = None;
    let mut max: Option<Value> = None;
    let mut every_bound = true;
    let mut nulls: Option<i64> = Some(0);
    for (s, rows) in stats {
        let Some(s) = s else { continue };
        let (lo, hi, n) = bounds(s);
        match (lo, hi) {
            (Some(lo), Some(hi)) => {
                if min.as_ref().is_none_or(|m| lo < *m) {
                    min = Some(lo);
                }
                if max.as_ref().is_none_or(|m| hi > *m) {
                    max = Some(hi);
                }
            }
            // A group of nulls has no bounds to give.
            (None, None) if n.is_some_and(|n| n as usize == *rows) => {}
            _ => every_bound = false,
        }
        nulls = nulls.zip(n).map(|(a, b)| a + b);
    }
    let mut said = Vec::new();
    if every_bound
        && let (Some(min), Some(max)) = (min, max)
        && let (Some(lo), Some(hi)) = (shown(ty, &min), shown(ty, &max))
    {
        said.push(format!("min {lo}"));
        said.push(format!("max {hi}"));
    }
    match nulls {
        Some(n) => said.push(count(n.max(0) as u64, "null", "nulls")),
        None => said.push(format!("nulls {dash}")),
    }
    said.join(", ")
}

/// A statistic as its column's logical type reads it; `None` for a type whose bounds
/// datui does not decode (a decimal, a UUID, bytes).
fn shown(ty: &PrimitiveType, value: &Value) -> Option<String> {
    use PrimitiveLogicalType as L;
    let logical = ty.logical_type.or(match ty.converted_type {
        Some(PrimitiveConvertedType::Utf8) => Some(L::String),
        Some(PrimitiveConvertedType::Date) => Some(L::Date),
        Some(PrimitiveConvertedType::Decimal(p, s)) => Some(L::Decimal(p, s)),
        Some(PrimitiveConvertedType::TimestampMillis) => Some(L::Timestamp {
            unit: TimeUnit::Milliseconds,
            is_adjusted_to_utc: true,
        }),
        Some(PrimitiveConvertedType::TimestampMicros) => Some(L::Timestamp {
            unit: TimeUnit::Microseconds,
            is_adjusted_to_utc: true,
        }),
        _ => None,
    });
    match (logical, value) {
        (Some(L::String | L::Enum | L::Json), Value::Bytes(b)) => {
            let text = String::from_utf8_lossy(b);
            let mut shown: String = text.chars().take(TEXT_STAT).collect();
            if text.chars().count() > TEXT_STAT {
                shown.push_str(crate::glyphs::get().ellipsis);
            }
            Some(format!("\"{shown}\""))
        }
        (Some(L::Date), Value::Int(days)) => {
            let day = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?
                .checked_add_signed(chrono::Duration::days(i64::try_from(*days).ok()?))?;
            Some(day.to_string())
        }
        (Some(L::Timestamp { unit, .. }), Value::Int(n)) => {
            let n = i64::try_from(*n).ok()?;
            let at = match unit {
                TimeUnit::Milliseconds => chrono::DateTime::from_timestamp_millis(n),
                TimeUnit::Microseconds => chrono::DateTime::from_timestamp_micros(n),
                TimeUnit::Nanoseconds => Some(chrono::DateTime::from_timestamp_nanos(n)),
            }?;
            Some(at.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        (None | Some(L::Integer(_)), Value::Int(n))
            if ty.physical_type == PhysicalType::Boolean =>
        {
            Some((*n != 0).to_string())
        }
        (None | Some(L::Integer(_)), Value::Int(n)) => {
            let digits = group_chrome(usize::try_from(n.unsigned_abs()).unwrap_or(usize::MAX));
            Some(if *n < 0 { format!("-{digits}") } else { digits })
        }
        (None | Some(L::Float16), Value::Float(f)) => Some(format!("{f}")),
        _ => None,
    }
}

/// Per-column compression: codec name and ratio, for the columns of `schema`.
pub fn column_compression(meta: &FileMetadata, schema: &Schema) -> HashMap<String, (String, f64)> {
    let mut by_name: HashMap<String, (u64, u64)> = HashMap::new();
    let mut codec_by_name: HashMap<String, String> = HashMap::new();
    for rg in &meta.row_groups {
        for cc in rg.parquet_columns() {
            let name = cc
                .descriptor()
                .path_in_schema
                .first()
                .map(|s| s.as_ref())
                .unwrap_or("");
            let comp = cc.compressed_size() as u64;
            let uncomp = cc.uncompressed_size() as u64;
            let codec = format!("{:?}", cc.compression()).to_lowercase();
            let e = by_name.entry(name.to_string()).or_insert((0, 0));
            e.0 = e.0.saturating_add(comp);
            e.1 = e.1.saturating_add(uncomp);
            codec_by_name.insert(name.to_string(), codec);
        }
    }
    let mut out = HashMap::new();
    for (name, (comp, uncomp)) in by_name {
        if !schema.contains(&name) {
            continue;
        }
        let codec = codec_by_name
            .get(&name)
            .cloned()
            .unwrap_or_else(|| crate::glyphs::get().dash.to_string());
        if comp > 0 && uncomp > 0 {
            let ratio = uncomp as f64 / comp as f64;
            out.insert(name, (codec, ratio));
        }
    }
    out
}

/// Compressed and uncompressed bytes, from the row groups.
pub fn overall_sizes(meta: &FileMetadata) -> (u64, u64) {
    let mut comp: u64 = 0;
    let mut uncomp: u64 = 0;
    for rg in &meta.row_groups {
        comp = comp.saturating_add(rg.compressed_size() as u64);
        uncomp = uncomp.saturating_add(rg.total_byte_size() as u64);
    }
    (comp, uncomp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    /// The tab says the rows and groups, and each column's bounds and nulls from the
    /// statistics, in the column's type.
    #[test]
    fn the_tab_reads_the_footer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let mut df = df!(
            "n" => [Some(3i64), None, Some(-7), Some(40)],
            "s" => ["pear", "apple", "fig", "kiwi"],
            "d" => [Some(19_000i32), Some(19_001), None, None],
        )
        .unwrap()
        .lazy()
        .with_column(col("d").cast(DataType::Date))
        .collect()
        .unwrap();
        ParquetWriter::new(std::fs::File::create(&path).unwrap())
            .with_row_group_size(Some(2))
            .with_statistics(StatisticsOptions::full())
            .finish(&mut df)
            .unwrap();
        let facts = facts(&path).unwrap();
        let detail = facts.detail.unwrap();
        assert_eq!(detail.tab, "Parquet");
        assert_eq!(detail.lines[0], "4 rows in 2 row groups");
        assert!(
            detail.lines[1].starts_with("Rows per group: 2"),
            "{:?}",
            detail.lines
        );
        let said = |name: &str| match &detail.list.iter().find(|(k, _)| k == name).unwrap().1 {
            MetaValue::Text(t) => t.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(said("n"), "min -7, max 40, 1 null");
        assert_eq!(said("s"), "min \"apple\", max \"pear\", 0 nulls");
        assert_eq!(said("d"), "min 2022-01-08, max 2022-01-09, 2 nulls");
    }
}
