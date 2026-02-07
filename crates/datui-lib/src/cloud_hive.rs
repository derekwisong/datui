//! Cloud Hive schema fast path: infer schema from one Parquet file (metadata only) for S3/GCS
//! to avoid slow collect_schema() over many files. Single-spine listing + footer read.

use color_eyre::Result;
use object_store::path::Path as OsPath;
use object_store::ObjectStore;
use polars::prelude::{ParquetReader, Schema, SchemaExt, SerReader};
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

const MAX_PARTITION_DEPTH: usize = 64;
const PARQUET_FOOTER_TAIL_BYTES: usize = 256 * 1024;

/// Find the first parquet object key along a single spine of a hive-style prefix.
/// Uses list_with_delimiter to walk one branch (first partition value at each level).
async fn first_parquet_key_spine(
    store: &Arc<dyn ObjectStore>,
    prefix: &OsPath,
    depth: usize,
) -> Result<Option<OsPath>> {
    if depth >= MAX_PARTITION_DEPTH {
        return Ok(None);
    }
    let result = store
        .list_with_delimiter(Some(prefix))
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Cloud list failed: {}", e))?;

    for obj in &result.objects {
        let loc = obj.location.as_ref();
        if loc.ends_with(".parquet") {
            return Ok(Some(obj.location.clone()));
        }
    }
    for common in &result.common_prefixes {
        let s = common.as_ref();
        if s.contains('=') {
            return Box::pin(first_parquet_key_spine(store, common, depth + 1)).await;
        }
    }
    Ok(None)
}

/// Discover partition column names from the first common prefix at each level (single spine).
fn partition_columns_from_prefix(prefix_str: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    for segment in prefix_str.split('/') {
        if let Some((key, _)) = segment.split_once('=') {
            if !key.is_empty() && seen.insert(key.to_string()) {
                columns.push(key.to_string());
            }
        }
    }
    columns
}

/// Read Parquet schema from the last N bytes of a file (footer). The slice must be the tail of the file.
fn schema_from_parquet_footer_tail(tail_bytes: &[u8]) -> Result<Schema> {
    let mut cursor = Cursor::new(tail_bytes);
    let mut reader = ParquetReader::new(&mut cursor);
    let arrow_schema = reader
        .schema()
        .map_err(|e| color_eyre::eyre::eyre!("Parquet schema read failed: {}", e))?;
    Ok(Schema::from_arrow_schema(arrow_schema.as_ref()))
}

/// Fetch the tail of one object and return its schema. Does not fetch full file.
async fn read_schema_from_cloud_parquet(
    store: &Arc<dyn ObjectStore>,
    path: &OsPath,
) -> Result<Schema> {
    let meta = store
        .head(path)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Cloud head failed: {}", e))?;
    let size = meta.size as u64;
    let start = size.saturating_sub(PARQUET_FOOTER_TAIL_BYTES as u64);
    let range = start..size;
    let ranges = store
        .get_ranges(path, &[range])
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Cloud get_ranges failed: {}", e))?;
    let tail = ranges
        .into_iter()
        .next()
        .ok_or_else(|| color_eyre::eyre::eyre!("Empty range response"))?;
    schema_from_parquet_footer_tail(&tail)
}

/// Infer (merged_schema, partition_columns) from one parquet file in a cloud hive prefix.
/// Uses single-spine listing and reads only parquet footer. Returns error on failure so caller can fall back to collect_schema().
pub async fn schema_from_one_cloud_hive(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<(Arc<Schema>, Vec<String>)> {
    let prefix_trimmed = prefix.trim_end_matches('/');
    let prefix_path = if prefix_trimmed.is_empty() {
        OsPath::default()
    } else {
        OsPath::from(prefix_trimmed)
    };
    let one_key = first_parquet_key_spine(&store, &prefix_path, 0)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("No parquet file found in cloud hive prefix"))?;
    let file_schema = read_schema_from_cloud_parquet(&store, &one_key).await?;
    let key_str = one_key.as_ref();
    let partition_columns = partition_columns_from_prefix(key_str);
    let part_set: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
    let mut merged = Schema::with_capacity(partition_columns.len() + file_schema.len());
    for name in &partition_columns {
        merged.with_column(name.clone().into(), polars::datatypes::DataType::String);
    }
    for (name, dtype) in file_schema.iter() {
        if !part_set.contains(name.as_str()) {
            merged.with_column(name.clone(), dtype.clone());
        }
    }
    Ok((Arc::new(merged), partition_columns))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_columns_from_prefix_basic() {
        let cols = partition_columns_from_prefix("dataset/year=2024/month=01");
        assert_eq!(cols, ["year", "month"]);
    }

    #[test]
    fn partition_columns_from_prefix_with_trailing_slash() {
        let cols = partition_columns_from_prefix("path/year=2024/month=01/day=15/");
        assert_eq!(cols, ["year", "month", "day"]);
    }

    #[test]
    fn partition_columns_from_prefix_dedup() {
        let cols = partition_columns_from_prefix("a/x=1/x=2");
        assert_eq!(cols, ["x"]);
    }

    #[test]
    fn partition_columns_from_prefix_empty() {
        let cols = partition_columns_from_prefix("");
        assert!(cols.is_empty());
    }

    #[test]
    fn schema_from_parquet_footer_tail_invalid_returns_err() {
        let invalid = vec![0u8; 100];
        let r = schema_from_parquet_footer_tail(&invalid);
        assert!(r.is_err());
    }
}
