//! Cloud Hive schema fast path: infer schema from one Parquet file (metadata only) for S3/GCS
//! to avoid slow collect_schema() over many files. Single-spine listing + footer read.

use color_eyre::Result;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt};
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
    values: &mut Vec<(String, String)>,
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
        if crate::discover::is_parquet_key(loc) {
            return Ok(Some(obj.location.clone()));
        }
    }
    for common in &result.common_prefixes {
        if let Some((k, v)) = common.filename().and_then(|n| n.split_once('=')) {
            values.push((k.to_string(), v.to_string()));
        }
    }
    for common in &result.common_prefixes {
        let s = common.as_ref();
        if s.contains('=') {
            return Box::pin(first_parquet_key_spine(store, common, depth + 1, values)).await;
        }
    }
    Ok(None)
}

/// Discover partition column names from the first common prefix at each level (single spine).
fn partition_columns_from_prefix(prefix_str: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    for segment in prefix_str.split('/') {
        if let Some((key, _)) = segment.split_once('=')
            && !key.is_empty()
            && seen.insert(key.to_string())
        {
            columns.push(key.to_string());
        }
    }
    columns
}

/// What one Parquet footer says about its object, short of the data.
pub struct ParquetFooter {
    pub schema: Arc<Schema>,
    /// Rows in each row group, in file order.
    pub row_group_rows: Vec<usize>,
    /// Uncompressed bytes per row of each column, averaged over the file and summed
    /// over a nested column's leaves. The schema gives a fixed-size column's width; a
    /// string's or a nested column's is only known from here.
    pub column_bytes_per_row: Vec<(String, usize)>,
}

/// Read the Parquet footer at the end of `tail_bytes`. The slice must be the tail of the
/// file.
fn footer_from_parquet_tail(tail_bytes: &[u8]) -> Result<ParquetFooter> {
    let mut cursor = Cursor::new(tail_bytes);
    let mut reader = ParquetReader::new(&mut cursor);
    let arrow_schema = reader
        .schema()
        .map_err(|e| color_eyre::eyre::eyre!("Parquet schema read failed: {}", e))?;
    let metadata = reader
        .get_metadata()
        .map_err(|e| color_eyre::eyre::eyre!("Parquet row count read failed: {}", e))?;
    let schema = Schema::from_arrow_schema(arrow_schema.as_ref());
    let row_group_rows: Vec<usize> = metadata.row_groups.iter().map(|rg| rg.num_rows()).collect();
    let rows: usize = row_group_rows.iter().sum();
    let column_bytes_per_row = schema
        .iter_names()
        .filter_map(|name| {
            let bytes: i64 = metadata
                .row_groups
                .iter()
                .flat_map(|rg| rg.columns_under_root_iter(name).into_iter().flatten())
                .map(|chunk| chunk.uncompressed_size())
                .sum();
            (rows > 0).then(|| (name.to_string(), (bytes.max(0) as usize) / rows))
        })
        .collect();
    Ok(ParquetFooter {
        schema: Arc::new(schema),
        row_group_rows,
        column_bytes_per_row,
    })
}

/// One object's footer, from a single tail read. Does not fetch the data.
pub async fn footer_of_cloud_parquet(
    store: Arc<dyn ObjectStore>,
    key: &str,
) -> Result<ParquetFooter> {
    read_parquet_footer(&store, &crate::cloud_browse::object_path(key)).await
}

/// Fetch the tail of one object and read its footer. Does not fetch the full file.
async fn read_parquet_footer(store: &Arc<dyn ObjectStore>, path: &OsPath) -> Result<ParquetFooter> {
    let meta = store
        .head(path)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Cloud head failed: {}", e))?;
    let size = meta.size;
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
    footer_from_parquet_tail(&tail)
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
        crate::cloud_browse::object_path(prefix_trimmed)
    };
    let mut values = Vec::new();
    let one_key = first_parquet_key_spine(&store, &prefix_path, 0, &mut values)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("No parquet file found in cloud hive prefix"))?;
    let file_schema = read_parquet_footer(&store, &one_key).await?.schema;
    let key_str = one_key.as_ref();
    let partition_columns = partition_columns_from_prefix(key_str);
    let part_set: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
    let mut merged = Schema::with_capacity(partition_columns.len() + file_schema.len());
    for name in &partition_columns {
        merged.with_column(
            name.clone().into(),
            crate::widgets::datatable::partition_dtype(name, &file_schema, &values),
        );
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
    use polars::prelude::{NamedFrom, Series};

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
    fn footer_from_parquet_tail_invalid_returns_err() {
        let invalid = vec![0u8; 100];
        let r = footer_from_parquet_tail(&invalid);
        assert!(r.is_err());
    }

    #[test]
    fn footer_from_parquet_tail_reads_schema_and_row_count() {
        use polars::prelude::{ParquetWriter, df};
        let mut df = df!("a" => &[1i32, 2, 3], "b" => &["x", "y", "z"]).unwrap();
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes).finish(&mut df).unwrap();
        let footer = footer_from_parquet_tail(&bytes).unwrap();
        assert_eq!(footer.row_group_rows, [3]);
        assert_eq!(footer.schema.len(), 2);
    }

    #[test]
    fn footer_from_parquet_tail_reads_row_groups_and_column_widths() {
        use polars::prelude::{ParquetWriter, df};
        let ids: Vec<i32> = (0..1000).collect();
        let notes: Vec<String> = ids.iter().map(|i| format!("note-{i:04}")).collect();
        let lists: Vec<Series> = ids
            .iter()
            .map(|i| Series::new("".into(), &[*i as f64; 10]))
            .collect();
        let mut df = df!("id" => ids, "note" => notes, "list" => lists).unwrap();
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes)
            .with_row_group_size(Some(400))
            .finish(&mut df)
            .unwrap();
        let footer = footer_from_parquet_tail(&bytes).unwrap();
        assert!(
            footer.row_group_rows.len() > 1,
            "{:?}",
            footer.row_group_rows
        );
        assert_eq!(footer.row_group_rows.iter().sum::<usize>(), 1000);
        let width = |column: &str| {
            footer
                .column_bytes_per_row
                .iter()
                .find(|(n, _)| n == column)
                .map(|(_, w)| *w)
                .unwrap_or_else(|| panic!("no width for {column}"))
        };
        // Nine characters, the length prefix, and the page headers spread over the rows.
        assert!(
            (9..=20).contains(&width("note")),
            "note width {}",
            width("note")
        );
        // Ten floats a row, so the nested column is not guessed at.
        assert!(
            (80..=120).contains(&width("list")),
            "list width {}",
            width("list")
        );
        assert!(width("id") >= 4);
    }
}
