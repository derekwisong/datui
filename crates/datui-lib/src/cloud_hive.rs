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
    newest: bool,
) -> Result<Option<OsPath>> {
    if depth >= MAX_PARTITION_DEPTH {
        return Ok(None);
    }
    let result = store
        .list_with_delimiter(Some(prefix))
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Cloud list failed: {}", e))?;

    let mut objects: Vec<&OsPath> = result
        .objects
        .iter()
        .map(|o| &o.location)
        .filter(|l| crate::discover::is_parquet_key(l.as_ref()))
        .collect();
    objects.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let object = if newest {
        objects.last()
    } else {
        objects.first()
    };
    if let Some(object) = object {
        return Ok(Some((*object).clone()));
    }
    for common in &result.common_prefixes {
        if let Some((k, v)) = common.filename().and_then(|n| n.split_once('=')) {
            values.push((k.to_string(), v.to_string()));
        }
    }
    let mut partitions: Vec<&OsPath> = result
        .common_prefixes
        .iter()
        .filter(|c| c.as_ref().contains('='))
        .collect();
    partitions.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let partition = if newest {
        partitions.last()
    } else {
        partitions.first()
    };
    match partition {
        Some(partition) => {
            Box::pin(first_parquet_key_spine(
                store,
                partition,
                depth + 1,
                values,
                newest,
            ))
            .await
        }
        None => Ok(None),
    }
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

/// Infer (merged_schema, partition_columns) from the first and the last parquet file in
/// a cloud hive prefix, by name. Uses single-spine listings and reads only footers.
/// Datasets gain columns over time (the first day of a blockchain has no previous
/// block), so the last file's new columns are added after the first file's; the scan
/// fills them with nulls where older files lack them. Returns error on failure so
/// caller can fall back to collect_schema().
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
    let one_key = first_parquet_key_spine(&store, &prefix_path, 0, &mut values, false)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("No parquet file found in cloud hive prefix"))?;
    let mut file_schema = (*read_parquet_footer(&store, &one_key).await?.schema).clone();
    let mut newest_values = Vec::new();
    if let Some(newest) =
        first_parquet_key_spine(&store, &prefix_path, 0, &mut newest_values, true).await?
        && newest != one_key
    {
        let newest_schema = read_parquet_footer(&store, &newest).await?.schema;
        for (name, dtype) in newest_schema.iter() {
            if !file_schema.contains(name) {
                file_schema.with_column(name.clone(), dtype.clone());
            }
        }
    }
    values.extend(newest_values);
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

/// One data file of a cloud dataset: its key in the store and its size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFile {
    pub key: String,
    pub size: u64,
}

/// Every Parquet file under `prefix`, sorted by key, which is the order a scan of the
/// prefix reads them in. One listing, however deep the partitions go. Job files,
/// hidden files and empty objects are left out: none of them is data, and a scan that
/// tried to read one would fail.
pub async fn list_dataset_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Vec<DatasetFile>> {
    use futures::TryStreamExt;
    let prefix = prefix.trim_matches('/');
    let prefix_path = (!prefix.is_empty()).then(|| crate::cloud_browse::object_path(prefix));
    let objects: Vec<object_store::ObjectMeta> = store
        .list(prefix_path.as_ref())
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Cloud list failed: {}", e))?;
    let mut files: Vec<DatasetFile> = objects
        .into_iter()
        .filter(|o| o.size > 0)
        .map(|o| DatasetFile {
            key: o.location.as_ref().to_string(),
            size: o.size,
        })
        .filter(|f| {
            let name = f.key.rsplit('/').next().unwrap_or("");
            !name.starts_with(['_', '.']) && crate::discover::is_parquet_key(&f.key)
        })
        .collect();
    files.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(files)
}

/// The schema to scan a dataset's files with, and its partition columns.
///
/// Taken from the newest file (the last by key), since datasets grow: new columns, and
/// new fields inside nested ones. The columns only the first file has are added after.
/// The scan reads older files into it, filling what they lack with nulls (see
/// [`lenient_scan`]). The first day of Bitcoin blocks has no `previousblockhash`, and
/// the first days of its transactions no `inputs`; later `inputs` gain `address`, then
/// `txinwitness`.
pub async fn dataset_schema(
    store: &Arc<dyn ObjectStore>,
    files: &[DatasetFile],
) -> Result<(Arc<Schema>, Vec<String>)> {
    let (first, newest) = match files {
        [] => {
            return Err(color_eyre::eyre::eyre!(
                "No parquet file found in cloud prefix"
            ));
        }
        [only] => (only, only),
        [first, .., last] => (first, last),
    };
    let newest_path = crate::cloud_browse::object_path(&newest.key);
    let newest_footer = read_parquet_footer(store, &newest_path);
    let first_footer = async {
        if first.key == newest.key {
            Ok(None)
        } else {
            read_parquet_footer(store, &crate::cloud_browse::object_path(&first.key))
                .await
                .map(Some)
        }
    };
    let (newest_footer, first_footer) = futures::try_join!(newest_footer, first_footer)?;
    let mut file_schema = (*newest_footer.schema).clone();
    if let Some(first_footer) = first_footer {
        for (name, dtype) in first_footer.schema.iter() {
            if !file_schema.contains(name) {
                file_schema.with_column(name.clone(), dtype.clone());
            }
        }
    }
    let partition_columns = partition_columns_from_prefix(&newest.key);
    let values: Vec<(String, String)> = [&first.key, &newest.key]
        .iter()
        .flat_map(|key| key.split('/'))
        .filter_map(|segment| segment.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
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

/// How many footers are read at once when counting.
const FOOTERS_AT_ONCE: usize = 64;
/// The first read of a footer. Most footers fit; a larger one costs a second request.
const COUNT_TAIL_BYTES: u64 = 16 * 1024;

/// The rows in each row group of every file, in file order, from their footers: a small
/// ranged read at the end of each file, many at once. No data is read.
pub async fn row_groups_of_files(
    store: &Arc<dyn ObjectStore>,
    files: &[DatasetFile],
) -> Result<Vec<Vec<usize>>> {
    let permits = Arc::new(tokio::sync::Semaphore::new(FOOTERS_AT_ONCE));
    let mut reads = tokio::task::JoinSet::new();
    for (index, file) in files.iter().cloned().enumerate() {
        let (store, permits) = (store.clone(), permits.clone());
        reads.spawn(async move {
            let _permit = permits.acquire_owned().await;
            (index, row_groups_of_file(&store, &file).await)
        });
    }
    let mut out = vec![Vec::new(); files.len()];
    while let Some(joined) = reads.join_next().await {
        let (index, groups) = joined.map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        out[index] = groups?;
    }
    Ok(out)
}

async fn row_groups_of_file(
    store: &Arc<dyn ObjectStore>,
    file: &DatasetFile,
) -> Result<Vec<usize>> {
    let path = crate::cloud_browse::object_path(&file.key);
    let tail_start = file.size.saturating_sub(COUNT_TAIL_BYTES);
    let tail = store
        .get_range(&path, tail_start..file.size)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Cloud read failed: {}", e))?;
    let footer_len = footer_length(&tail)
        .ok_or_else(|| color_eyre::eyre::eyre!("{} is not a Parquet file", file.key))?;
    let needed = footer_len + 8;
    let tail = if needed as usize <= tail.len() {
        tail
    } else {
        store
            .get_range(&path, file.size.saturating_sub(needed)..file.size)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Cloud read failed: {}", e))?
    };
    let mut cursor = Cursor::new(tail.as_ref());
    let metadata = ParquetReader::new(&mut cursor)
        .get_metadata()
        .map_err(|e| color_eyre::eyre::eyre!("Parquet footer read failed: {}", e))?
        .clone();
    Ok(metadata.row_groups.iter().map(|rg| rg.num_rows()).collect())
}

/// The length of the footer metadata, from the last eight bytes of a Parquet file: a
/// little-endian length, then `PAR1`.
fn footer_length(tail: &[u8]) -> Option<u64> {
    let end = tail.len().checked_sub(8)?;
    if &tail[end + 4..] != b"PAR1" {
        return None;
    }
    let bytes: [u8; 4] = tail[end..end + 4].try_into().ok()?;
    Some(u32::from_le_bytes(bytes) as u64)
}

/// A scan of `urls` into `schema` that reads files written at different times:
/// columns and nested fields a file lacks are filled with nulls, ones it has beyond the
/// schema are ignored, and integers and floats widen. Polars' `scan_parquet` offers
/// only the first of those, and a Bitcoin transactions file from 2015 fails against the
/// 2026 schema without the rest.
pub fn lenient_scan(
    urls: &[String],
    schema: Arc<Schema>,
    cloud_options: Option<polars::io::cloud::CloudOptions>,
) -> polars::prelude::PolarsResult<polars::prelude::LazyFrame> {
    use polars::lazy::dsl::{
        CastColumnsPolicy, DslBuilder, ExtraColumnsPolicy, MissingColumnsPolicy, ScanSources,
        UnifiedScanArgs,
    };
    let sources = ScanSources::Paths(
        urls.iter()
            .map(|url| polars::prelude::PlRefPath::new(url.as_str()))
            .collect(),
    );
    let options = polars::io::parquet::read::ParquetOptions {
        schema: Some(schema),
        ..Default::default()
    };
    let args = UnifiedScanArgs {
        cloud_options,
        hive_options: polars::io::HiveOptions::new_enabled(),
        glob: false,
        cast_columns_policy: CastColumnsPolicy {
            integer_upcast: true,
            float_upcast: true,
            datetime_nanoseconds_downcast: true,
            datetime_microseconds_downcast: true,
            missing_struct_fields: MissingColumnsPolicy::Insert,
            extra_struct_fields: ExtraColumnsPolicy::Ignore,
            ..CastColumnsPolicy::ERROR_ON_MISMATCH
        },
        missing_columns_policy: MissingColumnsPolicy::Insert,
        extra_columns_policy: ExtraColumnsPolicy::Ignore,
        ..Default::default()
    };
    Ok(DslBuilder::scan_parquet(sources, options, args)?
        .build()
        .into())
}

/// The URL of `key` in the same bucket or container as `url`.
pub fn url_of_key(url: &str, key: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    let root = rest.split('/').next()?;
    Some(format!("{scheme}://{root}/{key}"))
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

    /// Two days of a dataset whose files grew: the first has no `fee` and a struct
    /// without `address`; the second has both. Plus the clutter a listing turns up
    /// beside the data.
    fn evolving_dataset() -> Vec<(String, Vec<u8>)> {
        use polars::prelude::{IntoSeries, ParquetWriter, StructChunked, df};
        let write = |mut df: polars::prelude::DataFrame| {
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut df).unwrap();
            bytes
        };
        let old_input = StructChunked::from_series(
            "input".into(),
            2,
            [Series::new("value".into(), &[1.0f64, 2.0])].iter(),
        )
        .unwrap()
        .into_series();
        let old = df!("id" => &[1i64, 2], "input" => old_input).unwrap();
        let new_input = StructChunked::from_series(
            "input".into(),
            5,
            [
                Series::new("value".into(), &[3.0f64, 4.0, 5.0, 6.0, 7.0]),
                Series::new("address".into(), &["a", "b", "c", "d", "e"]),
            ]
            .iter(),
        )
        .unwrap()
        .into_series();
        let new = df!("id" => &[3i64, 4, 5, 6, 7], "fee" => &[10i64, 20, 30, 40, 50], "input" => new_input).unwrap();
        vec![
            (
                "data/date=2009-01-03/part-0.parquet".to_string(),
                write(old),
            ),
            (
                "data/date=2026-09-17/part-0.parquet".to_string(),
                write(new),
            ),
            ("data/date=2026-09-17/_SUCCESS".to_string(), b"x".to_vec()),
            (
                "data/date=2026-09-17/.part-0.parquet.crc".to_string(),
                b"crc".to_vec(),
            ),
        ]
    }

    #[test]
    fn a_dataset_is_listed_once_and_counted_from_its_footers() {
        use object_store::PutPayload;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for (key, bytes) in evolving_dataset() {
                store
                    .put(&OsPath::from(key.as_str()), PutPayload::from(bytes))
                    .await
                    .unwrap();
            }
            store
                .put(
                    &OsPath::from("elsewhere/x.parquet"),
                    PutPayload::from(vec![1u8]),
                )
                .await
                .unwrap();
            let files = list_dataset_files(&store, "data/").await.unwrap();
            let keys: Vec<&str> = files.iter().map(|f| f.key.as_str()).collect();
            assert_eq!(
                keys,
                [
                    "data/date=2009-01-03/part-0.parquet",
                    "data/date=2026-09-17/part-0.parquet"
                ]
            );

            let groups = row_groups_of_files(&store, &files).await.unwrap();
            assert_eq!(groups, [vec![2], vec![5]]);

            let (schema, partitions) = dataset_schema(&store, &files).await.unwrap();
            assert_eq!(partitions, ["date"]);
            let names: Vec<&str> = schema.iter_names().map(|n| n.as_str()).collect();
            assert_eq!(
                names,
                ["date", "id", "fee", "input"],
                "the newest file's columns"
            );
            assert!(
                format!("{:?}", schema.get("input").unwrap()).contains("address"),
                "and its struct fields"
            );
        });
    }

    #[test]
    fn a_lenient_scan_reads_files_written_years_apart() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut urls = Vec::new();
        for (key, bytes) in evolving_dataset().into_iter().take(2) {
            let path = dir.path().join(&key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, bytes).unwrap();
            urls.push(path.to_string_lossy().into_owned());
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let (schema, _) = rt.block_on(async {
            let files = list_dataset_files(&store, "data").await.unwrap();
            dataset_schema(&store, &files).await.unwrap()
        });
        let df = lenient_scan(&urls, schema, None)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(df.height(), 7);
        let fees = df.column("fee").unwrap();
        assert_eq!(fees.null_count(), 2, "the old file has no fee");
        let second_file = lenient_scan(&urls[1..], df.schema().clone(), None)
            .unwrap()
            .slice(3, 2)
            .collect()
            .unwrap();
        assert_eq!(
            second_file
                .column("id")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            [6, 7]
        );
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
