//! Cloud Hive schema fast path: infer schema from one Parquet file (metadata only) for S3/GCS
//! to avoid slow collect_schema() over many files. Single-spine listing + footer read.

use color_eyre::Result;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt};
use polars::prelude::{ParquetReader, Schema, SchemaExt, SerReader};
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

use crate::schema_union::FileSchema;
pub use crate::schema_union::lenient_scan;
use crate::schema_union::with_partition_columns;

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
/// Every column any file has, from the footers the row count already reads, so a column
/// a vendor added for a month is visible rather than hidden behind whichever file the
/// schema was taken from. See [`crate::schema_union`] for the ordering and the type
/// rules; [`lenient_scan`] does the reading.
pub fn dataset_schema_from_footers(
    files: &[DatasetFile],
    read: &[usize],
    footers: &[Option<FileFooter>],
) -> Result<(crate::schema_union::DatasetSchema, Vec<String>)> {
    let (first, newest) = match files {
        [] => {
            return Err(color_eyre::eyre::eyre!(
                "No parquet file found in cloud prefix"
            ));
        }
        [only] => (only, only),
        [first, .., last] => (first, last),
    };
    // The footers come back in the order of `read`, which indexes `files`: that is
    // where an object's size is, since a footer is a read of the tail and says nothing
    // about how long the object is. With a sampled read those two orders are not the
    // same list, so the index has to come from `read` and not from the position.
    let per_file: Vec<Option<FileSchema>> = footers
        .iter()
        .zip(read)
        .map(|(f, index)| {
            f.as_ref().map(|f| FileSchema {
                schema: f.schema.clone(),
                rows: f.row_group_rows.iter().sum(),
                file_bytes: files.get(*index).map(|f| f.size as usize).unwrap_or(0),
                row_group_bytes: f.row_group_bytes.clone(),
            })
        })
        .collect();
    let mut union = crate::schema_union::union_sampled(files.len(), read, &per_file);
    if union.schema.is_empty() {
        return Err(color_eyre::eyre::eyre!(
            "No readable parquet footer in cloud prefix"
        ));
    }

    let partition_columns = partition_columns_from_prefix(&newest.key);
    let values: Vec<(String, String)> = [&first.key, &newest.key]
        .iter()
        .flat_map(|key| key.split('/'))
        .filter_map(|segment| segment.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    union.schema = Arc::new(with_partition_columns(
        &union.schema,
        &partition_columns,
        &values,
    ));
    Ok((union, partition_columns))
}

/// How many footers are read at once when counting.
const FOOTERS_AT_ONCE: usize = 64;
/// The first read of a footer. Most footers fit; a larger one costs a second request.
const COUNT_TAIL_BYTES: u64 = 16 * 1024;

/// What one file's footer says: the columns it has, and the rows in each row group.
/// Both come from the same tail read, so knowing every file's columns costs the dataset
/// nothing beyond the count it already pays for.
#[derive(Debug, Clone)]
pub struct FileFooter {
    pub schema: Arc<Schema>,
    pub row_group_rows: Vec<usize>,
    /// Compressed bytes of each row group, in the same order.
    pub row_group_bytes: Vec<usize>,
}

/// Every file's footer, in file order: a small ranged read at the end of each file,
/// many at once. No data is read. A file whose footer cannot be read is `None` rather
/// than an error, so one object mid-write does not stop the dataset from opening.
pub async fn footers_of_files(
    store: &Arc<dyn ObjectStore>,
    files: &[DatasetFile],
    read: &[usize],
) -> Vec<Option<FileFooter>> {
    footers_of_files_reporting(
        store,
        files,
        read,
        &crate::schema_union::FooterProgress::default(),
    )
    .await
}

/// As [`footers_of_files`], counting each footer off against `progress` as it lands.
///
/// This is the pass the loading screen has most reason to narrate: every footer is a
/// ranged read over the network, sixty-four at a time, and a prefix of a few thousand
/// objects spends seconds here.
pub async fn footers_of_files_reporting(
    store: &Arc<dyn ObjectStore>,
    files: &[DatasetFile],
    read: &[usize],
    progress: &crate::schema_union::FooterProgress,
) -> Vec<Option<FileFooter>> {
    progress.begin(read.len());
    let permits = Arc::new(tokio::sync::Semaphore::new(FOOTERS_AT_ONCE));
    let mut reads = tokio::task::JoinSet::new();
    for (slot, file) in read
        .iter()
        .filter_map(|i| files.get(*i))
        .cloned()
        .enumerate()
    {
        let (store, permits) = (store.clone(), permits.clone());
        reads.spawn(async move {
            let _permit = permits.acquire_owned().await;
            (slot, footer_of_file(&store, &file).await.ok())
        });
    }
    let mut out = vec![None; read.len()];
    while let Some(joined) = reads.join_next().await {
        // Counted as it lands, whether or not it read: a footer that will not parse is
        // one the open is no longer waiting on.
        progress.advance();
        if let Ok((slot, footer)) = joined {
            out[slot] = footer;
        }
    }
    progress.done();
    out
}

/// The rows in each row group of every file, in file order. Files whose footer cannot
/// be read count as zero rows, as they always have.
pub async fn row_groups_of_files(
    store: &Arc<dyn ObjectStore>,
    files: &[DatasetFile],
) -> Result<Vec<Vec<usize>>> {
    Ok(
        footers_of_files(store, files, &(0..files.len()).collect::<Vec<_>>())
            .await
            .into_iter()
            .map(|f| f.map(|f| f.row_group_rows).unwrap_or_default())
            .collect(),
    )
}

async fn footer_of_file(store: &Arc<dyn ObjectStore>, file: &DatasetFile) -> Result<FileFooter> {
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
    let mut reader = ParquetReader::new(&mut cursor);
    let arrow_schema = reader
        .schema()
        .map_err(|e| color_eyre::eyre::eyre!("Parquet schema read failed: {}", e))?;
    let metadata = reader
        .get_metadata()
        .map_err(|e| color_eyre::eyre::eyre!("Parquet footer read failed: {}", e))?;
    Ok(FileFooter {
        schema: Arc::new(Schema::from_arrow_schema(arrow_schema.as_ref())),
        row_group_rows: metadata.row_groups.iter().map(|rg| rg.num_rows()).collect(),
        row_group_bytes: metadata
            .row_groups
            .iter()
            .map(|rg| rg.compressed_size())
            .collect(),
    })
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

    /// The dataset's schema from every file's footer, as an open does.
    async fn schema_of(
        store: &Arc<dyn ObjectStore>,
        files: &[DatasetFile],
    ) -> (crate::schema_union::DatasetSchema, Vec<String>) {
        let read: Vec<usize> = (0..files.len()).collect();
        let footers = footers_of_files(store, files, &read).await;
        dataset_schema_from_footers(files, &read, &footers).unwrap()
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

    /// A remote dataset carries its row-group sizes through to the schema too.
    ///
    /// The two routes read their footers differently — a ranged read of the tail here,
    /// a whole local file there — and it is the `FileSchema` each builds that decides
    /// whether datui can say anything about row groups at all. Dropping the sizes on
    /// this side leaves the note working perfectly for local datasets and silent for
    /// the ones it exists for.
    #[test]
    fn a_remote_dataset_carries_its_row_group_sizes_into_the_schema() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let write = |groups: i64| -> Vec<u8> {
            let mut frame = df!("n" => (0..groups * 1_000).collect::<Vec<i64>>()).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes)
                .with_row_group_size(Some(1_000))
                .finish(&mut frame)
                .unwrap();
            bytes
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for (key, bytes) in [
                ("data/date=2024-01-01/a.parquet", write(3)),
                ("data/date=2024-01-02/b.parquet", write(2)),
            ] {
                store
                    .put(&OsPath::from(key), PutPayload::from(bytes))
                    .await
                    .unwrap();
            }
            let files = list_dataset_files(&store, "data/").await.unwrap();
            let read: Vec<usize> = (0..files.len()).collect();
            let footers = footers_of_files(&store, &files, &read).await;

            let mut sizes: Vec<usize> = footers
                .iter()
                .flatten()
                .flat_map(|footer| footer.row_group_bytes.iter().copied())
                .collect();
            assert_eq!(sizes.len(), 5, "three row groups and two: {sizes:?}");
            assert!(sizes.iter().all(|size| *size > 0), "{sizes:?}");

            let (dataset, _) = dataset_schema_from_footers(&files, &read, &footers).unwrap();
            sizes.sort_unstable();
            assert_eq!(
                dataset.median_row_group_bytes,
                Some(sizes[2]),
                "the middle of the five reaches the schema: {sizes:?}"
            );

            // And they are the compressed sizes, as the local route's are. Twenty
            // thousand distinct strings of two hundred characters are about 4 MiB once
            // decoded and a small fraction of that on the wire; a column of one
            // repeated value would not tell the two apart, since the dictionary makes
            // the decoded figure the smaller of them.
            let rows: Vec<String> = (0..20_000)
                .map(|i| format!("{i:0>6}{}", "abcdefghij".repeat(19)))
                .collect();
            let mut wide = df!("s" => rows).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes)
                .with_row_group_size(Some(20_000))
                .finish(&mut wide)
                .unwrap();
            store
                .put(
                    &OsPath::from("wide/date=2024-01-01/w.parquet"),
                    PutPayload::from(bytes),
                )
                .await
                .unwrap();
            let wide_files = list_dataset_files(&store, "wide/").await.unwrap();
            assert_eq!(wide_files.len(), 1, "only the wide file: {wide_files:?}");
            let wide_footers = footers_of_files(&store, &wide_files, &[0]).await;
            let size = wide_footers[0].as_ref().unwrap().row_group_bytes[0];
            assert!(
                size < 1_000_000,
                "the compressed size, not the decoded one: {size} bytes"
            );
        });
    }

    /// An object's size is the listing's to know, and a sampled read has to look it up
    /// by the index it read at rather than by where the footer came back in the list.
    ///
    /// With every footer read the two orders are the same list and any mistake here is
    /// invisible. They part company exactly when the dataset is too large to open every
    /// footer — the case this matters for.
    #[test]
    fn a_sampled_remote_read_takes_each_size_from_the_file_it_read() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let write = |rows: i64| -> Vec<u8> {
            let mut frame = df!("n" => (0..rows).collect::<Vec<i64>>()).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            // Three objects of very different sizes, in ascending order of size.
            for (key, rows) in [
                ("s/date=2024-01-01/a.parquet", 1),
                ("s/date=2024-01-02/b.parquet", 200),
                ("s/date=2024-01-03/c.parquet", 40_000),
            ] {
                store
                    .put(&OsPath::from(key), PutPayload::from(write(rows)))
                    .await
                    .unwrap();
            }
            let files = list_dataset_files(&store, "s/").await.unwrap();
            assert_eq!(files.len(), 3);

            // Only the last one's footer is read: its size is the one the schema must
            // carry, and it is the one a "by position" lookup would never reach.
            let read = [2usize];
            let footers = footers_of_files(&store, &files, &read).await;
            let (dataset, _) = dataset_schema_from_footers(&files, &read, &footers).unwrap();
            assert_eq!(
                dataset.median_file_bytes,
                Some(files[2].size as usize),
                "the file read, not the first in the list: {:?}",
                files.iter().map(|f| f.size).collect::<Vec<_>>()
            );
        });
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

            let (dataset, partitions) = schema_of(&store, &files).await;
            let schema = dataset.schema;
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
        let schema = rt.block_on(async {
            let files = list_dataset_files(&store, "data").await.unwrap();
            schema_of(&store, &files).await.0.schema
        });
        let df = lenient_scan(&urls, schema, None, None, &[])
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(df.height(), 7);
        let fees = df.column("fee").unwrap();
        assert_eq!(fees.null_count(), 2, "the old file has no fee");
        let second_file = lenient_scan(&urls[1..], df.schema().clone(), None, None, &[])
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

    /// Write `files` under a temp dir and read the dataset as an open would: every
    /// footer, then a scan of the files by name. Returns the schema and the rows.
    fn open_dataset(
        files: Vec<(String, Vec<u8>)>,
    ) -> (
        crate::schema_union::DatasetSchema,
        polars::prelude::DataFrame,
        tempfile::TempDir,
    ) {
        let dir = tempfile::TempDir::new().unwrap();
        let mut urls = Vec::new();
        for (key, bytes) in &files {
            let path = dir.path().join(key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, bytes).unwrap();
            urls.push(path.to_string_lossy().into_owned());
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let (dataset, listed, file_rows) = rt.block_on(async {
            let listed = list_dataset_files(&store, "data").await.unwrap();
            let read: Vec<usize> = (0..listed.len()).collect();
            let footers = footers_of_files(&store, &listed, &read).await;
            let rows: Vec<usize> = footers
                .iter()
                .map(|f| {
                    f.as_ref()
                        .map(|f| f.row_group_rows.iter().sum())
                        .unwrap_or(0)
                })
                .collect();
            (schema_of(&store, &listed).await.0, listed, rows)
        });
        let urls: Vec<String> = listed
            .iter()
            .map(|f| dir.path().join(&f.key).to_string_lossy().into_owned())
            .collect();
        let drift = crate::schema_union::ScanDrift::new(&urls, &dataset, &file_rows);
        let mut df = lenient_scan(&urls, dataset.schema.clone(), None, drift.as_ref(), &[])
            .unwrap()
            .collect()
            .unwrap();
        // The hidden drift column is the state's business, not this test's.
        let _ = df.drop_in_place(crate::schema_union::DRIFT_COLUMN);
        (dataset, df, dir)
    }

    fn parquet(df: polars::prelude::DataFrame) -> Vec<u8> {
        use polars::prelude::ParquetWriter;
        let mut df = df;
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes).finish(&mut df).unwrap();
        bytes
    }

    #[test]
    fn a_column_only_a_middle_file_has_is_not_hidden() {
        use polars::prelude::df;
        let files = vec![
            (
                "data/a.parquet".to_string(),
                parquet(df!("id" => &[1i64]).unwrap()),
            ),
            (
                "data/b.parquet".to_string(),
                parquet(df!("id" => &[2i64], "oops" => &["x"]).unwrap()),
            ),
            (
                "data/c.parquet".to_string(),
                parquet(df!("id" => &[3i64]).unwrap()),
            ),
        ];
        let (dataset, df, _dir) = open_dataset(files);
        let names: Vec<&str> = dataset.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, ["id", "oops"]);
        assert_eq!(df.height(), 3);
        assert_eq!(df.column("oops").unwrap().null_count(), 2);
    }

    #[test]
    fn files_of_different_integer_widths_open_as_the_wider_one() {
        use polars::prelude::{DataType, df};
        let files = vec![
            (
                "data/a.parquet".to_string(),
                parquet(df!("n" => &[1i32, 2]).unwrap()),
            ),
            (
                "data/b.parquet".to_string(),
                parquet(df!("n" => &[3i64]).unwrap()),
            ),
        ];
        let (dataset, df, _dir) = open_dataset(files);
        assert_eq!(dataset.schema.get("n"), Some(&DataType::Int64));
        assert_eq!(df.height(), 3);
        assert_eq!(df.column("n").unwrap().null_count(), 0);
    }

    #[test]
    fn a_number_and_text_column_keeps_the_rows_of_both() {
        use polars::prelude::{DataType, df};
        let files = vec![
            (
                "data/a.parquet".to_string(),
                parquet(df!("price" => &["1", "2"]).unwrap()),
            ),
            (
                "data/b.parquet".to_string(),
                parquet(df!("price" => &[3i64, 4, 5]).unwrap()),
            ),
        ];
        let (dataset, df, _dir) = open_dataset(files);
        assert_eq!(
            dataset.schema.get("price"),
            Some(&DataType::Int64),
            "the type most rows have"
        );
        assert_eq!(df.height(), 5, "every row is still there");
        assert_eq!(
            df.column("price").unwrap().null_count(),
            2,
            "the text file is not read for the column"
        );
        let drifting: Vec<_> = dataset.drifting().map(|c| c.name.to_string()).collect();
        assert_eq!(drifting, ["price"]);
        assert_eq!(dataset.columns[0].conflicting_types, [DataType::String]);
    }

    #[test]
    fn one_corrupt_file_does_not_stop_the_dataset_opening() {
        use polars::prelude::df;
        let files = vec![
            (
                "data/a.parquet".to_string(),
                parquet(df!("id" => &[1i64]).unwrap()),
            ),
            ("data/b.parquet".to_string(), b"not a parquet file".to_vec()),
            (
                "data/c.parquet".to_string(),
                parquet(df!("id" => &[3i64]).unwrap()),
            ),
        ];
        let dir = tempfile::TempDir::new().unwrap();
        for (key, bytes) in &files {
            let path = dir.path().join(key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, bytes).unwrap();
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let (dataset, listed) = rt.block_on(async {
            let listed = list_dataset_files(&store, "data").await.unwrap();
            (schema_of(&store, &listed).await.0, listed)
        });
        assert_eq!(dataset.unreadable, [1], "named, and left out of the scan");
        let names: Vec<&str> = dataset.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, ["id"]);
        assert_eq!(listed.len(), 3);
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
