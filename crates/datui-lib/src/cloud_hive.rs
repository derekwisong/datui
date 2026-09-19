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
    meter: &crate::measurements::Meter,
) -> Result<ParquetFooter> {
    let began = std::time::Instant::now();
    let footer = read_parquet_footer(&store, &crate::cloud_browse::object_path(key), meter).await;
    meter.read_footers(began.elapsed(), Some(1), true);
    footer
}

/// Fetch the tail of one object and read its footer, counted against `meter`.
///
/// Does not fetch the full file.
///
/// Two requests, not one: this route does not know the object's size, so it asks before
/// it reads. Both are counted — unmetered, the routes that come through here would
/// report footers read against no requests at all.
async fn read_parquet_footer(
    store: &Arc<dyn ObjectStore>,
    path: &OsPath,
    meter: &crate::measurements::Meter,
) -> Result<ParquetFooter> {
    let head = store.head(path).await;
    // A `head` returns no body, so it is a request that brought back nothing.
    meter.footer_request(0);
    let meta = head.map_err(|e| color_eyre::eyre::eyre!("Cloud head failed: {}", e))?;
    let size = meta.size;
    let start = size.saturating_sub(PARQUET_FOOTER_TAIL_BYTES as u64);
    let range = start..size;
    let got = store.get_ranges(path, &[range]).await;
    meter.footer_request(
        got.as_ref()
            .map(|r| r.iter().map(|b| b.len() as u64).sum())
            .unwrap_or(0),
    );
    let ranges = got.map_err(|e| color_eyre::eyre::eyre!("Cloud get_ranges failed: {}", e))?;
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
    meter: &crate::measurements::Meter,
) -> Result<(Arc<Schema>, Vec<String>)> {
    let prefix_trimmed = prefix.trim_end_matches('/');
    let prefix_path = if prefix_trimmed.is_empty() {
        OsPath::default()
    } else {
        crate::cloud_browse::object_path(prefix_trimmed)
    };
    // The walks are the listing: this route finds the prefix's two ends by listing one
    // partition level at a time. They are timed as a listing and the footer reads are
    // timed as footers, rather than one stretch over both — on a deep layout the walks
    // are most of the wait, and billing them to the footer row would say a glob was
    // slow to read footers when it was slow to find its files.
    let listing_began = std::time::Instant::now();
    let mut values = Vec::new();
    let one_key = first_parquet_key_spine(&store, &prefix_path, 0, &mut values, false)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("No parquet file found in cloud hive prefix"))?;
    let mut newest_values = Vec::new();
    let newest = first_parquet_key_spine(&store, &prefix_path, 0, &mut newest_values, true)
        .await?
        .filter(|newest| *newest != one_key);
    // No file count. This route walks to the prefix's two ends and never lists what is
    // between them, so it knows how long finding them took and does not know how many
    // files there are. The two ends are not that number, and putting them under the
    // word the other routes use for the size of the dataset would say a prefix of
    // thousands holds two.
    //
    // And no request count either. This walk makes a `list_with_delimiter` call per
    // partition level, but that call pages inside the object store exactly as a flat
    // `list` does — so the number of calls datui makes is not the number of round trips
    // it costs, and counting the calls would report a figure that grows further from
    // the truth the larger the prefix.
    meter.listed(listing_began.elapsed(), None, false);
    // Two ends, or one when the prefix holds a single file and both walks land on it.
    let ends = 1 + usize::from(newest.is_some());

    let footers_began = std::time::Instant::now();
    let mut file_schema = (*read_parquet_footer(&store, &one_key, meter).await?.schema).clone();
    if let Some(newest) = newest {
        let newest_schema = read_parquet_footer(&store, &newest, meter).await?.schema;
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
    // As many footers as were actually read: one when both walks landed on the same
    // object, which a glob matching a single file does.
    meter.read_footers(footers_began.elapsed(), Some(ends), true);
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
) -> Result<(Vec<DatasetFile>, crate::schema_union::SkippedFiles)> {
    use futures::TryStreamExt;
    let prefix = prefix.trim_matches('/');
    let prefix_path = (!prefix.is_empty()).then(|| crate::cloud_browse::object_path(prefix));
    let objects: Vec<object_store::ObjectMeta> = store
        .list(prefix_path.as_ref())
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Cloud list failed: {}", e))?;
    // Counted as they are passed over rather than walked again: the listing is the one
    // place that sees every name, and a note that says how many objects were not read
    // costs nothing here and a second listing anywhere else.
    fn folder_of(key: &str) -> &str {
        key.rsplit_once('/').map_or("", |(dir, _)| dir)
    }
    let all: Vec<DatasetFile> = objects
        .into_iter()
        .map(|o| DatasetFile {
            key: o.location.as_ref().to_string(),
            size: o.size,
        })
        .collect();
    // Every segment below the prefix, not just the name: a `.json` inside `_delta_log/`
    // is the table's own record of itself, and its name alone does not say so. Empty
    // rather than the whole key when the prefix does not match, so a dataset that
    // happens to live under a `_`-named folder is not written off entirely.
    let bookkeeping_of = |key: &str| {
        key.strip_prefix(prefix)
            .unwrap_or("")
            .split('/')
            .any(crate::schema_union::is_bookkeeping)
    };
    let keep_of = |f: &DatasetFile| {
        f.size > 0 && !bookkeeping_of(&f.key) && crate::discover::is_parquet_key(&f.key)
    };
    // Every folder with data anywhere beneath it, which is every folder on the way down
    // to a file this keeps. What else is in one of those is beside somebody's data;
    // what is anywhere else is somebody's infrastructure, whatever the format calls it
    // — see `SkippedFiles`.
    let mut with_data: std::collections::HashSet<&str> = std::collections::HashSet::new();
    // From every object whose name says data, not only the ones kept: a write that
    // stopped leaves nothing behind, and a partition whose only file is that write
    // would otherwise be a folder with no data in it — so the one skip most worth
    // saying would be filed as plumbing, in exactly the case that matters.
    for f in all
        .iter()
        .filter(|f| !bookkeeping_of(&f.key) && crate::discover::is_parquet_key(&f.key))
    {
        let mut folder = folder_of(&f.key);
        while !folder.is_empty() && with_data.insert(folder) {
            folder = folder_of(folder);
        }
        with_data.insert("");
    }
    // A partition of a dataset is part of it even when its own files all failed to be
    // Parquet: a day that landed as CSV is the mistake this note is for. A folder whose
    // name carries a partition key, under one that holds data, is one of those. A
    // `metadata/` beside the data is not.
    let beside_data = |folder: &str| {
        with_data.contains(folder)
            || (folder.rsplit('/').next().unwrap_or(folder).contains('=')
                && with_data.contains(folder_of(folder)))
    };
    let mut skipped = crate::schema_union::SkippedFiles::default();
    for f in &all {
        if keep_of(f) {
            continue;
        }
        let parquet_named = crate::discover::is_parquet_key(&f.key);
        if bookkeeping_of(&f.key)
            || !beside_data(folder_of(&f.key))
            // Nothing in it and a name that never said data: a folder marker, which a
            // console writes one of per partition. Not a file anyone left behind by
            // mistake, and not a write that stopped either.
            || (f.size == 0 && !parquet_named)
        {
            skipped.count(true);
        } else if f.size == 0 {
            // A name that says data over nothing at all is a write that stopped, which
            // is the one skip worth its own count.
            skipped.empty += 1;
        } else {
            skipped.count(false);
        }
    }
    let mut files: Vec<DatasetFile> = all.into_iter().filter(|f| keep_of(f)).collect();
    files.sort_by(|a, b| a.key.cmp(&b.key));
    Ok((files, skipped))
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
pub const FOOTERS_AT_ONCE: usize = 64;
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
    meter: &Arc<crate::measurements::Meter>,
) -> Vec<Option<FileFooter>> {
    footers_of_files_reporting(
        store,
        files,
        read,
        &crate::schema_union::FooterProgress::default(),
        meter,
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
    meter: &Arc<crate::measurements::Meter>,
) -> Vec<Option<FileFooter>> {
    let began = std::time::Instant::now();
    let pass = progress.pass(read.len());
    let permits = Arc::new(tokio::sync::Semaphore::new(FOOTERS_AT_ONCE));
    let mut reads = tokio::task::JoinSet::new();
    for (slot, file) in read
        .iter()
        .filter_map(|i| files.get(*i))
        .cloned()
        .enumerate()
    {
        let (store, permits, meter) = (store.clone(), permits.clone(), meter.clone());
        reads.spawn(async move {
            let _permit = permits.acquire_owned().await;
            (slot, footer_of_file(&store, &file, &meter).await.ok())
        });
    }
    let mut out = vec![None; read.len()];
    while let Some(joined) = reads.join_next().await {
        // Counted as it lands, whether or not it read: a footer that will not parse is
        // one the open is no longer waiting on.
        pass.advance();
        if let Ok((slot, footer)) = joined {
            out[slot] = footer;
        }
    }
    drop(pass);
    // The requests are already counted — each read counted itself as it was made — so
    // this only hands the running total back to be stamped with how long the pass took.
    meter.read_footers(began.elapsed(), Some(read.len()), true);
    out
}

/// The rows in each row group of every file, in file order. Files whose footer cannot
/// be read count as zero rows, as they always have.
///
/// Metered like any other footer pass, because that is what it is: a dataset whose open
/// could not settle the count re-reads every footer to take it, and those reads cost
/// exactly what the open's did. Left unmetered, a staged cloud open — which is every
/// prefix past a wave of objects — would report about half the requests it made.
pub async fn row_groups_of_files(
    store: &Arc<dyn ObjectStore>,
    files: &[DatasetFile],
    meter: &Arc<crate::measurements::Meter>,
) -> Result<Vec<Vec<usize>>> {
    // Counted as the pass that settles the row count, which is recorded once: this runs
    // again every time the count is invalidated, and a dataset explored for a few
    // minutes would otherwise report an open that kept getting more expensive.
    let counting = Arc::new(crate::measurements::Meter::default());
    let began = std::time::Instant::now();
    let groups = footers_of_files(
        store,
        files,
        &(0..files.len()).collect::<Vec<_>>(),
        &counting,
    )
    .await;
    // Against a meter of its own first, so that a pass the one-shot declines adds
    // nothing to the dataset's figures.
    let wire = counting.footers().and_then(|c| c.over_the_wire);
    // Not recorded when nothing parsed, the same as the local count: a pass that
    // settled nothing must not take the one measurement this gets, or the pass that
    // eventually succeeds is declined and never reported.
    if groups.iter().any(Option::is_some) {
        meter.counted_rows(began.elapsed(), Some(files.len()), wire);
    }
    Ok(groups
        .into_iter()
        .map(|f| f.map(|f| f.row_group_rows).unwrap_or_default())
        .collect())
}

/// Read a range, counting the request against `meter` and the bytes it returned.
///
/// The request is counted whether or not it succeeded — it was made either way, and a
/// prefix that is slow because half its reads fail should say so — while only bytes
/// that arrived are added. Written as a macro rather than a function because naming
/// the store's byte buffer would mean taking a dependency on `bytes` for one signature.
macro_rules! counted_range {
    ($store:expr, $path:expr, $range:expr, $meter:expr) => {{
        let got = $store.get_range($path, $range).await;
        $meter.footer_request(got.as_ref().map(|b| b.len() as u64).unwrap_or(0));
        got.map_err(|e| color_eyre::eyre::eyre!("Cloud read failed: {}", e))
    }};
}

async fn footer_of_file(
    store: &Arc<dyn ObjectStore>,
    file: &DatasetFile,
    meter: &crate::measurements::Meter,
) -> Result<FileFooter> {
    let path = crate::cloud_browse::object_path(&file.key);
    let tail_start = file.size.saturating_sub(COUNT_TAIL_BYTES);
    let tail = counted_range!(store, &path, tail_start..file.size, meter)?;
    let footer_len = footer_length(&tail)
        .ok_or_else(|| color_eyre::eyre::eyre!("{} is not a Parquet file", file.key))?;
    let needed = footer_len + 8;
    let tail = if needed as usize <= tail.len() {
        tail
    } else {
        counted_range!(
            store,
            &path,
            file.size.saturating_sub(needed)..file.size,
            meter
        )?
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
        let footers = footers_of_files(
            store,
            files,
            &read,
            &Arc::new(crate::measurements::Meter::default()),
        )
        .await;
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
            let (files, _skipped) = list_dataset_files(&store, "data/").await.unwrap();
            let read: Vec<usize> = (0..files.len()).collect();
            let footers = footers_of_files(
                &store,
                &files,
                &read,
                &Arc::new(crate::measurements::Meter::default()),
            )
            .await;

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
            let (wide_files, _skipped) = list_dataset_files(&store, "wide/").await.unwrap();
            assert_eq!(wide_files.len(), 1, "only the wide file: {wide_files:?}");
            let wide_footers = footers_of_files(
                &store,
                &wide_files,
                &[0],
                &Arc::new(crate::measurements::Meter::default()),
            )
            .await;
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
            let (files, _skipped) = list_dataset_files(&store, "s/").await.unwrap();
            assert_eq!(files.len(), 3);

            // Only the last one's footer is read: its size is the one the schema must
            // carry, and it is the one a "by position" lookup would never reach.
            let read = [2usize];
            let footers = footers_of_files(
                &store,
                &files,
                &read,
                &Arc::new(crate::measurements::Meter::default()),
            )
            .await;
            let (dataset, _) = dataset_schema_from_footers(&files, &read, &footers).unwrap();
            assert_eq!(
                dataset.median_file_bytes,
                Some(files[2].size as usize),
                "the file read, not the first in the list: {:?}",
                files.iter().map(|f| f.size).collect::<Vec<_>>()
            );
        });
    }

    /// The cloud open reports its footers to the counter it was handed.
    ///
    /// This is the pass with most reason to be narrated — every footer is a ranged read
    /// over the network — and it is the one where nothing else would notice if the
    /// counter came unwired. The local route has the same test; shipping one without
    /// the other would leave the slower half unguarded.
    ///
    /// The scan past the footer pass cannot open a `memory://` URL and the route returns
    /// `None`, which is fine: the footers have already been read by then, and they are
    /// what this is about.
    #[test]
    fn a_cloud_open_counts_its_footers_against_the_counter_it_is_given() {
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
            for (key, rows) in [
                ("data/date=2024-01-01/a.parquet", 1),
                ("data/date=2024-01-02/b.parquet", 2),
                ("data/date=2024-01-03/c.parquet", 3),
            ] {
                store
                    .put(&OsPath::from(key), PutPayload::from(write(rows)))
                    .await
                    .unwrap();
            }
        });

        // Entered below the line that builds a store from the user's config, since an
        // in-memory one cannot be handed to that, but above the choice of route — so
        // a prefix reaching the globbing route, or either route being handed a fresh
        // counter instead of this one, fails here.
        let progress = Arc::new(crate::schema_union::FooterProgress::default());
        let _ = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/".to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: progress.clone(),
                meter: Arc::new(crate::measurements::Meter::default()),
            },
        );

        let pass = progress.last_pass();
        assert_eq!(pass.begun, 1, "the open ran its footer pass against it");
        assert_eq!(pass.read, 3, "counting each of the three objects off");
        assert_eq!(
            progress.reading(),
            None,
            "with nothing left to say once they landed"
        );
    }

    /// A cloud count that read nothing leaves the measurement for the one that does.
    ///
    /// The twin of the local route's guard. Counting gets one measurement, and a pass
    /// where no footer parsed settled nothing — a prefix caught mid-write is the case.
    /// If such a pass took it, the count that eventually works is declined and the
    /// Footers row reports the failed attempt for as long as the dataset is open.
    #[test]
    fn a_cloud_count_that_read_nothing_leaves_the_measurement_for_the_one_that_does() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let key = OsPath::from("data/date=2024-01-01/half-written.parquet");
        rt.block_on(async {
            store
                .put(&key, PutPayload::from(b"not parquet yet".to_vec()))
                .await
                .unwrap();
        });
        let files = vec![DatasetFile {
            key: "data/date=2024-01-01/half-written.parquet".to_string(),
            size: 15,
        }];

        let meter = Arc::new(crate::measurements::Meter::default());
        // As the open leaves it: a count belongs to an open this meter measured.
        meter.listed(std::time::Duration::from_millis(1), Some(1), false);
        rt.block_on(async {
            row_groups_of_files(&store, &files, &meter).await.unwrap();
        });
        assert_eq!(
            meter.footers(),
            None,
            "nothing under there parsed, so nothing was measured and the one \
             measurement counting gets is still to be had"
        );

        // The writer finishes, and the count that works is the one reported.
        let mut frame = df!("n" => &[1i64, 2, 3]).unwrap();
        let mut body = Vec::new();
        ParquetWriter::new(&mut body).finish(&mut frame).unwrap();
        let size = body.len() as u64;
        rt.block_on(async {
            store.put(&key, PutPayload::from(body)).await.unwrap();
        });
        let files = vec![DatasetFile {
            key: "data/date=2024-01-01/half-written.parquet".to_string(),
            size,
        }];
        let groups =
            rt.block_on(async { row_groups_of_files(&store, &files, &meter).await.unwrap() });
        assert_eq!(
            groups.iter().flatten().sum::<usize>(),
            3,
            "and it counts the three rows"
        );
        let footers = meter.footers().expect("the count that worked was measured");
        assert_eq!(footers.files, Some(1), "over the one object");
        assert!(
            footers
                .over_the_wire
                .is_some_and(|w| w.bytes.is_some_and(|b| b > 15)),
            "reporting the real read, not the fifteen bytes of the half-written one; \
             got {:?}",
            footers.over_the_wire
        );
    }

    /// A prefix walked to its two ends reports the footers it actually read.
    ///
    /// When the prefix holds a single file both walks land on it and only one footer is
    /// read, so reporting the two this route usually reads would be a figure of work
    /// that did not happen. The walks are the listing and the reads are the footers,
    /// timed apart: on a deep layout the walking is most of the wait, and billing it to
    /// the footer row would say the open was slow to read footers when it was slow to
    /// find its files.
    ///
    /// Against the function, not a route a user can reach: `schema_state_from_cloud_hive`
    /// sends only starred paths here and does not strip the star, so the listing below
    /// it matches nothing and every real glob falls through to a full scan — see #228.
    /// Phase 6 of #195 replaces this route with one that expands globs itself. Until
    /// then this holds the arithmetic so the replacement inherits it.
    #[test]
    fn a_glob_over_one_file_counts_the_one_footer_it_read() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut frame = df!("n" => &[1i64, 2, 3]).unwrap();
        let mut body = Vec::new();
        ParquetWriter::new(&mut body).finish(&mut frame).unwrap();
        rt.block_on(async {
            store
                .put(
                    &OsPath::from("data/date=2024-01-01/only.parquet"),
                    PutPayload::from(body),
                )
                .await
                .unwrap();
        });

        let meter = crate::measurements::Meter::default();
        rt.block_on(async {
            schema_from_one_cloud_hive(store.clone(), "data/", &meter)
                .await
                .expect("the prefix has a parquet file in it")
        });

        let footers = meter
            .footers()
            .expect("the route measured its footer reads");
        assert_eq!(
            footers.files,
            Some(1),
            "both walks landed on the same object, so one footer was read"
        );
        assert_eq!(
            footers.over_the_wire.map(|w| w.requests),
            Some(2),
            "which cost two requests: this route must ask an object's size before it \
             can ask for its tail"
        );
        let listing = meter
            .listing()
            .expect("and measured the walks that found it");
        assert_eq!(
            listing.files, None,
            "and no file count at all: this route walks to a prefix's two ends and \
             never lists what is between them, so it does not know how many files \
             there are — and two is a smaller number than most such prefixes hold"
        );
        assert_eq!(
            listing.over_the_wire, None,
            "and nothing over the wire: this walk makes a list call per partition \
             level, but each of those pages inside the object store just as a flat \
             listing does, so the calls are not the round trips and datui counts neither"
        );
    }

    /// A single remote object opens with a footer row and no listing row.
    ///
    /// There is nothing to list — the user named one object — and a listing row of zero
    /// files would say datui looked and found nothing.
    #[test]
    fn a_single_remote_object_measures_its_footer_and_lists_nothing() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut frame = df!("n" => &[1i64, 2, 3]).unwrap();
        let mut body = Vec::new();
        ParquetWriter::new(&mut body).finish(&mut frame).unwrap();
        rt.block_on(async {
            store
                .put(&OsPath::from("one.parquet"), PutPayload::from(body))
                .await
                .unwrap();
        });

        let meter = crate::measurements::Meter::default();
        rt.block_on(async {
            footer_of_cloud_parquet(store.clone(), "one.parquet", &meter)
                .await
                .expect("it is a parquet object")
        });

        let footers = meter.footers().expect("the read was measured");
        assert_eq!(
            footers.files,
            Some(1),
            "one footer, from the one object named"
        );
        assert_eq!(
            footers.over_the_wire.map(|w| w.requests),
            Some(2),
            "a head to learn its size and a range to read its tail"
        );
        assert!(
            footers
                .over_the_wire
                .is_some_and(|w| w.bytes.is_some_and(|b| b > 0)),
            "the range came back with bytes in it"
        );
        assert_eq!(
            meter.listing(),
            None,
            "and nothing was listed, so no listing row is claimed"
        );
        assert_eq!(
            meter.total(),
            None,
            "nor a total: with one stretch a total is that stretch over again, the \
             same time and the same requests under a second label"
        );
    }

    /// The twin of the test above, for the meter rather than the counter: a cloud open
    /// times its listing and its footer pass and counts what each cost.
    ///
    /// The requests and the bytes are the point. Each footer is a ranged read datui
    /// issues itself, so it can say exactly how many it made and exactly how much came
    /// back, and the Info panel says so without the word "estimated" — which is a claim
    /// only worth making if the counting is real. Three objects, one ranged read each
    /// (a Parquet footer this small sits inside the sixteen-kilobyte tail), so three
    /// requests; the bytes are whatever those reads returned, which is more than none
    /// and no more than the whole of the three objects.
    #[test]
    fn a_cloud_open_measures_what_its_listing_and_its_footers_cost() {
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
        let mut written = 0u64;
        rt.block_on(async {
            for (key, rows) in [
                ("data/date=2024-01-01/a.parquet", 1),
                ("data/date=2024-01-02/b.parquet", 2),
                ("data/date=2024-01-03/c.parquet", 3),
            ] {
                let body = write(rows);
                written += body.len() as u64;
                store
                    .put(&OsPath::from(key), PutPayload::from(body))
                    .await
                    .unwrap();
            }
        });

        let meter = Arc::new(crate::measurements::Meter::default());
        let _ = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/".to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: Arc::new(crate::schema_union::FooterProgress::default()),
                meter: meter.clone(),
            },
        );

        let listing = meter.listing().expect("the open measured its listing");
        assert_eq!(listing.files, Some(3), "the listing returned three objects");
        assert!(
            listing.over_the_wire.is_none(),
            "object_store turns the listing's pages over itself, so the requests are \
             not datui's to count and it claims none"
        );

        let footers = meter.footers().expect("the open measured its footer pass");
        assert_eq!(
            footers.files,
            Some(3),
            "a footer was read from each of the three"
        );
        let wire = footers
            .over_the_wire
            .expect("datui issued the footer reads itself, so it counts them");
        assert_eq!(
            wire.requests, 3,
            "one ranged read each: these footers fit inside the tail datui asks for"
        );
        assert!(
            wire.bytes.is_some_and(|b| b > 0 && b <= written),
            "the bytes are what those reads returned — some, and no more than the three \
             objects hold ({} of {written})",
            wire.bytes.unwrap_or(0)
        );

        let total = meter.total().expect("and a total over both");
        assert_eq!(
            total.over_the_wire.map(|w| w.requests),
            Some(3),
            "the total carries the requests of the stretch that made any"
        );
    }

    /// A dataset too big to read whole opens from its two ends, and the rest joins.
    ///
    /// The column only a middle file has is the whole point: the two ends cannot know
    /// about it, so it is missing from the dataset as it opens and arrives when the
    /// pass behind the open lands. It joins at the end of the order, and everything
    /// already there — including where the user has scrolled to — stays put.
    #[test]
    fn a_column_only_a_middle_file_has_joins_after_the_open() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let plain = |i: i64| -> Vec<u8> {
            let mut frame = df!("id" => &[i], "v" => &[i * 2]).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };
        let with_oops = |i: i64| -> Vec<u8> {
            let mut frame = df!("id" => &[i], "v" => &[i * 2], "oops" => &["vendor"]).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        // One more than a wave of concurrent reads, which is where the open stops
        // waiting for every footer.
        let files = FOOTERS_AT_ONCE + 1;
        let odd_one_out = files / 2;
        rt.block_on(async {
            for i in 0..files {
                let key = format!("data/date=2024-01-{:03}/part.parquet", i + 1);
                let body = if i == odd_one_out {
                    with_oops(i as i64)
                } else {
                    plain(i as i64)
                };
                store
                    .put(&OsPath::from(key.as_str()), PutPayload::from(body))
                    .await
                    .unwrap();
            }
        });

        let progress = Arc::new(crate::schema_union::FooterProgress::default());
        let mut state = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/".to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: progress.clone(),
                meter: Arc::new(crate::measurements::Meter::default()),
            },
        )
        .expect("the prefix opens");

        assert_eq!(
            progress.last_pass().read,
            2,
            "the open waited for two footers, not {files}"
        );
        assert!(
            !state.get_column_order().iter().any(|c| c == "oops"),
            "the two ends cannot know about a column only the middle has: {:?}",
            state.get_column_order()
        );
        // And nothing else is sent after the same footers. The counter this returns is
        // a second pass over every footer of the dataset — the one cost staging the
        // open was meant to avoid, and it would double it instead.
        assert!(
            state.remote_files_counter().is_none(),
            "the pass already reading every footer is where the count comes from"
        );

        // The user, meanwhile, has been reading it: scrolled a column across and moved
        // down the rows.
        state.scroll_right();
        let scrolled_to = state.termcol_index;
        assert!(scrolled_to > 0, "the fixture can be scrolled");

        let join = state
            .footers_pending()
            .expect("the rest are still to be read");
        let found = join(&progress).expect("the pass reads them");
        // As `build_schema_state` marks a prefix that is scanned where it lies, and as
        // a rendered table has a height.
        state.set_remote_source();
        state.visible_rows = 10;
        assert!(
            state.join_dataset_schema(found).is_ok(),
            "nothing is built on top of the scan here, so they go straight in"
        );

        assert_eq!(
            progress.last_pass().read,
            files,
            "the pass behind the open read every footer"
        );
        assert_eq!(
            state.get_column_order().last().map(String::as_str),
            Some("oops"),
            "the column joins, at the end, where nothing already shown has to move: \
             {:?}",
            state.get_column_order()
        );
        assert_eq!(
            state.termcol_index, scrolled_to,
            "and the view does not move under the user to make room"
        );
        assert!(
            state.footers_pending().is_none(),
            "with nothing left to wait for"
        );
        // And the dataset can still be read. Knowing every file's row groups turns on
        // the windowed read, which goes through the scan the dataset is holding rather
        // than through `lf` — and the scan it opened with was built at the two-footer
        // schema, which has never heard of the column that just joined. Left in place
        // it makes every page after the join fail with `unable to find column "oops"`,
        // which is the table going blank at the moment it was to show more.
        let mut request = state
            .prepare_async_collect(None)
            .expect("a page is planned");
        // Resolved rather than collected: Polars cannot fetch from the in-memory store,
        // so the read itself fails here for a reason that has nothing to do with this.
        // Resolving is where the fault showed anyway — the page asks the scan for the
        // columns on screen, and a scan that has not heard of one of them cannot be
        // planned at all.
        let planned = request.lf.collect_schema();
        assert!(
            planned.is_ok(),
            "the first page after the join could not even be planned: {:?}",
            planned.err()
        );
        let planned = planned.unwrap();
        assert!(
            planned.iter_names().any(|name| name == "oops"),
            "and it reads the column that just joined: {:?}",
            planned.iter_names().collect::<Vec<_>>()
        );
        assert_eq!(
            state.num_rows_if_valid(),
            Some(files),
            "and the count the pass brought back with it, one row a file — without a \
             second pass over the same footers to learn it"
        );
        // Nothing was read here. The join happens on the thread drawing the screen, so
        // a collect inside it is a remote read the whole terminal waits on — and with
        // no count yet it would be a `len()` over every file in the dataset.
        assert!(
            state.display_df().is_none(),
            "the frame is rebuilt but not read; the caller reads it back off the loop"
        );
    }

    /// A corrupt object the open could not see is left out when the pass finds it.
    ///
    /// The staged open reads two footers, so an object that will not parse anywhere but
    /// the two ends is invisible to it: the dataset opens with that object in its scan,
    /// and the pass behind it is the first thing to know better. Everything the pass
    /// hands over has to describe the same list — the scan it built, the urls it found,
    /// and the counter that answers one entry per file it was given. A counter left
    /// over from the open answers for a file more than the dataset now holds, and that
    /// answer is dropped on a length check without a word: no count, no offsets, and
    /// every page a scan of the whole prefix for the rest of the session.
    #[test]
    fn an_object_only_the_pass_finds_corrupt_is_left_out_by_the_pass() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = |i: i64| -> Vec<u8> {
            let mut frame = df!("id" => &[i]).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        // One more than a wave, so the open reads only the two ends — and the bad one
        // is in the middle, where neither end can see it.
        let files = FOOTERS_AT_ONCE + 1;
        let unreadable = files / 2;
        rt.block_on(async {
            for i in 0..files {
                let key = format!("data/date=2024-01-{:03}/part.parquet", i + 1);
                let bytes = if i == unreadable {
                    b"not a parquet file".to_vec()
                } else {
                    body(i as i64)
                };
                store
                    .put(&OsPath::from(key.as_str()), PutPayload::from(bytes))
                    .await
                    .unwrap();
            }
        });

        let progress = Arc::new(crate::schema_union::FooterProgress::default());
        let mut state = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/".to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: progress.clone(),
                meter: Arc::new(crate::measurements::Meter::default()),
            },
        )
        .expect("the prefix opens");

        let join = state
            .footers_pending()
            .expect("the rest are still to be read");
        let found = join(&progress).expect("the pass reads them");
        assert!(
            state.join_dataset_schema(found).is_ok(),
            "nothing is built on top of the scan here"
        );

        let plan = state
            .visible_lf()
            .explain(false)
            .expect("the scan can be planned");
        assert!(
            !plan.contains(&format!("date=2024-01-{:03}", unreadable + 1)),
            "the object that will not parse is not one of the sources: {plan}"
        );

        let counter = state
            .remote_files_counter()
            .expect("the dataset has not counted itself yet");
        let groups = counter().expect("the readable objects are counted");
        state.set_file_row_groups(&groups);
        assert_eq!(
            state.num_rows_if_valid(),
            Some(files - 1),
            "and the count lands — one row from every object that would open, rather \
             than an answer for a list the dataset no longer holds, dropped in silence"
        );
    }

    /// A dataset small enough to read in one wave opens whole, rather than twice.
    ///
    /// `footers_of_files_reporting` fetches `FOOTERS_AT_ONCE` at a time, so up to that
    /// many the footers cost the same one round trip whether two are read or all of
    /// them. Opening such a dataset from two would show it incomplete for a moment and
    /// then rebuild it, for nothing — and it would lose the row numbering that tells an
    /// absent cell from a null.
    #[test]
    fn a_dataset_of_one_wave_of_footers_opens_whole() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = |i: i64| -> Vec<u8> {
            let mut frame = df!("id" => &[i]).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for i in 0..FOOTERS_AT_ONCE {
                let key = format!("data/date=2024-01-{:03}/part.parquet", i + 1);
                store
                    .put(
                        &OsPath::from(key.as_str()),
                        PutPayload::from(body(i as i64)),
                    )
                    .await
                    .unwrap();
            }
        });

        let progress = Arc::new(crate::schema_union::FooterProgress::default());
        let state = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/".to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: progress.clone(),
                meter: Arc::new(crate::measurements::Meter::default()),
            },
        )
        .expect("the prefix opens");

        assert_eq!(
            progress.last_pass().read,
            FOOTERS_AT_ONCE,
            "a wave's worth is read at the open, not two of them"
        );
        assert!(
            state.footers_pending().is_none(),
            "with nothing left to read behind it"
        );
        assert_eq!(
            state.num_rows_if_valid(),
            Some(FOOTERS_AT_ONCE),
            "counted from those footers as it opens, rather than left to a later pass"
        );
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
            let (files, _skipped) = list_dataset_files(&store, "data/").await.unwrap();
            let keys: Vec<&str> = files.iter().map(|f| f.key.as_str()).collect();
            assert_eq!(
                keys,
                [
                    "data/date=2009-01-03/part-0.parquet",
                    "data/date=2026-09-17/part-0.parquet"
                ]
            );

            let groups = row_groups_of_files(
                &store,
                &files,
                &Arc::new(crate::measurements::Meter::default()),
            )
            .await
            .unwrap();
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
            let (files, _skipped) = list_dataset_files(&store, "data").await.unwrap();
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
            let (listed, _skipped) = list_dataset_files(&store, "data").await.unwrap();
            let read: Vec<usize> = (0..listed.len()).collect();
            let footers = footers_of_files(
                &store,
                &listed,
                &read,
                &Arc::new(crate::measurements::Meter::default()),
            )
            .await;
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
            let (listed, _skipped) = list_dataset_files(&store, "data").await.unwrap();
            (schema_of(&store, &listed).await.0, listed)
        });
        assert_eq!(dataset.unreadable, [1], "named, and left out of the scan");
        let names: Vec<&str> = dataset.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, ["id"]);
        assert_eq!(listed.len(), 3);

        // And the rows of the other two can be read, which is the whole of the claim.
        // Naming the file in `unreadable` is not leaving it out: the scan is built from
        // a list of paths, and one that will not parse fails the read for all of them.
        let paths: Vec<String> = files
            .iter()
            .map(|(key, _)| dir.path().join(key).to_string_lossy().into_owned())
            .collect();
        let readable = crate::schema_union::readable_paths(&paths, &dataset.unreadable);
        assert_eq!(
            readable.len(),
            2,
            "the one that will not parse is not scanned"
        );
        let rows =
            crate::schema_union::lenient_scan(&readable, dataset.schema.clone(), None, None, &[])
                .and_then(|lf| lf.collect());
        assert_eq!(
            rows.map(|df| df.height()).ok(),
            Some(2),
            "the two readable files' rows"
        );
        // The same scan over every listed path is the failure this avoids.
        let all =
            crate::schema_union::lenient_scan(&paths, dataset.schema.clone(), None, None, &[])
                .and_then(|lf| lf.collect());
        assert!(
            all.is_err(),
            "left in, it takes the readable files down with it"
        );
    }

    /// The dataset a corrupt object leaves behind still counts itself.
    ///
    /// Leaving the object out of the scan is only half of it. Everything downstream has
    /// to describe the same list: the counter returns one entry per object it is given
    /// and the offsets want one per url, so a counter still covering the whole listing
    /// beside a shorter url list is not a wrong count but no count at all — dropped on
    /// a length check, without a word, leaving the dataset re-counting itself forever
    /// and never reaching an end to jump to.
    #[test]
    fn a_dataset_with_a_corrupt_object_still_counts_the_rest() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = |i: i64| -> Vec<u8> {
            let mut frame = df!("id" => &[i]).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for (key, bytes) in [
                ("data/date=2024-01-01/a.parquet", body(1)),
                (
                    "data/date=2024-01-02/b.parquet",
                    b"not a parquet file".to_vec(),
                ),
                ("data/date=2024-01-03/c.parquet", body(3)),
            ] {
                store
                    .put(&OsPath::from(key), PutPayload::from(bytes))
                    .await
                    .unwrap();
            }
        });

        let progress = Arc::new(crate::schema_union::FooterProgress::default());
        let mut state = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/".to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: progress.clone(),
                meter: Arc::new(crate::measurements::Meter::default()),
            },
        )
        .expect("the prefix opens despite the one that will not parse");

        // The frame the first paint collects, before any offsets exist and so before the
        // url list below is what is read. Its plan rather than its rows, because Polars
        // cannot fetch from an in-memory store — but the plan is where the fault was:
        // the object that will not parse named as a source is what takes the read down.
        let plan = state
            .visible_lf()
            .explain(false)
            .expect("the scan can be planned");
        assert!(
            !plan.contains("b.parquet"),
            "the object that will not parse is not one of the sources: {plan}"
        );
        assert!(
            plan.contains("a.parquet") && plan.contains("c.parquet"),
            "and the two that will are: {plan}"
        );

        let counter = state
            .remote_files_counter()
            .expect("the dataset has not counted itself yet, so it offers to");
        let groups = counter().expect("the readable objects are counted");
        state.set_file_row_groups(&groups);
        assert_eq!(
            state.num_rows_if_valid(),
            Some(2),
            "one row from each object that would open, and the count lands rather than \
             being dropped on a length nobody mentions"
        );
    }

    /// A table format's own files are its own, whatever the format calls them.
    ///
    /// Delta and Hudi put a `_` or a `.` on theirs and Iceberg does not — its log is a
    /// plain `metadata/` beside the data. Naming each convention is a game with no end,
    /// so the test is where a file is: a folder with no Parquet in it is nobody's
    /// table. The same rule silences the zero-byte folder markers a console leaves,
    /// one per partition, which would otherwise read as hundreds of stopped writes.
    #[test]
    fn a_folder_with_no_data_in_it_is_nobodys_table() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let parquet = || -> Vec<u8> {
            let mut frame = df!("id" => &[1i64]).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for (key, body) in [
                ("t/data/date=1/part-0.parquet", parquet()),
                // Iceberg's log: no underscore, no dot, and not a mistake.
                ("t/metadata/v1.metadata.json", b"{}".to_vec()),
                ("t/metadata/v2.metadata.json", b"{}".to_vec()),
                ("t/metadata/snap-123.avro", b"x".to_vec()),
                ("t/metadata/version-hint.text", b"2".to_vec()),
                // What a console leaves when somebody makes a folder: nothing at all,
                // under a name with no extension.
                ("t/data/date=1", Vec::new()),
                ("t/data/date=2", Vec::new()),
                // And beside the data: one real mistake, one write that stopped, and
                // one empty file whose name never said it was data.
                ("t/data/date=1/extra.csv", b"id\n1\n".to_vec()),
                ("t/data/date=1/part-1.parquet", Vec::new()),
                ("t/data/date=1/README", Vec::new()),
                // A whole partition whose only write stopped, two levels down, so no
                // folder above it holds data either. Nothing readable is left anywhere
                // on that path — it is part of the dataset because the name of a file
                // that was meant to be there says so, and the stopped write is the
                // thing worth saying.
                ("t/data/y=2024/m=03/part-0.parquet", Vec::new()),
                // While a day that landed as CSV is the ordinary mistake.
                ("t/data/date=4/part-0.csv", b"id\n4\n".to_vec()),
            ] {
                store
                    .put(&OsPath::from(key), PutPayload::from(body))
                    .await
                    .unwrap();
            }
        });

        let (files, skipped) = rt
            .block_on(list_dataset_files(&store, "t"))
            .expect("the prefix lists");
        assert_eq!(
            files.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
            ["t/data/date=1/part-0.parquet"]
        );
        assert_eq!(
            skipped.not_parquet, 2,
            "the csv beside the data and the day that landed as one: both are files \
             somebody meant to be in the table"
        );
        assert_eq!(
            skipped.empty, 2,
            "and both whose names said Parquet over nothing at all — including the \
             one alone in its partition, which is the case that matters most"
        );
        assert_eq!(
            skipped.bookkeeping, 7,
            "the four Iceberg files, the two folder markers, and a README with \
             nothing in it: placeholders and plumbing"
        );
    }

    /// What the listing passed over survives the pass behind a staged open.
    ///
    /// A prefix of more than a wave of objects opens from two footers and reads the
    /// rest behind the data. The pass builds a whole new dataset, and anything the open
    /// recorded that the pass does not carry is on screen from the open and gone the
    /// moment the columns join — a note that flashes and disappears, on every prefix
    /// big enough to be staged, which is every prefix worth staging.
    #[test]
    fn a_staged_open_does_not_lose_what_the_listing_passed_over() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = |i: i64| -> Vec<u8> {
            let mut frame = df!("id" => &[i]).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let files = FOOTERS_AT_ONCE + 1;
        rt.block_on(async {
            for i in 0..files {
                let key = format!("data/date=2024-01-{:03}/part.parquet", i + 1);
                store
                    .put(
                        &OsPath::from(key.as_str()),
                        PutPayload::from(body(i as i64)),
                    )
                    .await
                    .unwrap();
            }
            store
                .put(
                    &OsPath::from("data/date=2024-01-001/extra.csv"),
                    PutPayload::from(b"id\n1\n".to_vec()),
                )
                .await
                .unwrap();
        });

        let progress = Arc::new(crate::schema_union::FooterProgress::default());
        let mut state = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/".to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: progress.clone(),
                meter: Arc::new(crate::measurements::Meter::default()),
            },
        )
        .expect("the prefix opens");

        let said = |state: &crate::widgets::datatable::DataTableState| {
            state
                .notes()
                .iter()
                .any(|note| note.summary.contains("not Parquet"))
        };
        assert!(said(&state), "the open says so");

        let join = state
            .footers_pending()
            .expect("the rest are still to be read");
        let found = join(&progress).expect("the pass reads them");
        assert!(state.join_dataset_schema(found).is_ok());
        assert!(
            said(&state),
            "and it still does once the columns have joined: {:#?}",
            state.notes()
        );
    }

    /// The listing counts what it passes over, and says which kind each was.
    ///
    /// Three kinds, and they mean different things to a reader: a `.csv` somebody
    /// thought was in the table, a write that stopped and left nothing behind, and the
    /// table's own log — which is not a mistake at all, however many files it is.
    #[test]
    fn the_listing_counts_what_it_passes_over() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let parquet = || -> Vec<u8> {
            let mut frame = df!("id" => &[1i64]).unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
            bytes
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for (key, body) in [
                ("data/date=1/part-0.parquet", parquet()),
                ("data/date=1/extra.csv", b"id\n1\n".to_vec()),
                ("data/date=1/notes.txt", b"read me".to_vec()),
                // A write that stopped: the name says data, the object has nothing in
                // it, and no footer note can reach it because it never gets that far.
                ("data/date=1/part-1.parquet", Vec::new()),
                ("data/_SUCCESS", Vec::new()),
                // The table's own log, whose files are named like anybody's.
                ("data/_delta_log/00000000000000000000.json", b"{}".to_vec()),
                ("data/_delta_log/.00000000000000000000.json.crc", Vec::new()),
            ] {
                store
                    .put(&OsPath::from(key), PutPayload::from(body))
                    .await
                    .unwrap();
            }
        });

        let (files, skipped) = rt
            .block_on(list_dataset_files(&store, "data"))
            .expect("the prefix lists");
        assert_eq!(
            files.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
            ["data/date=1/part-0.parquet"],
            "one object is the table"
        );
        assert_eq!(
            skipped,
            crate::schema_union::SkippedFiles {
                not_parquet: 2,
                empty: 1,
                bookkeeping: 3,
            },
            "the csv and the txt are somebody's, the empty part is a write that \
             stopped, and `_SUCCESS` and both log files are the writer's own"
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
