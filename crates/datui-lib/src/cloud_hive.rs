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

const PARQUET_FOOTER_TAIL_BYTES: usize = 256 * 1024;
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

/// One data file of a cloud dataset: its key in the store and its size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFile {
    pub key: String,
    pub size: u64,
    /// When the object was last written, in seconds since the epoch, where the store
    /// said. Part of the fingerprint that decides whether what datui remembers about
    /// this dataset still describes it.
    pub stamp: u64,
    /// The store's own tag for this version of the object, where it gave one.
    ///
    /// The strongest part of that fingerprint, and free — it comes back in the same
    /// listing response as the size. A size and a whole-second timestamp cannot see a
    /// file overwritten within the same second at the same length; an ETag can.
    pub etag: Option<String>,
}

/// Every Parquet file under `prefix`, sorted by key, which is the order a scan of the
/// prefix reads them in. One listing, however deep the partitions go. Job files,
/// hidden files and empty objects are left out: none of them is data, and a scan that
/// tried to read one would fail.
/// The literal part of a globbed key: everything up to the last `/` before the first
/// `*`, which is the deepest prefix a listing can start from.
///
/// `data/*.parquet` lists `data/`; `logs/year=*/day=*/x.parquet` lists `logs/`; a key
/// whose first segment is starred lists the whole bucket, which is what it asked for.
pub fn prefix_of_glob(key: &str) -> &str {
    let star = match key.find('*') {
        Some(at) => at,
        None => return key,
    };
    match key[..star].rfind('/') {
        Some(slash) => &key[..slash],
        None => "",
    }
}

/// Keeping only the keys `pattern` matches, where one was given.
///
/// This is how datui opens a glob: it lists the literal prefix and does the matching
/// itself, so a glob becomes an ordinary list of files and gets everything a prefix
/// gets — the schema union over every footer, the row count, the notes and the
/// measurements. Handing the star to the object store instead matches nothing, because
/// a listing prefix is a literal string and `*` is a character like any other.
pub async fn list_dataset_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    pattern: Option<&globset::GlobMatcher>,
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
            stamp: o.last_modified.timestamp().try_into().unwrap_or_default(),
            etag: o.e_tag.clone(),
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
            .any(crate::discover::is_bookkeeping)
    };
    // What counts as data under this prefix, whether or not a glob then narrows it.
    // The narrowing is deliberately not part of this: the skipped-file counts are built
    // from the same test, and a Parquet file a glob excluded is not one somebody might
    // have meant as data and left unreadable — it is one they told datui to leave out.
    // Folding the pattern in here made a glob report its own siblings as "not Parquet".
    let is_data = |f: &DatasetFile| {
        f.size > 0 && !bookkeeping_of(&f.key) && crate::discover::is_parquet_key(&f.key)
    };
    // A glob names the files it wants; everything else under the prefix is somebody
    // else's, and is neither read nor counted.
    let wanted = |f: &DatasetFile| pattern.is_none_or(|p| p.is_match(&f.key));
    let keep_of = |f: &DatasetFile| is_data(f) && wanted(f);
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
        // Counted against what the prefix holds, not what the glob asked for: a file
        // the pattern excluded was never a candidate, and saying so would tell a user
        // their own glob had passed over data.
        if is_data(f) || !wanted(f) {
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

/// What a footer pass learned, in the form the cache keeps it.
///
/// The schemas are gathered into a table and referred to by index: a dataset of ten
/// thousand files usually has one schema, and writing each file's columns out in full
/// would make the cache larger than the footers it saves reading.
pub fn footers_to_cache(
    footers: &[Option<FileFooter>],
) -> (
    Vec<crate::cache::CachedFooter>,
    Vec<Vec<(String, polars::prelude::DataType)>>,
) {
    let mut schemas: Vec<Vec<(String, polars::prelude::DataType)>> = Vec::new();
    let cached = footers
        .iter()
        .map(|footer| match footer {
            None => crate::cache::CachedFooter::default(),
            Some(f) => {
                let columns: Vec<(String, polars::prelude::DataType)> = f
                    .schema
                    .iter()
                    .map(|(name, dtype)| (name.to_string(), dtype.clone()))
                    .collect();
                let at = schemas
                    .iter()
                    .position(|s| *s == columns)
                    .unwrap_or_else(|| {
                        schemas.push(columns);
                        schemas.len() - 1
                    });
                crate::cache::CachedFooter {
                    schema: Some(at),
                    row_group_rows: f.row_group_rows.clone(),
                    row_group_bytes: f.row_group_bytes.clone(),
                }
            }
        })
        .collect();
    (cached, schemas)
}

/// The footers a cache kept, back in the form a fresh pass would have produced.
///
/// `None` for a file whose footer would not read, which is how the pass reports one and
/// so how the cache has to give it back: a reopen that quietly read it again would be a
/// different dataset from the one that was cached.
///
/// A schema index the table does not have means the cache is inconsistent with itself,
/// and the whole entry is refused rather than half-used.
pub fn footers_from_cache(
    cached: &[crate::cache::CachedFooter],
    schemas: &[Vec<(String, polars::prelude::DataType)>],
) -> Option<Vec<Option<FileFooter>>> {
    cached
        .iter()
        .map(|f| {
            let Some(at) = f.schema else {
                return Some(None);
            };
            let columns = schemas.get(at)?;
            let mut schema = Schema::with_capacity(columns.len());
            for (name, dtype) in columns {
                schema.with_column(name.as_str().into(), dtype.clone());
            }
            Some(Some(FileFooter {
                schema: Arc::new(schema),
                row_group_rows: f.row_group_rows.clone(),
                row_group_bytes: f.row_group_bytes.clone(),
            }))
        })
        .collect()
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

pub(crate) async fn footer_of_file(
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
            let (files, _skipped) = list_dataset_files(&store, "data/", None).await.unwrap();
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
            let (wide_files, _skipped) = list_dataset_files(&store, "wide/", None).await.unwrap();
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
            let (files, _skipped) = list_dataset_files(&store, "s/", None).await.unwrap();
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
                remembered: None,
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
            stamp: 0,
            etag: None,
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
            stamp: 0,
            etag: None,
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

    /// Footers survive the cache unchanged, unreadable ones included.
    ///
    /// Everything downstream — the union, the drift groups, the row numbering, the
    /// notes — is computed from these shapes, so a reopen is only as right as this
    /// round trip. A file whose footer would not read has to come back as one that
    /// would not read: a reopen that quietly read it again would build a different
    /// dataset from the one it was told to remember, and nothing would say so.
    #[test]
    fn a_footer_pass_survives_the_cache_and_comes_back_the_same() {
        use polars::prelude::DataType;

        let schema_of = |cols: &[(&str, DataType)]| {
            let mut schema = Schema::with_capacity(cols.len());
            for (name, dtype) in cols {
                schema.with_column((*name).into(), dtype.clone());
            }
            Arc::new(schema)
        };
        let original = vec![
            Some(FileFooter {
                schema: schema_of(&[("id", DataType::Int64), ("note", DataType::String)]),
                row_group_rows: vec![100, 50],
                row_group_bytes: vec![4_096, 2_048],
            }),
            // A second file with the same shape: the schema table must hold it once.
            Some(FileFooter {
                schema: schema_of(&[("id", DataType::Int64), ("note", DataType::String)]),
                row_group_rows: vec![7],
                row_group_bytes: vec![512],
            }),
            // One that drifted, and one that would not read at all.
            Some(FileFooter {
                schema: schema_of(&[("id", DataType::Int64), ("extra", DataType::Boolean)]),
                row_group_rows: vec![3],
                row_group_bytes: vec![128],
            }),
            None,
        ];

        let (cached, schemas) = footers_to_cache(&original);
        assert_eq!(
            schemas.len(),
            2,
            "two distinct shapes among four files, not four copies of them"
        );
        assert_eq!(cached[3].schema, None, "and the unreadable one says so");

        let back = footers_from_cache(&cached, &schemas).expect("the table is consistent");
        assert_eq!(back.len(), original.len());
        for (before, after) in original.iter().zip(&back) {
            match (before, after) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    assert_eq!(a.schema, b.schema, "same columns, same types");
                    assert_eq!(a.row_group_rows, b.row_group_rows);
                    assert_eq!(a.row_group_bytes, b.row_group_bytes);
                }
                _ => panic!("a footer changed whether it could be read"),
            }
        }

        // An index the table does not have means the entry disagrees with itself, and
        // half of it is worse than none.
        let broken = vec![crate::cache::CachedFooter {
            schema: Some(9),
            row_group_rows: vec![1],
            row_group_bytes: vec![1],
        }];
        assert!(
            footers_from_cache(&broken, &schemas).is_none(),
            "an entry that points at a schema it does not have is refused whole"
        );
    }

    /// A dataset too large to open in one wave is remembered by the pass behind it.
    ///
    /// This is the case the cache exists for — a prefix whose footers cost seconds —
    /// and it is the one that used to be missed. Such a dataset opens from two footers
    /// and reads the rest behind the data, so the open itself has nothing worth
    /// keeping; saving only there meant the cache held nothing but datasets small
    /// enough to open in a single wave, which are the cheapest to read anyway.
    ///
    /// Also pins the guard that keeps the two-footer view out: caching it would hand
    /// the next open a five-thousand-file dataset with two files' worth of schema, no
    /// row numbering and sample-scoped notes, and nothing would say so.
    #[test]
    fn a_dataset_read_behind_the_open_is_remembered_by_the_pass_that_read_it() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = || {
            let mut frame = df!("n" => &[1i64]).unwrap();
            let mut out = Vec::new();
            ParquetWriter::new(&mut out).finish(&mut frame).unwrap();
            out
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        // One past the wave, so the open stages.
        let count = FOOTERS_AT_ONCE + 1;
        rt.block_on(async {
            for i in 0..count {
                store
                    .put(
                        &OsPath::from(format!("data/f{i:04}.parquet")),
                        PutPayload::from(body()),
                    )
                    .await
                    .unwrap();
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(dir.path().to_path_buf());
        let report = || crate::measurements::OpenReport {
            progress: Arc::new(crate::schema_union::FooterProgress::default()),
            meter: Arc::new(crate::measurements::Meter::default()),
            remembered: Some(cache.clone()),
        };
        let open = || {
            let r = report();
            let state = crate::App::schema_state_from_cloud_hive_with(
                "memory://data/".to_string(),
                "data/".to_string(),
                store.clone(),
                polars::prelude::cloud::CloudOptions::default(),
                &crate::OpenOptions::default(),
                rt.handle(),
                &r,
            );
            (state, r.meter.clone())
        };

        let (state, meter) = open();
        assert_eq!(
            meter.footers().and_then(|c| c.files),
            Some(2),
            "the open itself reads the two ends, which is what staging is"
        );
        assert!(
            cache.load_dataset_shapes().is_empty(),
            "and keeps nothing: a two-footer view of {count} files is not this dataset, \
             and kept as one it would open next time with two files' worth of schema"
        );

        // The pass behind it reads every footer, and that is the one worth keeping.
        let pending = state
            .as_ref()
            .and_then(|s| s.footers_pending())
            .expect("a staged open leaves a pass behind it");
        let _ = pending(&Arc::new(crate::schema_union::FooterProgress::default()));

        let (_, second) = open();
        assert_eq!(
            second.footers(),
            None,
            "so the next open reads no footers at all, for a dataset of {count} files"
        );
    }

    /// A footer that would not read this time is not remembered as unreadable forever.
    ///
    /// A read fails for a corrupt file and for a throttled request alike, and nothing
    /// here can tell them apart. Keeping the failure would turn a moment's trouble into
    /// a file missing from the dataset on every open from now until something else in
    /// the prefix changes — and the note would go on saying one file could not be read,
    /// about a file that reads perfectly well.
    #[test]
    fn a_footer_that_would_not_read_is_not_remembered_as_unreadable() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let good = || {
            let mut frame = df!("n" => &[1i64]).unwrap();
            let mut out = Vec::new();
            ParquetWriter::new(&mut out).finish(&mut frame).unwrap();
            out
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            store
                .put(&OsPath::from("data/a.parquet"), PutPayload::from(good()))
                .await
                .unwrap();
            // Mid-write, or throttled, or a token that expired: all the same from here.
            store
                .put(
                    &OsPath::from("data/b.parquet"),
                    PutPayload::from(b"not parquet yet".to_vec()),
                )
                .await
                .unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(dir.path().to_path_buf());
        let open = || {
            let meter = Arc::new(crate::measurements::Meter::default());
            let _ = crate::App::schema_state_from_cloud_hive_with(
                "memory://data/".to_string(),
                "data/".to_string(),
                store.clone(),
                polars::prelude::cloud::CloudOptions::default(),
                &crate::OpenOptions::default(),
                rt.handle(),
                &crate::measurements::OpenReport {
                    progress: Arc::new(crate::schema_union::FooterProgress::default()),
                    meter: meter.clone(),
                    remembered: Some(cache.clone()),
                },
            );
            meter
        };

        let first = open();
        assert_eq!(
            first.footers().and_then(|c| c.files),
            Some(2),
            "both were tried"
        );
        assert!(
            cache.load_dataset_shapes().is_empty(),
            "and nothing was kept, because one of them did not come back"
        );

        let second = open();
        assert_eq!(
            second.footers().and_then(|c| c.files),
            Some(2),
            "so the next open tries again rather than taking the failure as settled"
        );
    }

    /// Opening a dataset a second time reads no footers, and a changed one does.
    ///
    /// This is what the cache is for. The listing happens either way — it is how datui
    /// knows what the dataset is now — and it is what decides whether the footers can
    /// be skipped. The Footers measurement is how the test can tell: a remembered open
    /// records none, because none were read.
    #[test]
    fn a_dataset_opened_again_is_not_read_again() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = |rows: i64| {
            let mut frame = df!("n" => (0..rows).collect::<Vec<i64>>()).unwrap();
            let mut out = Vec::new();
            ParquetWriter::new(&mut out).finish(&mut frame).unwrap();
            out
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for (key, rows) in [("data/a.parquet", 3i64), ("data/b.parquet", 4)] {
                store
                    .put(&OsPath::from(key), PutPayload::from(body(rows)))
                    .await
                    .unwrap();
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(dir.path().to_path_buf());
        let open = |store: Arc<dyn ObjectStore>| {
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
                    remembered: Some(cache.clone()),
                },
            );
            meter
        };

        let first = open(store.clone());
        assert_eq!(
            first.footers().and_then(|c| c.files),
            Some(2),
            "the first open reads both footers"
        );

        let second = open(store.clone());
        assert_eq!(
            second.listing().and_then(|c| c.files),
            Some(2),
            "the second lists the prefix, which is how it knows nothing has changed"
        );
        assert_eq!(
            second.footers(),
            None,
            "and reads no footers at all, which is what remembering them is for"
        );

        // A file rewritten: the fingerprint moves and the cache is ignored.
        rt.block_on(async {
            store
                .put(&OsPath::from("data/b.parquet"), PutPayload::from(body(9)))
                .await
                .unwrap();
        });
        let third = open(store);
        assert_eq!(
            third.footers().and_then(|c| c.files),
            Some(2),
            "a dataset that has changed is read again rather than remembered wrongly"
        );
    }

    /// A glob opens through the route that gives it the schema union and the count.
    ///
    /// Through `schema_state_from_cloud_hive_with`, which is the function that connects
    /// the pattern to the listing — the pieces each work on their own, and the bug this
    /// closes (#228) was in the joining. Handing the starred key to the listing lists a
    /// prefix containing a literal `*`, matches nothing, and drops the open onto a
    /// whole-dataset scan with none of phases 1-5, silently.
    ///
    /// The scan past the schema cannot open a `memory://` URL and the route returns
    /// `None`, which is fine: the listing and the footers have happened by then, and
    /// the meter is what this reads them off.
    #[test]
    fn a_glob_reaches_the_route_that_lists_and_reads_it() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = || {
            let mut frame = df!("n" => &[1i64]).unwrap();
            let mut out = Vec::new();
            ParquetWriter::new(&mut out).finish(&mut frame).unwrap();
            out
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for key in [
                "data/year=2024/a.parquet",
                "data/year=2025/b.parquet",
                "data/other/c.parquet",
            ] {
                store
                    .put(&OsPath::from(key), PutPayload::from(body()))
                    .await
                    .unwrap();
            }
        });

        let meter = Arc::new(crate::measurements::Meter::default());
        let _ = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/year=*/*.parquet".to_string(),
            "data/year=*/*.parquet".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: Arc::new(crate::schema_union::FooterProgress::default()),
                meter: meter.clone(),
                remembered: None,
            },
        );

        assert_eq!(
            meter.listing().and_then(|c| c.files),
            Some(2),
            "the listing found the two files the glob names — not the sibling folder \
             it does not. A starred key handed to the listing finds none of them"
        );
        assert_eq!(
            meter.footers().and_then(|c| c.files),
            Some(2),
            "and their footers were read, which is what a glob used to get none of"
        );

        // The root the notes measure each file's path against has to be a literal
        // prefix of those paths. Handing them the URL as typed gives a root with a star
        // in it, which is a prefix of nothing — so `with_partition_layouts` and the
        // column-range notes match no file and go quietly empty, and a glob silently
        // loses two families of note the docs say it gets.
        let full = "s3://bucket/data/year=*/*.parquet";
        let root = url_of_key(full, prefix_of_glob("data/year=*/*.parquet")).unwrap();
        assert_eq!(root, "s3://bucket/data");
        let file_url = url_of_key(full, "data/year=2024/a.parquet").unwrap();
        assert!(
            file_url.starts_with(&root),
            "{file_url} has to sit under {root}, or every note measured from the root \
             is silently about no files at all"
        );
        assert!(!file_url.starts_with(full), "which the URL as typed is not");
    }

    /// A glob opens as a dataset, not as whatever Polars makes of it.
    ///
    /// datui lists the literal part of the key and matches the rest itself. Handing the
    /// star to the object store lists a prefix containing a literal `*`, which matches
    /// nothing — so every glob used to fall through to a whole-dataset scan and get
    /// none of the schema union, the row count, the notes or the measurements (#228).
    #[test]
    fn a_glob_opens_the_files_it_names_and_no_others() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = || {
            let mut frame = df!("n" => &[1i64]).unwrap();
            let mut out = Vec::new();
            ParquetWriter::new(&mut out).finish(&mut frame).unwrap();
            out
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for key in [
                "data/year=2024/a.parquet",
                "data/year=2025/b.parquet",
                "data/other/c.parquet",
                "elsewhere/d.parquet",
            ] {
                store
                    .put(&OsPath::from(key), PutPayload::from(body()))
                    .await
                    .unwrap();
            }
        });

        // The literal prefix a glob lists from.
        assert_eq!(prefix_of_glob("data/year=*/*.parquet"), "data");
        assert_eq!(prefix_of_glob("data/*.parquet"), "data");
        assert_eq!(prefix_of_glob("*.parquet"), "");
        assert_eq!(prefix_of_glob("data/plain.parquet"), "data/plain.parquet");

        let matcher = globset::GlobBuilder::new("data/year=*/*.parquet")
            .literal_separator(true)
            .build()
            .unwrap()
            .compile_matcher();
        let (files, _skipped) = rt
            .block_on(async {
                list_dataset_files(
                    &store,
                    prefix_of_glob("data/year=*/*.parquet"),
                    Some(&matcher),
                )
                .await
            })
            .expect("the prefix lists");
        let keys: Vec<&str> = files.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(
            keys,
            ["data/year=2024/a.parquet", "data/year=2025/b.parquet"],
            "the two the glob names — not the sibling folder it does not, and not the \
             one outside the prefix altogether"
        );

        // `literal_separator` is what keeps a single star inside one path segment.
        assert!(
            !matcher.is_match("data/year=2024/deeper/a.parquet"),
            "a single star does not cross a slash"
        );
    }

    /// A folder is the same table whether it is read from a disk or a bucket.
    ///
    /// The two listings used to disagree in one direction: the local walk checked the
    /// extension, so it missed the `occurrence.parquet/part-00001` shape that Spark and
    /// GBIF write, where the part files have no extension and only the folder name says
    /// what they are. Both now ask `is_parquet_key`.
    ///
    /// The `_`-prefixed row of the fixture is not what this is testing — the local walk
    /// classified those as the writer's own bookkeeping before this change too. It is
    /// here because the two routes reaching the same answer by different means is the
    /// thing worth pinning, not just the one case that moved.
    #[test]
    fn a_folder_is_the_same_table_from_a_disk_or_a_bucket() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let body = || {
            let mut frame = df!("n" => &[1i64]).unwrap();
            let mut out = Vec::new();
            ParquetWriter::new(&mut out).finish(&mut frame).unwrap();
            out
        };
        // One of each shape the two routes used to disagree about.
        let layout = [
            ("date=1/data.parquet", true),
            ("date=1/_2024.parquet", false),
            ("occurrence.parquet/part-00001", true),
            ("date=1/notes.csv", false),
        ];

        let dir = tempfile::tempdir().unwrap();
        for (rel, _) in layout {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, body()).unwrap();
        }
        let (local, _read, _footers, _skipped) =
            crate::widgets::datatable::DataTableState::footers_of_parquet_dir_reporting(
                dir.path(),
                &crate::schema_union::FooterProgress::default(),
                &crate::measurements::Meter::default(),
            );
        let mut from_disk: Vec<String> = local
            .iter()
            .map(|p| {
                p.strip_prefix(dir.path())
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();
        from_disk.sort();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for (rel, _) in layout {
                store
                    .put(
                        &OsPath::from(format!("data/{rel}")),
                        PutPayload::from(body()),
                    )
                    .await
                    .unwrap();
            }
        });
        let (cloud, _skipped) = rt
            .block_on(async { list_dataset_files(&store, "data/", None).await })
            .expect("the prefix lists");
        let mut from_bucket: Vec<String> = cloud
            .iter()
            .map(|f| f.key.trim_start_matches("data/").to_string())
            .collect();
        from_bucket.sort();

        let mut wanted: Vec<String> = layout
            .iter()
            .filter(|(_, keep)| *keep)
            .map(|(rel, _)| rel.to_string())
            .collect();
        wanted.sort();

        assert_eq!(from_disk, wanted, "the disk reads the table");
        assert_eq!(from_bucket, wanted, "and the bucket reads the same one");
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
                remembered: None,
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
                remembered: None,
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
                remembered: None,
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
                remembered: None,
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
            let (files, _skipped) = list_dataset_files(&store, "data/", None).await.unwrap();
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
            let (files, _skipped) = list_dataset_files(&store, "data", None).await.unwrap();
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
            let (listed, _skipped) = list_dataset_files(&store, "data", None).await.unwrap();
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
            let (listed, _skipped) = list_dataset_files(&store, "data", None).await.unwrap();
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
                remembered: None,
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
            .block_on(list_dataset_files(&store, "t", None))
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
                remembered: None,
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
            .block_on(list_dataset_files(&store, "data", None))
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
