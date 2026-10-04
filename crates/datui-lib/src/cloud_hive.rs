//! Parquet in an object store, short of its data: a prefix listed in key ranges at
//! once, and footers read by small ranged reads. [`crate::dataset_files::StoreFiles`]
//! opens a dataset over these.

use color_eyre::Result;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt};
use polars::prelude::Schema;
use std::sync::Arc;

use crate::dataset_files::DatasetFile;
pub use crate::schema_union::FileFooter;
#[cfg(test)]
use crate::schema_union::lenient_scan;
#[cfg(test)]
use crate::schema_union::with_partition_columns;

const PARQUET_FOOTER_TAIL_BYTES: usize = 256 * 1024;

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

/// One object's footer, with each column's width, and the store's tag for the object:
/// a head for its size, then one tail read. Does not fetch the data.
///
/// Two requests, not one: this route does not know the object's size, so it asks before
/// it reads. Both are counted against `meter`.
pub async fn footer_of_cloud_parquet(
    store: Arc<dyn ObjectStore>,
    key: &str,
    meter: &crate::measurements::Meter,
) -> Result<(FileFooter, Option<String>)> {
    let began = std::time::Instant::now();
    let path = crate::cloud_browse::object_path(key);
    let read = async {
        let head = store.head(&path).await;
        // A `head` returns no body, so it is a request that brought back nothing.
        meter.footer_request(0);
        let meta = head.map_err(|e| color_eyre::eyre::eyre!("Cloud head failed: {}", e))?;
        let size = meta.size;
        let start = size.saturating_sub(PARQUET_FOOTER_TAIL_BYTES as u64);
        let tail = counted_range!(store, &path, start..size, meter)?;
        FileFooter::from_tail(&tail, size as usize, true).map(|footer| (footer, meta.e_tag))
    };
    let footer = read.await;
    meter.read_footers(began.elapsed(), Some(1), true);
    footer
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
#[cfg(test)]
pub async fn list_dataset_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    pattern: Option<&globset::GlobMatcher>,
) -> Result<(Vec<DatasetFile>, crate::schema_union::SkippedFiles)> {
    let progress = crate::schema_union::FooterProgress::default();
    let listing = progress.listing();
    list_dataset_files_reporting(
        store,
        prefix,
        pattern,
        ListShards::ONE,
        listing.counter(),
        listing.cancel_flag(),
    )
    .await
}

/// As [`list_dataset_files`], counting each object off against `listed` as it is
/// listed, and stopping once `cancelled` is set. A prefix of a few hundred thousand
/// objects is hundreds of pages, and this count is all the loading screen has to say
/// about them.
pub async fn list_dataset_files_reporting(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    pattern: Option<&globset::GlobMatcher>,
    plan: ListShards,
    listed: Arc<std::sync::atomic::AtomicUsize>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(Vec<DatasetFile>, crate::schema_union::SkippedFiles)> {
    let prefix = prefix.trim_matches('/');
    let prefix_path = (!prefix.is_empty()).then(|| crate::cloud_browse::object_path(prefix));
    let objects = list_objects(store, prefix_path.as_ref(), plan, listed, cancelled).await?;
    // Counted as they are passed over rather than walked again: the listing is the one
    // place that sees every name, and a note that says how many objects were not read
    // costs nothing here and a second listing anywhere else.
    fn directory_of(key: &str) -> &str {
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
    // happens to live under a `_`-named directory is not written off entirely.
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
    // Every directory with data anywhere beneath it, which is every directory on the way
    // down to a file this keeps. What else is in one of those is beside somebody's data;
    // what is anywhere else is somebody's infrastructure, whatever the format calls it —
    // see `SkippedFiles`.
    let mut with_data: std::collections::HashSet<&str> = std::collections::HashSet::new();
    // From every object whose name says data, not only the ones kept: a write that
    // stopped leaves nothing behind, and a partition whose only file is that write
    // would otherwise be a directory with no data in it — so the one skip most worth
    // saying would be filed as plumbing, in exactly the case that matters.
    for f in all
        .iter()
        .filter(|f| !bookkeeping_of(&f.key) && crate::discover::is_parquet_key(&f.key))
    {
        let mut directory = directory_of(&f.key);
        while !directory.is_empty() && with_data.insert(directory) {
            directory = directory_of(directory);
        }
        with_data.insert("");
    }
    // A partition of a dataset is part of it even when its own files all failed to be
    // Parquet: a day that landed as CSV is the mistake this note is for. A directory
    // whose name carries a partition key, under one that holds data, is one of those. A
    // `metadata/` beside the data is not.
    let beside_data = |directory: &str| {
        with_data.contains(directory)
            || (directory
                .rsplit('/')
                .next()
                .unwrap_or(directory)
                .contains('=')
                && with_data.contains(directory_of(directory)))
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
            || !beside_data(directory_of(&f.key))
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

/// How a listing is shared out among concurrent requests.
///
/// A listing is a chain of pages, each request naming where the last one stopped, so
/// one prefix of 842,000 objects is 843 round trips one after another. Split into
/// ranges of keys, each range is its own chain and they run side by side.
#[derive(Debug, Clone, Copy)]
pub struct ListShards {
    /// Ranges listed at once.
    pub at_once: usize,
    /// Ranges made in all. Each costs at least one request, and its last page usually
    /// runs past its end into keys the next range lists.
    pub most: usize,
    /// Keys a range lists before it looks to divide what is left of it: one page.
    pub split_after: usize,
    /// New ranges one range divides off at a time, room allowing.
    pub split_into: usize,
}

impl ListShards {
    /// One range, listed from start to end.
    pub const ONE: Self = Self {
        at_once: 1,
        most: 1,
        split_after: usize::MAX,
        split_into: 0,
    };
    /// For a store that starts a listing from a key itself (S3, Google Cloud). One that
    /// does not lists everything and filters, so each range would cost a whole listing.
    ///
    /// Tuned against `by_station`'s 842,225 keys, replayed with 110 ms a page: 64 at
    /// once, four at a time, lists it in 3 to 4 s and about 1,400 pages where one range
    /// takes 843 pages and 93 s. The cap on ranges made bounds the extra pages; with it
    /// too low, a busy range can no longer divide and the listing waits on it.
    pub const PARALLEL: Self = Self {
        at_once: 64,
        most: 1024,
        split_after: 1000,
        split_into: 4,
    };

    /// How to list the prefix at `url`: in parallel where the store lists from an
    /// offset itself. Azure's emulator and S3 Express do not, and are not told apart
    /// from the real thing by the URL alone, so Azure lists in one range, as does S3
    /// Express by its bucket suffix.
    pub fn for_url(url: &str) -> Self {
        let Some((scheme, rest)) = url.split_once("://") else {
            return Self::ONE;
        };
        let bucket = rest.split('/').next().unwrap_or("");
        match scheme.to_ascii_lowercase().as_str() {
            "s3" | "s3a" if !bucket.ends_with("--x-s3") => Self::PARALLEL,
            "gs" | "gcs" => Self::PARALLEL,
            _ => Self::ONE,
        }
    }
}

/// A range of keys to list: those after `after`, up to and including `through`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyRange {
    after: Option<String>,
    through: Option<String>,
}

/// Characters a split point is made from, in byte order. A key may hold others; it
/// still falls in exactly one range, since ranges are bounded by these points and not
/// by what the keys contain.
const SPLIT_ALPHABET: &[u8] = b"-.0123456789=ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
/// Characters past the part a range's keys share that a split point is placed by.
const SPLIT_DEPTH: usize = 6;

/// The characters a key may hold at one position, judged from the keys that do: the
/// whole class (digits, capitals, lower case) of each one seen there, and any of the
/// alphabet's punctuation seen there as itself. A station ID is capitals then digits,
/// and a point made with a lower-case letter there would be a range with nothing in it.
fn alphabet_at(seen: &[u8]) -> Vec<u8> {
    let any = |test: fn(&u8) -> bool| seen.iter().any(test);
    let (digits, upper, lower) = (
        any(u8::is_ascii_digit),
        any(u8::is_ascii_uppercase),
        any(u8::is_ascii_lowercase),
    );
    SPLIT_ALPHABET
        .iter()
        .copied()
        .filter(|c| {
            (digits && c.is_ascii_digit())
                || (upper && c.is_ascii_uppercase())
                || (lower && c.is_ascii_lowercase())
                || (!c.is_ascii_alphanumeric() && seen.contains(c))
        })
        .collect()
}

/// Where to divide the keys after `last`, up to `through`, into `n` more ranges.
///
/// Nothing is known of the keys ahead but the shape of those behind, so the remaining
/// range is cut evenly, reading the characters past the part every key in it shares
/// (`last` and `through` agree on that much, and nothing inside a listing prefix of
/// `fixed` bytes or a partition's `name=` varies) as the digits of a number. Each
/// position counts only the kinds of character `first`, `last` and `through` have
/// there, so a run of digits is cut among digits.
///
/// Even cuts of a skewed range are uneven in keys: past `STATION=`, 72% of
/// `by_station`'s keys begin with `U`. That is why a range divides again after every
/// page while there is room, rather than once: a busy part is cut again where it is
/// busy, and an empty one costs one request.
fn split_points(
    first: &str,
    last: &str,
    through: Option<&str>,
    fixed: usize,
    n: usize,
) -> Vec<String> {
    fn common(a: &str, b: &str) -> usize {
        a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
    }
    let mut shared = through.map_or(0, |t| common(last, t)).max(fixed);
    while !last.is_char_boundary(shared.min(last.len())) {
        shared -= 1;
    }
    // Past a partition's name: every key in the range has the same one.
    let segment = last[..shared.min(last.len())]
        .rfind('/')
        .map_or(0, |slash| slash + 1);
    if let Some(equals) = last[segment..]
        .find(['=', '/'])
        .map(|at| segment + at)
        .filter(|&at| last.as_bytes()[at] == b'=' && shared <= at)
    {
        shared = equals + 1;
    }
    if n == 0 || shared >= last.len() {
        return Vec::new();
    }
    let base = &last[..shared];
    let digits_of = |key: &str| -> Vec<u8> {
        key.strip_prefix(base)
            .map(|rest| rest.bytes().take(SPLIT_DEPTH).collect())
            .unwrap_or_default()
    };
    let keys = [
        digits_of(first),
        digits_of(last),
        through.map(digits_of).unwrap_or_default(),
    ];
    let mut alphabets: Vec<Vec<u8>> = (0..SPLIT_DEPTH)
        .map(|i| {
            alphabet_at(
                &keys
                    .iter()
                    .filter_map(|k| k.get(i).copied())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    // Past the end of every key seen, whatever comes next is a guess; the class of the
    // last position known stands in for it.
    for i in 1..SPLIT_DEPTH {
        if alphabets[i].is_empty() {
            alphabets[i] = alphabets[i - 1].clone();
        }
    }
    if alphabets[0].is_empty() {
        return Vec::new();
    }
    // A key's place under `base` as a fraction: its characters as the digits of a
    // number whose radix at each position is that position's alphabet. A character
    // between two of the alphabet's sits half way.
    let value = |key: &str| -> f64 {
        let Some(rest) = key.strip_prefix(base) else {
            return if key < base { 0.0 } else { 1.0 };
        };
        let (mut v, mut scale) = (0.0, 1.0);
        for (c, alphabet) in rest.bytes().zip(&alphabets) {
            scale /= alphabet.len() as f64;
            let at = alphabet.partition_point(|&a| a < c);
            let digit = if alphabet.get(at) == Some(&c) {
                at as f64
            } else {
                at as f64 - 0.5
            };
            v += digit * scale;
        }
        v
    };
    let point_at = |mut v: f64| -> String {
        let mut point = base.to_string();
        for alphabet in &alphabets {
            v *= alphabet.len() as f64;
            let digit = (v.floor().max(0.0) as usize).min(alphabet.len() - 1);
            point.push(alphabet[digit] as char);
            v -= digit as f64;
            if v <= 0.0 {
                break;
            }
        }
        point
    };
    let (from, to) = (value(last), through.map_or(1.0, value));
    if to <= from {
        return Vec::new();
    }
    let mut points: Vec<String> = (1..=n)
        .map(|i| point_at(from + (to - from) * i as f64 / (n + 1) as f64))
        .filter(|p| p.as_str() > last && through.is_none_or(|t| p.as_str() < t))
        .filter(|p| OsPath::parse(p).is_ok())
        .collect();
    points.sort();
    points.dedup();
    points
}

/// The listing's bookkeeping: ranges running and ranges made, against the plan's
/// limits.
struct Sharing {
    plan: ListShards,
    counts: std::sync::Mutex<(usize, usize)>,
}

impl Sharing {
    /// Room for up to `want` more ranges, taken now.
    fn take(&self, want: usize) -> usize {
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        let (running, made) = *counts;
        let room = want
            .min(self.plan.at_once.saturating_sub(running))
            .min(self.plan.most.saturating_sub(made));
        *counts = (running + room, made + room);
        room
    }

    /// `n` ranges taken and not used, or finished.
    fn give_back(&self, n: usize, made: bool) {
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        counts.0 = counts.0.saturating_sub(n);
        if !made {
            counts.1 = counts.1.saturating_sub(n);
        }
    }
}

/// What every range of one listing shares.
#[derive(Clone)]
struct RangeLister {
    store: Arc<dyn ObjectStore>,
    prefix: Option<OsPath>,
    /// Bytes of every key that are the prefix and its `/`.
    fixed: usize,
    sharing: Arc<Sharing>,
    /// Where a range sends the ranges it divides off.
    more: tokio::sync::mpsc::UnboundedSender<KeyRange>,
    listed: Arc<std::sync::atomic::AtomicUsize>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

/// List one range, dividing what is left of it into new ranges while there is room
/// for them.
async fn list_range(lister: RangeLister, range: KeyRange) -> Result<Vec<object_store::ObjectMeta>> {
    use futures::StreamExt;
    use std::sync::atomic::Ordering;
    let RangeLister {
        store,
        prefix,
        fixed,
        sharing,
        more,
        listed,
        cancelled,
    } = lister;
    let mut stream = match &range.after {
        None => store.list(prefix.as_ref()),
        Some(after) => {
            let offset = OsPath::parse(after)
                .map_err(|e| color_eyre::eyre::eyre!("Cloud list failed: {}", e))?;
            store.list_with_offset(prefix.as_ref(), &offset)
        }
    };
    let mut through = range.through;
    let mut objects: Vec<object_store::ObjectMeta> = Vec::new();
    let mut first: Option<String> = None;
    let mut since = 0usize;
    while let Some(object) = stream.next().await {
        if cancelled.load(Ordering::Relaxed) {
            return Err(color_eyre::eyre::eyre!("Cloud list cancelled"));
        }
        let object = object.map_err(|e| color_eyre::eyre::eyre!("Cloud list failed: {}", e))?;
        let key = object.location.as_ref();
        // The rest belongs to the next range. Listings come back in key order, which
        // is the order the ranges are cut in.
        if through.as_deref().is_some_and(|through| key > through) {
            break;
        }
        if first.is_none() {
            first = Some(key.to_string());
        }
        since += 1;
        if since >= sharing.plan.split_after {
            since = 0;
            let room = sharing.take(sharing.plan.split_into);
            if room > 0 {
                let points = split_points(
                    first.as_deref().unwrap_or(key),
                    key,
                    through.as_deref(),
                    fixed,
                    room,
                );
                sharing.give_back(room - points.len(), false);
                if let Some(nearest) = points.first().cloned() {
                    let ends: Vec<Option<String>> = points
                        .iter()
                        .skip(1)
                        .cloned()
                        .map(Some)
                        .chain(std::iter::once(through.take()))
                        .collect();
                    for (after, through) in points.into_iter().zip(ends) {
                        let _ = more.send(KeyRange {
                            after: Some(after),
                            through,
                        });
                    }
                    through = Some(nearest);
                }
            }
        }
        objects.push(object);
        listed.fetch_add(1, Ordering::Relaxed);
    }
    Ok(objects)
}

/// Every object under `prefix`, in no particular order, listed in ranges as `plan`
/// allows: each range is a key past where the one before it ends, so together they
/// list every object once.
///
/// Stops at the first page after the load is abandoned: a listing nobody is waiting on
/// is hundreds of requests for nothing.
async fn list_objects(
    store: &Arc<dyn ObjectStore>,
    prefix: Option<&OsPath>,
    plan: ListShards,
    listed: Arc<std::sync::atomic::AtomicUsize>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
) -> Result<Vec<object_store::ObjectMeta>> {
    use futures::future::{Either, select};
    // Split points never fall inside the prefix, nor its `/`.
    let fixed = prefix.map_or(0, |p| p.as_ref().len() + 1);
    let sharing = Arc::new(Sharing {
        plan,
        counts: std::sync::Mutex::new((1, 1)),
    });
    let (more_tx, mut more_rx) = tokio::sync::mpsc::unbounded_channel::<KeyRange>();
    // Dropped with this future, which aborts every range still listing.
    let mut running = tokio::task::JoinSet::new();
    let lister = RangeLister {
        store: store.clone(),
        prefix: prefix.cloned(),
        fixed,
        sharing: sharing.clone(),
        more: more_tx,
        listed,
        cancelled,
    };
    let spawn = |running: &mut tokio::task::JoinSet<Result<Vec<object_store::ObjectMeta>>>,
                 range: KeyRange| {
        running.spawn(list_range(lister.clone(), range));
    };
    spawn(
        &mut running,
        KeyRange {
            after: None,
            through: None,
        },
    );
    let mut objects = Vec::new();
    loop {
        let next = {
            let joined = std::pin::pin!(running.join_next());
            let divided = std::pin::pin!(more_rx.recv());
            match select(joined, divided).await {
                Either::Left((joined, _)) => Either::Left(joined),
                Either::Right((range, _)) => Either::Right(range),
            }
        };
        match next {
            Either::Right(Some(range)) => spawn(&mut running, range),
            // The lister holds a sender, so the channel outlives the loop.
            Either::Right(None) => break,
            Either::Left(Some(joined)) => {
                sharing.give_back(1, true);
                let found =
                    joined.map_err(|e| color_eyre::eyre::eyre!("Cloud list failed: {}", e))??;
                objects.extend(found);
            }
            // A range sends what it divides off before it finishes, so anything it
            // sent is waiting here by the time the last one is joined.
            Either::Left(None) => match more_rx.try_recv() {
                Ok(range) => spawn(&mut running, range),
                Err(_) => break,
            },
        }
    }
    let made = sharing.counts.lock().map(|c| c.1).unwrap_or_default();
    log::debug!(target: "datui", "listed {} objects in {made} ranges", objects.len());
    Ok(objects)
}

/// The schema to scan a dataset's files with, and its partition columns.
///
/// Every column any file has, from the footers the row count already reads, so a column
/// a vendor added for a month is visible rather than hidden behind whichever file the
/// schema was taken from. See [`crate::schema_union`] for the ordering and the type
/// rules; [`lenient_scan`] does the reading.
#[cfg(test)]
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
    let mut union = crate::schema_union::union_sampled(files.len(), read, footers);
    if union.schema.is_empty() {
        return Err(color_eyre::eyre::eyre!(
            "No readable parquet footer in cloud prefix"
        ));
    }

    let (partition_columns, values) =
        crate::schema_union::partitions_of_listing(&first.key, &newest.key);
    union.schema = Arc::new(with_partition_columns(
        &union.schema,
        &partition_columns,
        &values,
    ));
    Ok((union, partition_columns))
}

/// How many footers are read at once when counting.
pub const FOOTERS_AT_ONCE: usize = crate::schema_union::FOOTERS_AT_ONCE;
/// The first read of a footer. Most footers fit; a larger one costs a second request.
const COUNT_TAIL_BYTES: u64 = 16 * 1024;

/// Every file's footer, in file order: a small ranged read at the end of each file,
/// many at once. No data is read. A file whose footer cannot be read is `None` rather
/// than an error, so one object mid-write does not stop the dataset from opening.
#[cfg(test)]
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
    let cancelled = progress.cancel_flag();
    let mut reads = tokio::task::JoinSet::new();
    for (slot, file) in read
        .iter()
        .filter_map(|i| files.get(*i))
        .cloned()
        .enumerate()
    {
        let (store, permits, meter) = (store.clone(), permits.clone(), meter.clone());
        let cancelled = cancelled.clone();
        reads.spawn(async move {
            let _permit = permits.acquire_owned().await;
            // Checked at the permit, so an abandoned load stops issuing
            // requests within one wave instead of reading every footer for a
            // dataset nobody is waiting on.
            if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                return (slot, None);
            }
            (slot, footer_of_file(&store, &file, &meter).await.ok())
        });
    }
    let mut out = vec![None; read.len()];
    // One schema shared by every footer that has it. A prefix of 842,000 files usually
    // has a handful, and the count holds every footer until it has them all.
    let mut schemas: Vec<Arc<Schema>> = Vec::new();
    while let Some(joined) = reads.join_next().await {
        // Counted as it lands, whether or not it read: a footer that will not parse is
        // one the open is no longer waiting on.
        pass.advance();
        if let Ok((slot, footer)) = joined {
            out[slot] = footer.map(|mut footer: FileFooter| {
                match schemas.iter().find(|s| **s == footer.schema) {
                    Some(same) => footer.schema = same.clone(),
                    // Bounded, so a prefix whose every file differs is not searched
                    // end to end for each one.
                    None if schemas.len() < 64 => schemas.push(footer.schema.clone()),
                    None => {}
                }
                footer
            });
        }
    }
    drop(pass);
    // The requests are already counted — each read counted itself as it was made — so
    // this only hands the running total back to be stamped with how long the pass took.
    meter.read_footers(began.elapsed(), Some(read.len()), true);
    out
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
    FileFooter::from_tail(&tail, file.size as usize, false)
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
    use crate::schema_union::partition_columns_of_key as partition_columns_from_prefix;
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

    /// Split points lie strictly between the last key listed and the range's end, in
    /// order, and never inside the prefix or a partition's name.
    #[test]
    fn split_points_follow_the_keys_down() {
        let prefix = "parquet/by_station/";
        let first = "parquet/by_station/STATION=ACW00011604/ELEMENT=PGTM/a.parquet";
        let last = "parquet/by_station/STATION=AEM00041217/ELEMENT=TMAX/b.parquet";
        let points = split_points(first, last, None, prefix.len(), 3);
        assert_eq!(points.len(), 3, "{points:?}");
        assert!(points.windows(2).all(|w| w[0] < w[1]), "sorted, no repeats");
        assert!(points.iter().all(|p| p.as_str() > last));
        // Past the prefix and the partition's name, and among capitals, which is all
        // either key has there: a point made of digits or lower case would sit in
        // keys nobody has.
        for point in &points {
            let id = point
                .strip_prefix("parquet/by_station/STATION=")
                .unwrap_or_else(|| panic!("{point}"));
            assert!(id.starts_with(|c: char| c.is_ascii_uppercase()), "{point}");
        }

        // A busy range inside one country is cut inside it, among what its keys hold.
        let first = "parquet/by_station/STATION=US009052008/ELEMENT=PRCP/a.parquet";
        let last = "parquet/by_station/STATION=US1AKAB0001/ELEMENT=PRCP/a.parquet";
        let through = "parquet/by_station/STATION=US2";
        let points = split_points(first, last, Some(through), prefix.len(), 4);
        assert_eq!(points.len(), 4, "{points:?}");
        assert!(points.windows(2).all(|w| w[0] < w[1]));
        for point in &points {
            assert!(point.as_str() > last && point.as_str() < through, "{point}");
            assert!(
                point.starts_with("parquet/by_station/STATION=US1"),
                "{point}"
            );
        }

        assert!(split_points(first, last, Some(through), prefix.len(), 0).is_empty());
        // Nothing between a key and the key after it.
        assert!(split_points(first, last, Some(&format!("{last}0")), prefix.len(), 4).is_empty());
    }

    /// Keys shaped like `by_station`, skewed the way it is, plus keys a split point
    /// lands on exactly, odd characters, and neighbours outside the prefix.
    fn skewed_keys() -> Vec<String> {
        let mut keys = Vec::new();
        let elements = ["PRCP", "SNOW", "TMAX"];
        let mut station = |code: String| {
            for element in elements {
                keys.push(format!(
                    "p/by_station/STATION={code}/ELEMENT={element}/x_0.snappy.parquet"
                ));
            }
        };
        for i in 0..60 {
            station(format!("AC{i:09}"));
        }
        for country in ["BR", "CA", "GM", "SF", "UK"] {
            for i in 0..40 {
                station(format!("{country}{i:09}"));
            }
        }
        for kind in ["1AK", "1CA", "1TX", "C00", "W00"] {
            for i in 0..300 {
                station(format!("US{kind}{i:06}"));
            }
        }
        for odd in [
            "p/by_station/STATION=B",
            "p/by_station/STATION=U",
            "p/by_station/STATION=US1B",
            "p/by_station/STATION=Z~tilde/a.parquet",
            "p/by_station/STATION=ü/a.parquet",
            "p/by_station/_SUCCESS",
            "p/by_station/zz/a.parquet",
        ] {
            keys.push(odd.to_string());
        }
        keys
    }

    /// Listing in ranges finds exactly what one listing does: every key once, none
    /// missing, none outside the prefix.
    #[test]
    fn a_listing_in_ranges_finds_what_one_listing_does() {
        use object_store::PutPayload;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let keys = skewed_keys();
        rt.block_on(async {
            for key in keys.iter().map(String::as_str).chain([
                "p/by_stationx/a.parquet",
                "p/a.parquet",
                "q/b.parquet",
            ]) {
                store
                    .put(&OsPath::from(key), PutPayload::from(b"x".to_vec()))
                    .await
                    .unwrap();
            }
            let prefix = OsPath::from("p/by_station");
            let (listed, cancelled): (
                Arc<std::sync::atomic::AtomicUsize>,
                Arc<std::sync::atomic::AtomicBool>,
            ) = (Arc::default(), Arc::default());
            let keys_of = |objects: Vec<object_store::ObjectMeta>| {
                let mut keys: Vec<String> = objects
                    .into_iter()
                    .map(|o| o.location.as_ref().to_string())
                    .collect();
                keys.sort();
                keys
            };
            let one = keys_of(
                list_objects(
                    &store,
                    Some(&prefix),
                    ListShards::ONE,
                    listed.clone(),
                    cancelled.clone(),
                )
                .await
                .unwrap(),
            );
            let mut expected: Vec<String> = keys
                .iter()
                .map(|k| OsPath::from(k.as_str()).to_string())
                .collect();
            expected.sort();
            assert_eq!(one, expected);
            for (at_once, most, split_after, split_into) in [
                (2, 8, 10, 1),
                (8, 64, 25, 4),
                (64, 512, 7, 64),
                (4, 1000, 1, 2),
                (64, 1024, 30, 4),
            ] {
                let plan = ListShards {
                    at_once,
                    most,
                    split_after,
                    split_into,
                };
                let ranges = keys_of(
                    list_objects(
                        &store,
                        Some(&prefix),
                        plan,
                        listed.clone(),
                        cancelled.clone(),
                    )
                    .await
                    .unwrap(),
                );
                assert_eq!(ranges.len(), one.len(), "{plan:?}: a key twice or missing");
                assert_eq!(ranges, one, "{plan:?}");
            }

            // And through the whole listing, the dataset files come out the same.
            let (whole, skipped) = list_dataset_files(&store, "p/by_station", None)
                .await
                .unwrap();
            let (shared, shared_skipped) = list_dataset_files_reporting(
                &store,
                "p/by_station",
                None,
                ListShards {
                    at_once: 8,
                    most: 64,
                    split_after: 20,
                    split_into: 4,
                },
                listed,
                cancelled,
            )
            .await
            .unwrap();
            assert_eq!(whole, shared);
            assert_eq!(skipped, shared_skipped);
        });
    }

    /// Only stores that start a listing from a key themselves list in ranges.
    #[test]
    fn only_stores_that_list_from_an_offset_list_in_ranges() {
        let parallel = |url: &str| ListShards::for_url(url).at_once > 1;
        assert!(parallel("s3://noaa-ghcn-pds/parquet/by_station/"));
        assert!(parallel("gs://bucket/data/"));
        assert!(!parallel("s3://my-bucket--usw2-az1--x-s3/data/"));
        assert!(!parallel("az://container/data/"));
        assert!(!parallel("https://example.com/data/"));
    }

    /// A listing counts every object it passes over, data or not, and stops saying so
    /// when it ends; a cancelled one stops.
    #[test]
    fn a_listing_counts_what_it_finds() {
        use object_store::PutPayload;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for key in ["data/a.parquet", "data/b.parquet", "data/_SUCCESS"] {
                store
                    .put(&OsPath::from(key), PutPayload::from(b"PAR1".to_vec()))
                    .await
                    .unwrap();
            }
            let progress = crate::schema_union::FooterProgress::default();
            {
                let listing = progress.listing();
                let (files, _skipped) = list_dataset_files_reporting(
                    &store,
                    "data/",
                    None,
                    ListShards::ONE,
                    listing.counter(),
                    listing.cancel_flag(),
                )
                .await
                .unwrap();
                assert_eq!(files.len(), 2);
                assert_eq!(progress.listed(), Some(3), "every object, data or not");
            }
            assert_eq!(progress.listed(), None, "a finished listing shows no count");
            let listing = progress.listing();
            assert_eq!(
                progress.listed(),
                Some(0),
                "a new listing starts from nothing"
            );

            progress.cancel();
            assert!(
                list_dataset_files_reporting(
                    &store,
                    "data/",
                    None,
                    ListShards::ONE,
                    listing.counter(),
                    listing.cancel_flag(),
                )
                .await
                .is_err(),
                "an abandoned load stops listing"
            );
        });
    }

    /// An abandoned load's footer pass stops issuing reads: with the counter
    /// cancelled, the pass returns empty-handed and requests nothing.
    #[test]
    fn a_cancelled_pass_reads_no_footers() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let mut frame = df!("n" => (0..10i64).collect::<Vec<i64>>()).unwrap();
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for key in ["data/a.parquet", "data/b.parquet"] {
                store
                    .put(&OsPath::from(key), PutPayload::from(bytes.clone()))
                    .await
                    .unwrap();
            }
            let (files, _skipped) = list_dataset_files(&store, "data/", None).await.unwrap();
            let read: Vec<usize> = (0..files.len()).collect();
            let progress = crate::schema_union::FooterProgress::default();
            progress.cancel();
            let meter = Arc::new(crate::measurements::Meter::default());
            let footers =
                footers_of_files_reporting(&store, &files, &read, &progress, &meter).await;
            assert!(footers.iter().all(|f| f.is_none()), "nothing was read");
            let requests = meter
                .footers()
                .and_then(|cost| cost.over_the_wire)
                .map(|wire| wire.requests)
                .unwrap_or(0);
            assert_eq!(requests, 0, "nothing was requested");
        });
    }

    /// A remote dataset carries its row-group sizes through to the schema too.
    ///
    /// The two routes read their footers differently — a ranged read of the tail here,
    /// a whole local file there — but into the one `FileFooter`, which decides whether
    /// datui can say anything about row groups at all. Dropping the sizes on this side
    /// would leave the note working for local datasets and silent for the ones it
    /// exists for.
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
                writes: Default::default(),
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
        let source = crate::dataset_files::StoreFiles::new(
            "memory://data/",
            "data/".to_string(),
            None,
            store.clone(),
            polars::prelude::cloud::CloudOptions::default(),
            rt.handle(),
        );
        crate::dataset_files::footers_for_count(&source, &Arc::new(files), &[0], &meter);
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
        let footers =
            crate::dataset_files::footers_for_count(&source, &Arc::new(files), &[0], &meter)
                .unwrap();
        assert_eq!(
            footers
                .iter()
                .flatten()
                .flat_map(|f| f.row_group_rows.iter())
                .sum::<usize>(),
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
                file_bytes: 10,
                // A local read keeps each column's width; it comes back by name.
                column_bytes: vec![("id".into(), 1_200), ("note".into(), 900)],
            }),
            // A second file with the same shape: the schema table must hold it once.
            Some(FileFooter {
                schema: schema_of(&[("id", DataType::Int64), ("note", DataType::String)]),
                row_group_rows: vec![7],
                row_group_bytes: vec![512],
                file_bytes: 20,
                column_bytes: Vec::new(),
            }),
            // One that drifted, and one that would not read at all.
            Some(FileFooter {
                schema: schema_of(&[("id", DataType::Int64), ("extra", DataType::Boolean)]),
                row_group_rows: vec![3],
                row_group_bytes: vec![128],
                file_bytes: 30,
                column_bytes: Vec::new(),
            }),
            None,
        ];

        let (cached, schemas) = crate::schema_union::footers_to_cache(&original);
        let sizes = [10, 20, 30, 0];
        assert_eq!(
            schemas.len(),
            2,
            "two distinct shapes among four files, not four copies of them"
        );
        assert_eq!(cached[3].schema, None, "and the unreadable one says so");

        let back = crate::schema_union::footers_from_cache(&cached, &schemas, &sizes)
            .expect("the table is consistent");
        assert_eq!(back.len(), original.len());
        for (before, after) in original.iter().zip(&back) {
            match (before, after) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    assert_eq!(a.schema, b.schema, "same columns, same types");
                    assert_eq!(a.row_group_rows, b.row_group_rows);
                    assert_eq!(a.row_group_bytes, b.row_group_bytes);
                    assert_eq!(a.file_bytes, b.file_bytes, "the size, from the listing");
                    assert_eq!(a.column_bytes, b.column_bytes);
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
            column_bytes: Vec::new(),
        }];
        assert!(
            crate::schema_union::footers_from_cache(&broken, &schemas, &[1]).is_none(),
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
            writes: Default::default(),
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
            cache.dataset_shapes_kept() == 0,
            "and keeps nothing: a two-footer view of {count} files is not this dataset, \
             and kept as one it would open next time with two files' worth of schema"
        );

        // The pass behind it reads every footer, and that is the one worth keeping.
        let pending = state
            .and_then(|(_, facts)| facts.footers_pending)
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
                    writes: Default::default(),
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
            cache.dataset_shapes_kept() == 0,
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
                    writes: Default::default(),
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
                writes: Default::default(),
            },
        );

        assert_eq!(
            meter.listing().and_then(|c| c.files),
            Some(2),
            "the listing found the two files the glob names — not the sibling directory \
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
            "the two the glob names — not the sibling directory it does not, and not the \
             one outside the prefix altogether"
        );

        // `literal_separator` is what keeps a single star inside one path segment.
        assert!(
            !matcher.is_match("data/year=2024/deeper/a.parquet"),
            "a single star does not cross a slash"
        );
    }

    /// A directory is the same table whether it is read from a disk or a bucket.
    ///
    /// The two listings used to disagree in one direction: the local walk checked the
    /// extension, so it missed the `occurrence.parquet/part-00001` shape that Spark and
    /// GBIF write, where the part files have no extension and only the directory name
    /// says what they are. Both now ask `is_parquet_key`.
    ///
    /// The `_`-prefixed row of the fixture is not what this is testing — the local walk
    /// classified those as the writer's own bookkeeping before this change too. It is
    /// here because the two routes reaching the same answer by different means is the
    /// thing worth pinning, not just the one case that moved.
    #[test]
    fn a_directory_is_the_same_table_from_a_disk_or_a_bucket() {
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
        let (local, _skipped) = crate::dataset_files::LocalFiles::new(dir.path()).walk(None);
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

    /// Partition columns and their types are the same whether a tree is opened from a
    /// disk or a bucket: both derive them from the listing (#710). The middle `k=2x`
    /// and the `m` only the newest file has are what a walk of the first directory
    /// would have got differently.
    #[test]
    fn partitions_are_the_same_from_a_disk_or_a_bucket() {
        use object_store::PutPayload;
        use polars::prelude::{DataType, ParquetWriter, df};

        let body = || {
            let mut frame = df!("n" => &[1i64]).unwrap();
            let mut out = Vec::new();
            ParquetWriter::new(&mut out).finish(&mut frame).unwrap();
            out
        };
        let layout = [
            "k=1/a.parquet",
            "k=2x/b.parquet",
            "k=3/m=2024-01-01/c.parquet",
        ];
        let partitions = |state: &crate::widgets::datatable::DataTableState| {
            let columns = state.partition_columns().unwrap_or_default().to_vec();
            columns
                .iter()
                .map(|c| (c.clone(), state.schema().get(c).cloned()))
                .collect::<Vec<_>>()
        };

        let dir = tempfile::tempdir().unwrap();
        for rel in layout {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, body()).unwrap();
        }
        let report = || crate::measurements::OpenReport {
            progress: Arc::new(crate::schema_union::FooterProgress::default()),
            meter: Arc::new(crate::measurements::Meter::default()),
            remembered: None,
            writes: Default::default(),
        };
        let (local, _) = crate::App::schema_state_from_local_hive(
            Some(dir.path()),
            &crate::OpenOptions {
                hive: true,
                ..crate::OpenOptions::default()
            },
            &report(),
        )
        .expect("the directory opens");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            for rel in layout {
                store
                    .put(
                        &OsPath::from(format!("data/{rel}")),
                        PutPayload::from(body()),
                    )
                    .await
                    .unwrap();
            }
        });
        let (cloud, _) = crate::App::schema_state_from_cloud_hive_with(
            "memory://data/".to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &report(),
        )
        .expect("the prefix opens");

        let from_disk = partitions(&local);
        assert_eq!(
            from_disk,
            [
                ("k".to_string(), Some(DataType::Int64)),
                ("m".to_string(), Some(DataType::Date)),
            ],
            "the newest file names the columns and the two ends type them"
        );
        assert_eq!(partitions(&cloud), from_disk, "and the bucket agrees");
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
                writes: Default::default(),
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
                writes: Default::default(),
            },
        )
        // As `build_schema_state` marks a prefix that is scanned where it lies.
        .map(|(state, facts)| {
            state.with_open(crate::widgets::datatable::OpenFacts {
                remote_source: true,
                ..facts
            })
        })
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
        // As a rendered table has a height.
        state.visible_rows = 10;
        assert!(
            state.join_dataset_schema(found).is_ok(),
            "nothing is built on top of the scan here, so they go straight in"
        );

        assert_eq!(
            progress.last_pass().read,
            files - 2,
            "the pass behind the open read every footer but the two the open did"
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
                writes: Default::default(),
            },
        )
        .map(|(state, facts)| state.with_open(facts))
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
        let total = groups.iter().flatten().sum();
        assert!(state.count_landed(state.len_generation(), total, Some(&groups)));
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
                writes: Default::default(),
            },
        )
        .map(|(state, facts)| state.with_open(facts))
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

            let footers = footers_of_files(
                &store,
                &files,
                &[0, 1],
                &Arc::new(crate::measurements::Meter::default()),
            )
            .await;
            let groups: Vec<Vec<usize>> = footers
                .into_iter()
                .map(|f| f.map(|f| f.row_group_rows).unwrap_or_default())
                .collect();
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

    /// The count reads only the footers the open did not, and once it has them all the
    /// dataset is remembered, so a reopen reads no footers.
    #[test]
    fn the_count_reads_only_what_the_open_did_not_and_remembers_the_dataset() {
        use object_store::PutPayload;
        use polars::prelude::{ParquetWriter, df};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let files = rt.block_on(async {
            for day in 1..=5i64 {
                let mut frame = df!("id" => (0..day).collect::<Vec<i64>>()).unwrap();
                let mut bytes = Vec::new();
                ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
                store
                    .put(
                        &OsPath::from(format!("data/date=2024-01-0{day}/a.parquet")),
                        PutPayload::from(bytes),
                    )
                    .await
                    .unwrap();
            }
            list_dataset_files(&store, "data/", None).await.unwrap().0
        });
        let files = Arc::new(files);
        let full = "memory://data/";
        let fingerprint = crate::cache::DatasetShape::fingerprint_of(
            files
                .iter()
                .map(|f| (f.key.as_str(), f.size, f.stamp, f.etag.as_deref())),
        );
        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(dir.path().to_path_buf());
        let meter = Arc::new(crate::measurements::Meter::default());
        meter.listed(std::time::Duration::from_millis(1), Some(5), false);

        // As a sampled open leaves it: two footers read. The first is planted with a
        // count it does not have, so a count that read it again would say so.
        let sampled = rt.block_on(footers_of_files(
            &store,
            &files,
            &[0, 4],
            &Arc::new(crate::measurements::Meter::default()),
        ));
        let mut planted = sampled[0].clone().unwrap();
        planted.row_group_rows = vec![999];
        let source = Arc::new(crate::dataset_files::StoreFiles::new(
            full,
            "data/".to_string(),
            None,
            store.clone(),
            polars::prelude::cloud::CloudOptions::default(),
            rt.handle(),
        ));
        let count = crate::dataset_files::counter_for(
            source,
            files.to_vec(),
            [(0, Some(planted)), (4, sampled[1].clone())],
            Some(fingerprint.clone()),
            meter.clone(),
            Some(cache.clone()),
        );
        let groups = count().unwrap();
        assert_eq!(groups, [vec![999], vec![2], vec![3], vec![4], vec![5]]);
        assert_eq!(
            meter.footers().and_then(|c| c.files),
            Some(3),
            "the count read the three footers the open had not"
        );
        assert_eq!(
            meter
                .footers()
                .and_then(|c| c.over_the_wire)
                .map(|w| w.requests),
            Some(3),
            "one request each, and none for the two it was given"
        );
        let shape = cache
            .dataset_shape(full, &fingerprint)
            .expect("every footer is in, so the dataset is remembered");
        assert_eq!(shape.files.len(), 5);

        // A reopen finds it, and reads no footers.
        let progress = Arc::new(crate::schema_union::FooterProgress::default());
        let reopen = Arc::new(crate::measurements::Meter::default());
        let (state, facts) = crate::App::schema_state_from_cloud_hive_with(
            full.to_string(),
            "data/".to_string(),
            store,
            polars::prelude::cloud::CloudOptions::default(),
            &crate::OpenOptions::default(),
            rt.handle(),
            &crate::measurements::OpenReport {
                progress: progress.clone(),
                meter: reopen.clone(),
                remembered: Some(cache),
                writes: Default::default(),
            },
        )
        .expect("the reopen opens");
        assert_eq!(progress.last_pass().begun, 0, "no footer pass");
        assert!(reopen.footers().is_none(), "and no footer read");
        assert!(facts.footers_pending.is_none(), "nor one behind the open");
        let state = state.with_open(facts);
        assert_eq!(state.num_rows_if_valid(), Some(999 + 2 + 3 + 4 + 5));
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
                writes: Default::default(),
            },
        )
        .map(|(state, facts)| state.with_open(facts))
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
        let total = groups.iter().flatten().sum();
        assert!(state.count_landed(state.len_generation(), total, Some(&groups)));
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
    /// so the test is where a file is: a directory with no Parquet in it is nobody's
    /// table. The same rule silences the zero-byte folder markers a console leaves,
    /// one per partition, which would otherwise read as hundreds of stopped writes.
    #[test]
    fn a_directory_with_no_data_in_it_is_nobodys_table() {
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
                // directory above it holds data either. Nothing readable is left anywhere
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
                writes: Default::default(),
            },
        )
        .map(|(state, facts)| state.with_open(facts))
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
    fn a_footer_from_a_tail_invalid_returns_err() {
        let invalid = vec![0u8; 100];
        let r = FileFooter::from_tail(&invalid, invalid.len(), true);
        assert!(r.is_err());
    }

    #[test]
    fn a_footer_from_a_tail_reads_schema_and_row_count() {
        use polars::prelude::{ParquetWriter, df};
        let mut df = df!("a" => &[1i32, 2, 3], "b" => &["x", "y", "z"]).unwrap();
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes).finish(&mut df).unwrap();
        let footer = FileFooter::from_tail(&bytes, bytes.len(), true).unwrap();
        assert_eq!(footer.row_group_rows, [3]);
        assert_eq!(footer.schema.len(), 2);
    }

    #[test]
    fn a_footer_from_a_tail_reads_row_groups_and_column_widths() {
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
        let footer = FileFooter::from_tail(&bytes, bytes.len(), true).unwrap();
        assert!(
            footer.row_group_rows.len() > 1,
            "{:?}",
            footer.row_group_rows
        );
        assert_eq!(footer.row_group_rows.iter().sum::<usize>(), 1000);
        let widths = crate::schema_union::column_bytes_per_row(&[Some(footer.clone())]);
        let width = |column: &str| {
            widths
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
