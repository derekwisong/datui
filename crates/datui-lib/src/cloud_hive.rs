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

/// Every Parquet file under `prefix`, sorted by key, which is the order a scan of the
/// prefix reads them in. One listing, however deep the partitions go. Job files,
/// hidden files and empty objects are left out: none of them is data, and a scan that
/// tried to read one would fail. With a `pattern`, only the keys it matches.
///
/// This is how datui opens a glob: it lists the literal prefix and does the matching
/// itself, so a glob becomes an ordinary list of files and gets everything a prefix
/// gets — the schema union over every footer, the row count, the notes and the
/// measurements. Handing the star to the object store instead matches nothing, because
/// a listing prefix is a literal string and `*` is a character like any other.
///
/// Each object counts off against `listed` as it is listed, and the listing stops once
/// `cancelled` is set. A prefix of a few hundred thousand objects is hundreds of pages,
/// and this count is all the loading screen has to say about them.
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

/// The first read of a footer. Most footers fit; a larger one costs a second request.
const COUNT_TAIL_BYTES: u64 = 16 * 1024;

/// Every file's footer, in file order: a small ranged read at the end of each file,
/// many at once. No data is read. A file whose footer cannot be read is `None` rather
/// than an error, so one object mid-write does not stop the dataset from opening.
/// Each footer counts off against `progress` as it lands.
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
    let permits = Arc::new(tokio::sync::Semaphore::new(progress.reads_at_once()));
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
mod tests;
