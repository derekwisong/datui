//! A dataset of many Parquet files, wherever they are.
//!
//! [`DatasetFiles`] lists a dataset's files, stats them and reads their footers; a local
//! directory ([`LocalFiles`]) and a prefix in an object store ([`StoreFiles`]) each
//! implement it. Everything built on those three steps exists once, here: the open
//! from the two ends past one wave of footers, the pass that reads the rest behind it,
//! the count that reads only what neither read, and the shape and facts kept for the
//! next open and the home screen.
//!
//! The two differ only where the medium does: a store lists in key ranges at once and
//! carries each object's size and tag in its listing; a directory is walked a level at
//! a time by the type each entry already gives, stat'ed only when the dataset is worth
//! remembering, and its footers keep each column's width.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::io::cloud::CloudOptions;
use polars::prelude::{LazyFrame, PlSmallStr};

use crate::measurements::{Meter, OpenReport};
use crate::schema_union::{
    FOOTERS_AT_ONCE, FileFooter, FooterProgress, Listing, SkippedFiles, ends_of, footers_to_read,
};
use crate::widgets::datatable::{
    DataTableState, DatasetAtOpen, FileCounter, FileScan, FootersFound, OpenFacts, RemoteRead,
};

/// One data file of a dataset: where it is, its size and when it was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFile {
    /// Its key in the store, or its path.
    pub key: String,
    /// Its size, where the listing or a stat said; `0` until then.
    pub size: u64,
    /// When it was last written, where the listing or a stat said. Part of the
    /// fingerprint that decides whether what datui remembers about the dataset still
    /// describes it.
    pub stamp: u64,
    /// The store's own tag for this version of the object, where it gave one.
    ///
    /// The strongest part of that fingerprint, and free — it comes back in the same
    /// listing response as the size. A size and a whole-second timestamp cannot see a
    /// file overwritten within the same second at the same length; an ETag can.
    pub etag: Option<String>,
}

/// Where a dataset's files are, and how to list, stat and read them.
pub trait DatasetFiles: Send + Sync {
    /// What the shape cache and the dataset index file the dataset under.
    fn key(&self) -> &str;
    /// What every file's name is measured against for the layout notes.
    fn root(&self) -> &str;
    /// Every data file, in the order a scan reads them, and what the listing passed
    /// over, counted off against `listing` as found. `None` when it could not list.
    fn list(&self, listing: &Listing<'_>) -> Option<(Vec<DatasetFile>, SkippedFiles)>;
    /// Whether a listing of `files` files is worth a fingerprint, and the stat it costs.
    fn fingerprints(&self, files: usize) -> bool;
    /// Fill in each file's size and stamp where the listing did not. `false` when a file
    /// went between the listing and its stat, or the open was abandoned.
    fn stat(&self, files: &mut [DatasetFile], progress: &FooterProgress) -> bool;
    /// The footers of `files` at `read`, in that order, counted off against `progress`
    /// and timed into `meter`. A footer that will not read is `None`: one file mid-write
    /// must not stop the dataset from opening. `None` when the reads were abandoned.
    fn read_footers(
        &self,
        files: &Arc<Vec<DatasetFile>>,
        read: &[usize],
        progress: &Arc<FooterProgress>,
        meter: &Arc<Meter>,
    ) -> Option<Vec<Option<FileFooter>>>;
    /// What the scan reads `file` by: its path or its URL.
    fn name_of(&self, file: &DatasetFile) -> Option<String>;
    /// The `/`-separated path its partition keys are read from.
    fn partition_path(&self, file: &DatasetFile) -> String;
    /// Polars' options for reading the files where they are, for files in a store.
    fn cloud_options(&self) -> Option<CloudOptions>;
    /// What a local copy of the whole dataset would fetch: nothing, for files here.
    fn remote_objects(&self, _files: &[DatasetFile]) -> Vec<crate::local_copy::RemoteObject> {
        Vec::new()
    }
    /// The modification time the dataset index records for the dataset.
    fn modified(&self, files: &[DatasetFile]) -> u64;
    /// Whether the listing is the one object the path names rather than a directory.
    fn is_one_object(&self, _files: &[DatasetFile]) -> bool {
        false
    }
}

/// A dataset as its listing found it: what every set of its footers is read against.
struct Listed {
    source: Arc<dyn DatasetFiles>,
    /// Every file, in scan order, as listed.
    files: Arc<Vec<DatasetFile>>,
    /// What the scan names each file by, in the same order.
    names: Vec<String>,
    partition_columns: Vec<String>,
    /// The first and newest files' partition values, which type the partition columns.
    values: Vec<(String, String)>,
    skipped: SkippedFiles,
    /// What the listing says the dataset is now, where it was worth taking: what its
    /// shape is kept against.
    fingerprint: Option<String>,
    /// The open's meter, which the pass's and the count's reads are tallied into.
    meter: Arc<Meter>,
    remembered: Option<crate::cache::CacheManager>,
}

/// A dataset as some set of its footers describes it, and the scan that reads it.
struct Opened {
    dataset: crate::schema_union::DatasetSchema,
    lf: LazyFrame,
    /// Each file's rows, or empty when they are not all known.
    file_rows: Vec<usize>,
    /// Each readable file's row groups, or empty unless every footer was read: the
    /// count, without a pass of its own.
    row_groups: Vec<Vec<usize>>,
    /// The readable files, a scan of any of them, and their count: once every footer
    /// is known a page reads only the files holding its rows (#659).
    by_file: RemoteRead,
}

/// Open the dataset `source` lists, from its footers. `None` when it lists nothing
/// readable, which sends the caller to the general scan.
///
/// Past one wave of files the two ends open the dataset and the rest are read behind
/// it, joining when they land; up to a wave they cost one round of reads either way,
/// so the dataset opens whole. A dataset whose listing has not changed since its
/// footers were last all read opens from what they said then, reading none.
pub(crate) fn open(
    source: Arc<dyn DatasetFiles>,
    options: &crate::OpenOptions,
    report: &OpenReport,
) -> Option<(DataTableState, OpenFacts)> {
    let (files, skipped, fingerprint) = list(&*source, &report.progress, &report.meter)?;
    if report.progress.is_cancelled() || files.is_empty() {
        return None;
    }
    let listed = Arc::new(Listed::new(
        source,
        files,
        skipped,
        fingerprint,
        report.meter.clone(),
        report.remembered.clone(),
    )?);
    let files = listed.files.clone();
    let remembered = listed.shape();
    let from_cache = remembered.is_some();
    let staged = !from_cache && files.len() > FOOTERS_AT_ONCE;
    let read = if from_cache {
        // Every file, because the cache holds every file: a remembered dataset opens
        // whole, with its rows numbered and its notes complete, however large it is.
        (0..files.len()).collect()
    } else if staged {
        ends_of(files.len())
    } else {
        footers_to_read(files.len())
    };
    let footers = match remembered {
        Some(footers) => footers,
        None => listed
            .source
            .read_footers(&files, &read, &report.progress, &report.meter)?,
    };
    // A shape just found needs no storing again: the lookup has dated it.
    listed.remember(&read, &footers, !from_cache);
    log::debug!(
        target: "datui",
        "dataset of {} files: {} footers {}",
        files.len(),
        read.len(),
        if from_cache {
            "from the shape cache"
        } else if staged {
            "read, the rest behind"
        } else {
            "read"
        }
    );
    let opened = listed.dataset(&read, &footers)?;
    let state = DataTableState::from_schema_and_lazyframe(
        opened.dataset.schema.clone(),
        opened.lf,
        options,
        Some(listed.partition_columns.clone()),
    )
    .ok()?;
    let mut facts = OpenFacts {
        remote_files: Some(opened.by_file.into()),
        // The listing's sizes: what a full scan's local copy would fetch, known before
        // it fetches anything.
        remote_objects: listed.source.remote_objects(&files),
        // A local footer says how wide each column is: a binary column's width is
        // known nowhere else.
        column_bytes: crate::schema_union::column_bytes_per_row(&footers),
        // The count is in the footers just read when every one was, so no pass reads
        // them again for it.
        row_groups: opened.row_groups,
        dataset: Some(DatasetAtOpen {
            schema: opened.dataset,
            file_rows: opened.file_rows,
            files: listed.names.clone(),
        }),
        ..Default::default()
    };
    if staged {
        let ends = read;
        facts.footers_pending = Some(Arc::new(move |progress: &Arc<FooterProgress>| {
            let read = footers_to_read(files.len());
            // The ends were read by the open; a footer is read once.
            let rest: Vec<usize> = read.iter().copied().filter(|i| !ends.contains(i)).collect();
            let mut fresh = listed
                .source
                .read_footers(&files, &rest, progress, &listed.meter)?
                .into_iter();
            if progress.is_cancelled() {
                return None;
            }
            let footers: Vec<Option<FileFooter>> = read
                .iter()
                .map(|i| match ends.iter().position(|e| e == i) {
                    Some(at) => footers[at].clone(),
                    None => fresh.next().flatten(),
                })
                .collect();
            // This is the pass that reads a large dataset's footers, so this is where
            // a large dataset gets remembered.
            listed.remember(&read, &footers, true);
            // Past `MAX_FOOTER_READS` this read a sample, and the dataset has no row
            // groups until its count reads the rest — only the rest.
            let whole = listed.dataset(&read, &footers)?;
            Some(FootersFound {
                dataset: whole.dataset,
                lf: whole.lf,
                file_rows: whole.file_rows,
                // Every file listed: the dataset's per-file findings index this.
                files: listed.names.clone(),
                row_groups: whole.row_groups,
                remote: Some(whole.by_file),
            })
        }));
    }
    Some((state, facts))
}

/// Every data file `source` lists, what the listing passed over, and the listing's
/// fingerprint where it is worth taking. Timed into `meter` as the listing.
fn list(
    source: &dyn DatasetFiles,
    progress: &FooterProgress,
    meter: &Meter,
) -> Option<(Vec<DatasetFile>, SkippedFiles, Option<String>)> {
    // The listing and the stat behind it are one wait, counted on the loading screen,
    // and an abandoned open stops both.
    let listing = progress.listing();
    let began = std::time::Instant::now();
    let (mut files, skipped) = source.list(&listing)?;
    // After the sort: the files are not found until they are in the order the scan
    // will read them in.
    meter.listed(began.elapsed(), Some(files.len()), false);
    // Taking it costs nothing in a store, whose listing carries every file's size and
    // tag; on a disk it costs a stat a file, which within a wave is not worth it.
    let fingerprint = (source.fingerprints(files.len()) && source.stat(&mut files, progress))
        .then(|| fingerprint_of(&files));
    Some((files, skipped, fingerprint))
}

/// The fingerprint of a listing: see [`crate::cache::DatasetShape::fingerprint_of`].
fn fingerprint_of(files: &[DatasetFile]) -> String {
    crate::cache::DatasetShape::fingerprint_of(
        files
            .iter()
            .map(|f| (f.key.as_str(), f.size, f.stamp, f.etag.as_deref())),
    )
}

impl Listed {
    /// `None` when a file has no name the scan can read it by.
    fn new(
        source: Arc<dyn DatasetFiles>,
        files: Vec<DatasetFile>,
        skipped: SkippedFiles,
        fingerprint: Option<String>,
        meter: Arc<Meter>,
        remembered: Option<crate::cache::CacheManager>,
    ) -> Option<Self> {
        let names = files
            .iter()
            .map(|f| source.name_of(f))
            .collect::<Option<Vec<_>>>()?;
        let (first, newest) = (files.first()?, files.last()?);
        // From the listing: the newest file names the columns and the two ends type
        // them, so a tree is the same table from a disk or a bucket, and no directory
        // is read twice to find them.
        let (partition_columns, values) = crate::schema_union::partitions_of_listing(
            &source.partition_path(first),
            &source.partition_path(newest),
        );
        Some(Self {
            source,
            files: Arc::new(files),
            names,
            partition_columns,
            values,
            skipped,
            fingerprint,
            meter,
            remembered,
        })
    }

    /// Every footer as a previous pass left them, if the listing has not changed.
    fn shape(&self) -> Option<Vec<Option<FileFooter>>> {
        let shape = self
            .remembered
            .as_ref()?
            .dataset_shape(self.source.key(), self.fingerprint.as_ref()?)?;
        let sizes: Vec<u64> = self.files.iter().map(|f| f.size).collect();
        // A damaged entry can carry the right header and the wrong count; refused
        // rather than indexed past the listing.
        crate::schema_union::footers_from_cache(&shape.files, &shape.schemas, &sizes)
    }

    /// Keep what a pass learned: what the home screen shows for the dataset, and, with
    /// `shape`, every footer — if every one was read and parsed.
    ///
    /// Every footer must have been read. A staged open has read two of them and a
    /// sampled one a spread, and either kept as though it were the whole dataset would
    /// hand the next open a smaller dataset than it asked for, with nothing to say that
    /// is what happened.
    ///
    /// Every footer must have *parsed*. A footer read can fail because the file is
    /// corrupt, and it can fail because the store throttled the request or a token
    /// expired — and nothing here can tell those apart. Remembering the failure turns a
    /// moment's trouble into a file that is missing from the dataset on every open from
    /// now until something else changes. Read them again next time; the one that was
    /// really corrupt costs a read and says the same thing.
    fn remember(&self, read: &[usize], footers: &[Option<FileFooter>], shape: bool) {
        let Some(cache) = self.remembered.as_ref() else {
            return;
        };
        // What the home screen reads. Written before the shape, because a sampled read
        // still says what the columns are, and the shape below wants every footer. A
        // sampled read does not replace a whole one, though: the shape cache is the
        // smaller of the two and forgets a dataset long before the index does.
        let path = PathBuf::from(self.source.key());
        if let Some(facts) = self.facts(read, footers)
            && facts_worth_recording(cache.dataset_facts(&path).as_ref(), &facts)
        {
            cache.record_dataset_facts(&[(path, facts)]);
        }
        let Some(fingerprint) = self.fingerprint.as_ref().filter(|_| shape) else {
            return;
        };
        if read.len() != self.files.len() || !footers.iter().all(Option::is_some) {
            return;
        }
        let (cached, schemas) = crate::schema_union::footers_to_cache(footers);
        cache.save_dataset_shape(
            self.source.key(),
            crate::cache::DatasetShape {
                fingerprint: fingerprint.clone(),
                files: cached,
                schemas,
                taken_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default(),
            },
        );
    }

    /// Every column the footers at `read` give, typed with the partition columns ahead.
    /// `None` when none of them could be read.
    fn schema(
        &self,
        read: &[usize],
        footers: &[Option<FileFooter>],
    ) -> Option<crate::schema_union::DatasetSchema> {
        let mut dataset = crate::schema_union::union_sampled(self.files.len(), read, footers);
        if dataset.schema.is_empty() {
            return None;
        }
        dataset.schema = Arc::new(crate::schema_union::with_partition_columns(
            &dataset.schema,
            &self.partition_columns,
            &self.values,
        ));
        Some(dataset)
    }

    /// What the home screen can say about the dataset from the footers at `read`: its
    /// columns, its rows when every footer was read, its kind and what it holds.
    fn facts(
        &self,
        read: &[usize],
        footers: &[Option<FileFooter>],
    ) -> Option<crate::cache::DatasetFacts> {
        use crate::discover::{CLASSIFIER_VERSION, EntryKind, Holds};
        let dataset = self.schema(read, footers)?;
        let columns: Vec<String> = dataset
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        let files = &self.files;
        let every_footer = read.len() == files.len() && footers.iter().all(Option::is_some);
        let rows = every_footer.then(|| footers.iter().flatten().map(FileFooter::rows).sum());
        let directory = !self.source.is_one_object(files);
        let kind = if !directory {
            EntryKind::File
        } else if !self.partition_columns.is_empty() {
            EntryKind::Hive
        } else {
            EntryKind::MultiFile
        };
        let holds = if directory {
            Holds {
                formats: vec![("parquet".to_string(), files.len())],
                ..Default::default()
            }
        } else {
            Default::default()
        };
        // From the listing where it said, else from the footers, which a read off a
        // disk takes from the file it opened.
        let listed: u64 = files.iter().map(|f| f.size).sum();
        let size = if files.iter().all(|f| f.size > 0) || !every_footer {
            listed
        } else {
            footers.iter().flatten().map(|f| f.file_bytes as u64).sum()
        };
        Some(crate::cache::DatasetFacts {
            mtime: self.source.modified(files),
            size,
            rows,
            cols: Some(columns.len()),
            cols_sampled: !every_footer,
            columns,
            kind: Some(kind),
            classified_by: CLASSIFIER_VERSION,
            cost: Default::default(),
            holds,
        })
    }

    /// What the footers at `read` say about the dataset. Shared by the open, which may
    /// have read only the two ends, and the pass that reads the rest: the two differ
    /// only in how much they know. `None` when nothing could be read.
    fn dataset(self: &Arc<Self>, read: &[usize], footers: &[Option<FileFooter>]) -> Option<Opened> {
        let files = &self.files;
        let dataset = self.schema(read, footers)?;
        let every_footer = read.len() == files.len();
        // Numbering rows needs every file's row count; a sampled dataset has not read
        // them all, so it forgoes the distinction rather than guessing at it.
        let file_rows: Vec<usize> = if every_footer {
            footers
                .iter()
                .map(|f| f.as_ref().map(FileFooter::rows))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        // Over the readable files only, as the scan is: a file mid-write is in neither.
        // A footer sampled past or failed would count as no rows, which both
        // undercounts and puts its rows out of reach of a windowed scan; the count
        // reads it instead.
        let row_groups: Vec<Vec<usize>> = if every_footer && footers.iter().all(Option::is_some) {
            footers
                .iter()
                .flatten()
                .map(|f| f.row_group_rows.clone())
                .collect()
        } else {
            Vec::new()
        };
        // A file that stores a column in a type the dataset's column cannot hold is not
        // read for it; its rows are null there rather than failing the scan, and carry
        // their file's drift group so the null can be told from a real one.
        let drift = crate::schema_union::ScanDrift::new(&self.names, &dataset, &file_rows);
        // The files that will open. One whose footer would not read is one Polars
        // cannot read either, and left in the scan it takes the dataset down on the
        // first page. A staged open can only leave out what it has read; the pass
        // behind it finds the rest and the join swaps in a scan without them.
        let readable = crate::schema_union::readable_paths(&self.names, &dataset.unreadable);
        if readable.is_empty() {
            return None;
        }
        let scan: FileScan = {
            let (schema, partition_columns, drift, cloud) = (
                dataset.schema.clone(),
                self.partition_columns.clone(),
                drift.map(Arc::new),
                self.source.cloud_options(),
            );
            Arc::new(move |files: &[String], as_text: &[PlSmallStr]| {
                let lf = crate::schema_union::lenient_scan(
                    files,
                    schema.clone(),
                    cloud.clone(),
                    drift.as_deref(),
                    as_text,
                )?;
                Ok(crate::hoist_partition_columns(
                    lf,
                    &schema,
                    &partition_columns,
                    drift.is_some(),
                ))
            })
        };
        let lf = scan(&readable, &[]).ok()?;
        let count: FileCounter = if row_groups.is_empty() {
            // Over the same files as the scan: the counter answers one entry per file,
            // which has to be the list beside it, or the answer is dropped on a length
            // check and the dataset never learns its own size. Searched rather than
            // scanned: a dataset can be hundreds of thousands of files.
            let counted: Vec<usize> = (0..files.len())
                .filter(|index| dataset.unreadable.binary_search(index).is_err())
                .collect();
            if counted.len() != readable.len() {
                return None;
            }
            self.counter(crate::schema_union::FooterCount::new(
                files.len(),
                counted,
                read.iter().copied().zip(footers.iter().cloned()),
            ))
        } else {
            // The rows are in the footers, so the counter answers without reading.
            let counted = row_groups.clone();
            Arc::new(move || Ok(counted.clone()))
        };
        let by_file = RemoteRead {
            urls: readable.into_owned(),
            scan,
            count,
        };
        let dataset = dataset
            .with_partition_layouts(self.source.root(), &self.names)
            .with_skipped(self.skipped);
        Some(Opened {
            dataset,
            lf,
            file_rows,
            row_groups,
            by_file,
        })
    }

    /// The counter for a dataset whose open did not read every footer: it reads the
    /// rest, once, and keeps the shape when they are all in, so a reopen reads none.
    fn counter(
        self: &Arc<Self>,
        count: crate::schema_union::FooterCount<FileFooter>,
    ) -> FileCounter {
        let (listed, count) = (self.clone(), Arc::new(count));
        Arc::new(move || {
            let counted = count
                .count(
                    |missing| {
                        footers_for_count(&*listed.source, &listed.files, missing, &listed.meter)
                    },
                    |footer| footer.row_group_rows.clone(),
                )
                .ok_or_else(|| "cancelled".to_string())?;
            if let Some(whole) = counted.whole.as_deref() {
                let every: Vec<usize> = (0..listed.files.len()).collect();
                listed.remember(&every, whole, true);
            }
            Ok(counted.row_groups)
        })
    }
}

/// The footers of `files` at `read`, as the pass that settles the row count: timed into
/// `meter` as that pass, once, and only when something parsed.
///
/// Counted against a meter of its own first, so that a pass the one-shot declines adds
/// nothing to the dataset's figures: this runs again every time the count is
/// invalidated, and a dataset explored for a few minutes would otherwise report an
/// open that kept getting more expensive. Not recorded when nothing parsed: a pass
/// that settled nothing must not take the one measurement this gets.
pub(crate) fn footers_for_count(
    source: &dyn DatasetFiles,
    files: &Arc<Vec<DatasetFile>>,
    read: &[usize],
    meter: &Meter,
) -> Option<Vec<Option<FileFooter>>> {
    let counting = Arc::new(Meter::default());
    let began = std::time::Instant::now();
    let footers =
        source.read_footers(files, read, &Arc::new(FooterProgress::default()), &counting)?;
    if footers.iter().any(Option::is_some) {
        let wire = counting.footers().and_then(|c| c.over_the_wire);
        meter.counted_rows(began.elapsed(), Some(read.len()), wire);
    }
    Some(footers)
}

/// The counter an open leaves a dataset of `files` with, given the footers `known`.
#[cfg(all(test, feature = "cloud"))]
pub(crate) fn counter_for(
    source: Arc<dyn DatasetFiles>,
    files: Vec<DatasetFile>,
    known: impl IntoIterator<Item = (usize, Option<FileFooter>)>,
    fingerprint: Option<String>,
    meter: Arc<Meter>,
    remembered: Option<crate::cache::CacheManager>,
) -> FileCounter {
    let listed = Arc::new(
        Listed::new(
            source,
            files,
            SkippedFiles::default(),
            fingerprint,
            meter,
            remembered,
        )
        .expect("a listing"),
    );
    let files = listed.files.len();
    listed.counter(crate::schema_union::FooterCount::new(
        files,
        (0..files).collect(),
        known,
    ))
}

/// What an open would record for the home screen from the footers at `read`.
#[cfg(all(test, feature = "cloud"))]
pub(crate) fn facts_of(
    source: Arc<dyn DatasetFiles>,
    files: Vec<DatasetFile>,
    read: &[usize],
    footers: &[Option<FileFooter>],
) -> Option<(PathBuf, crate::cache::DatasetFacts)> {
    let listed = Listed::new(
        source,
        files,
        SkippedFiles::default(),
        None,
        Arc::new(Meter::default()),
        None,
    )?;
    let facts = listed.facts(read, footers)?;
    Some((PathBuf::from(listed.source.key()), facts))
}

/// Every footer of the local directory `dir` as the last open that read them all left
/// them, if its files are as they were then. Only a directory an open remembered is
/// listed whole for this; any other costs one stat.
pub(crate) fn remembered_footers(
    dir: &Path,
    cache: &crate::cache::CacheManager,
) -> Option<(Vec<DatasetFile>, Vec<Option<FileFooter>>)> {
    let local = LocalFiles::new(dir);
    if !cache.has_dataset_shape(local.key()) {
        return None;
    }
    let (files, _, fingerprint) = list(&local, &FooterProgress::default(), &Meter::default())?;
    let shape = cache.dataset_shape(local.key(), &fingerprint?)?;
    let sizes: Vec<u64> = files.iter().map(|f| f.size).collect();
    let footers = crate::schema_union::footers_from_cache(&shape.files, &shape.schemas, &sizes)?;
    Some((files, footers))
}

/// Whether a record learned from an open should replace what the index has: anything
/// replaces nothing, a whole read replaces anything, and a sampled read replaces only
/// another sample.
pub(crate) fn facts_worth_recording(
    existing: Option<&crate::cache::DatasetFacts>,
    new: &crate::cache::DatasetFacts,
) -> bool {
    match existing {
        None => true,
        Some(_) if new.rows.is_some() => true,
        Some(old) => old.rows.is_none(),
    }
}

/// A directory of Parquet files on this machine or a mount.
pub struct LocalFiles {
    dir: PathBuf,
    /// The directory as resolved, so it is one dataset however it was named.
    key: String,
    root: String,
}

impl LocalFiles {
    pub fn new(dir: &Path) -> Self {
        let key = crate::canonical::canonicalize(dir)
            .unwrap_or_else(|_| dir.to_path_buf())
            .to_string_lossy()
            .into_owned();
        Self {
            dir: dir.to_path_buf(),
            key,
            root: dir.to_string_lossy().into_owned(),
        }
    }

    /// Every Parquet file under the directory, sorted, and what the walk passed over,
    /// counted off against `listing` as found.
    ///
    /// The directories are read many at once and classified afterwards, in one pass
    /// that sees the tree as a serial walk would: on a network mount each directory is
    /// a round trip, and a Hive tree is thousands of them. A cancelled listing stops
    /// reading directories and returns what it had, which the caller drops.
    pub fn walk(&self, listing: Option<&Listing<'_>>) -> (Vec<PathBuf>, SkippedFiles) {
        let mut files = Vec::new();
        let mut skipped = SkippedFiles::default();
        let walked = walk_dirs(&self.dir, MAX_DEPTH, listing, None);
        collect_data_files(
            &walked,
            &self.dir,
            &mut files,
            &mut skipped,
            0,
            MAX_DEPTH,
            false,
        );
        // Load-bearing beyond reading in a predictable order. The scan hands these to
        // Polars as they are, and Polars takes the hive schema from the first of them, so
        // this decides whether a directory whose partition keys disagree opens with its
        // partition column null or fails to open at all — see the two `directories_that`
        // integration tests, which are the same directory differing by one file name.
        files.sort();
        (files, skipped)
    }

    /// The first files of the directory, sorted, down `levels` levels: more than
    /// `enough` of them when there are, which is how a caller tells a dataset too large
    /// to measure from one it can. For the home screen, which looks at a dataset
    /// rather than opening it.
    pub fn first_files(&self, levels: usize, enough: usize) -> Vec<PathBuf> {
        let walked = walk_dirs(&self.dir, levels, None, Some(enough));
        let mut files = Vec::new();
        collect_data_files(
            &walked,
            &self.dir,
            &mut files,
            &mut SkippedFiles::default(),
            0,
            levels,
            false,
        );
        files.sort();
        files.truncate(enough + 1);
        files
    }

    /// The directory's row count from every file's footer, for a dataset that opened
    /// without listing its files: a directory read as one scan. `Err` when no footer
    /// under it reads.
    ///
    /// Timed from before the walk: this pass has to find the files again before it
    /// can read them, and what it cost is both halves. Only the first such pass counts
    /// (see `Meter::counted_rows`); it runs again every time a filter is cleared.
    pub fn count_rows(&self, meter: &Meter) -> color_eyre::Result<usize> {
        let began = std::time::Instant::now();
        let (files, _) = self.walk(None);
        if files.is_empty() {
            return Err(color_eyre::eyre::eyre!(
                "No parquet files found under {}",
                self.dir.display()
            ));
        }
        let files = Arc::new(files.into_iter().map(local_file).collect::<Vec<_>>());
        let every: Vec<usize> = (0..files.len()).collect();
        let footers = self
            .read_footers(
                &files,
                &every,
                &Arc::new(FooterProgress::default()),
                &Arc::new(Meter::default()),
            )
            .unwrap_or_default();
        if !footers.iter().any(Option::is_some) {
            // Not recorded, and so not counted against the one shot this measurement
            // gets: a pass where nothing parsed settled nothing.
            return Err(color_eyre::eyre::eyre!(
                "Could not read any parquet footers under {}",
                self.dir.display()
            ));
        }
        meter.counted_rows(began.elapsed(), Some(files.len()), None);
        Ok(footers.iter().flatten().map(FileFooter::rows).sum())
    }
}

/// How deep a local walk goes. Past this the files belong to something else, and a
/// link loop ends.
const MAX_DEPTH: usize = 64;

/// A file found by a walk, before anything is known of it but its name.
fn local_file(path: PathBuf) -> DatasetFile {
    DatasetFile {
        key: path.to_string_lossy().into_owned(),
        size: 0,
        stamp: 0,
        etag: None,
    }
}

impl DatasetFiles for LocalFiles {
    fn key(&self) -> &str {
        &self.key
    }

    fn root(&self) -> &str {
        &self.root
    }

    fn list(&self, listing: &Listing<'_>) -> Option<(Vec<DatasetFile>, SkippedFiles)> {
        let (files, skipped) = self.walk(Some(listing));
        Some((files.into_iter().map(local_file).collect(), skipped))
    }

    /// Past one wave: up to it the footers cost one round of reads either way, so the
    /// dataset opens whole and nothing is worth remembering.
    fn fingerprints(&self, files: usize) -> bool {
        files > FOOTERS_AT_ONCE
    }

    /// Each file's size and modification time in nanoseconds, many at once.
    fn stat(&self, files: &mut [DatasetFile], progress: &FooterProgress) -> bool {
        let stats = each_at_once(files.len(), |i| {
            if progress.is_cancelled() {
                return None;
            }
            let meta = std::fs::metadata(&files[i].key).ok()?;
            let modified = meta
                .modified()
                .ok()?
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_nanos() as u64;
            Some((meta.len(), modified))
        });
        let mut whole = true;
        for (file, stat) in files.iter_mut().zip(stats) {
            match stat {
                Some((size, stamp)) => (file.size, file.stamp) = (size, stamp),
                None => whole = false,
            }
        }
        whole
    }

    fn read_footers(
        &self,
        files: &Arc<Vec<DatasetFile>>,
        read: &[usize],
        progress: &Arc<FooterProgress>,
        meter: &Arc<Meter>,
    ) -> Option<Vec<Option<FileFooter>>> {
        let began = std::time::Instant::now();
        let pass = progress.pass(read.len());
        let footers = each_at_once(read.len(), |i| {
            // An abandoned open stops issuing reads; what it has is thrown away.
            if progress.is_cancelled() {
                pass.advance();
                return None;
            }
            let footer = files
                .get(read[i])
                .and_then(|f| local_footer(Path::new(&f.key)));
            // After the read, not before: the count is footers done with, and a footer
            // that will not parse is done with too.
            pass.advance();
            footer
        });
        drop(pass);
        // A local directory is read, not requested: no requests or bytes to report.
        meter.read_footers(began.elapsed(), Some(read.len()), false);
        Some(footers)
    }

    fn name_of(&self, file: &DatasetFile) -> Option<String> {
        Some(file.key.clone())
    }

    fn partition_path(&self, file: &DatasetFile) -> String {
        let path = Path::new(&file.key);
        path.strip_prefix(&self.dir)
            .unwrap_or(path)
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    }

    fn cloud_options(&self) -> Option<CloudOptions> {
        None
    }

    /// The directory's own, which moves when a file is added or removed: what the home
    /// screen holds a directory's record to.
    fn modified(&self, _files: &[DatasetFile]) -> u64 {
        std::fs::metadata(&self.dir)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_secs())
    }
}

/// One local Parquet file's footer, with each column's width.
pub(crate) fn local_footer(path: &Path) -> Option<FileFooter> {
    use polars::prelude::{ParquetReader, Schema, SchemaExt, SerReader};
    crate::schema_union::before_local_footer_read(path);
    let file = std::fs::File::open(path).ok()?;
    // Asked of the open handle, so it is the file the footer was read from and not
    // whatever is at that path by the time anyone looks again.
    let file_bytes = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
    let mut reader = ParquetReader::new(file);
    let arrow_schema = reader.schema().ok()?;
    let metadata = reader.get_metadata().ok()?;
    Some(FileFooter::from_metadata(
        Schema::from_arrow_schema(arrow_schema.as_ref()),
        metadata,
        file_bytes,
        true,
    ))
}

/// Each directory a walk read, with its entries in the order `read_dir` gave them and
/// whether each is a directory. One that could not be read is absent.
type WalkedDirs = HashMap<PathBuf, Vec<(PathBuf, bool)>>;

/// Read every directory under `root` down to `max_depth` levels, a level at a time
/// and many directories at once. Nothing is classified here, so the walk that does
/// classify sees the tree exactly as reading it one directory at a time would.
///
/// With a `listing`, each directory's files are counted off against it as it is read,
/// and an abandoned load stops the walk: what it has is incomplete and is not used.
///
/// With `enough`, the walk stops at the end of the level where it has seen more data
/// files than that, and reads at most [`MAX_NAMES_PER_DIR`] entries of a directory: a
/// look at a dataset rather than a listing of it.
fn walk_dirs(
    root: &Path,
    max_depth: usize,
    listing: Option<&Listing<'_>>,
    enough: Option<usize>,
) -> WalkedDirs {
    let mut walked = WalkedDirs::new();
    let mut level = vec![root.to_path_buf()];
    let mut seen = 0usize;
    for _ in 0..max_depth {
        if level.is_empty()
            || listing.is_some_and(|l| l.is_cancelled())
            || enough.is_some_and(|enough| seen > enough)
        {
            break;
        }
        let read = each_at_once(level.len(), |i| {
            if listing.is_some_and(|l| l.is_cancelled()) {
                return None;
            }
            let entries = std::fs::read_dir(&level[i]).ok()?;
            let entries: Vec<(PathBuf, bool)> = entries
                .flatten()
                .take(enough.map_or(usize::MAX, |_| MAX_NAMES_PER_DIR))
                .map(|entry| {
                    let path = entry.path();
                    // The entry's own type costs no stat; a link is followed, as
                    // `is_dir` would.
                    let is_dir = match entry.file_type() {
                        Ok(t) if t.is_symlink() => path.is_dir(),
                        Ok(t) => t.is_dir(),
                        Err(_) => path.is_dir(),
                    };
                    (path, is_dir)
                })
                .collect();
            if let Some(listing) = listing {
                listing.add(entries.iter().filter(|(_, is_dir)| !is_dir).count());
            }
            Some(entries)
        });
        let mut next = Vec::new();
        for (dir, entries) in level.into_iter().zip(read) {
            let Some(entries) = entries else {
                continue;
            };
            next.extend(
                entries
                    .iter()
                    .filter(|(_, is_dir)| *is_dir)
                    .map(|(p, _)| p.clone()),
            );
            if enough.is_some() {
                seen += entries
                    .iter()
                    .filter(|(path, is_dir)| {
                        !is_dir
                            && crate::discover::is_parquet_key(
                                &crate::discover::directory_and_name(path),
                            )
                    })
                    .count();
            }
            walked.insert(dir, entries);
        }
        level = next;
    }
    walked
}

/// Entries read from one directory by a walk that only looks. Past it the sample is
/// over the names this listing saw rather than over the directory, and the row count
/// is long out of reach either way.
const MAX_NAMES_PER_DIR: usize = 20_000;

/// `work` for each index below `n`, on up to a wave of threads pulling the next index
/// as each finishes, and the answers in index order. Sized for waiting on a disk or a
/// network mount rather than for the cores; a worker that panics answers `None` for
/// the indices it took.
fn each_at_once<T: Send>(n: usize, work: impl Fn(usize) -> Option<T> + Sync) -> Vec<Option<T>> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let next = AtomicUsize::new(0);
    let workers = n.min(FOOTERS_AT_ONCE);
    let mut out: Vec<Option<T>> = std::iter::repeat_with(|| None).take(n).collect();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut done = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        // Caught here rather than at the join: a worker that panicked
                        // would otherwise lose the answers it had already given.
                        let answer =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(i)))
                                .ok()
                                .flatten();
                        done.push((i, answer));
                    }
                    done
                })
            })
            .collect();
        for handle in handles {
            for (i, answer) in handle.join().unwrap_or_default() {
                out[i] = answer;
            }
        }
    });
    out
}

/// The data files `walked` found under `dir`, into `out`, counting what it passed over.
///
/// A directory of Parquet files often holds other things, and datui reads none of
/// them. Which ones they are is the difference between bookkeeping and a mistake:
/// `_SUCCESS` beside the data is a writer saying it finished, while three CSVs in
/// the same directory are three files somebody expected to be in the table.
fn collect_data_files(
    walked: &WalkedDirs,
    dir: &Path,
    out: &mut Vec<PathBuf>,
    skipped: &mut crate::schema_union::SkippedFiles,
    depth: usize,
    max_depth: usize,
    // Carried down rather than read off each leaf: a `.json` is a mistake beside
    // the data and a record of it inside `_delta_log`, and its own name cannot say
    // which. Every file under a writer's directory is that writer's.
    under_bookkeeping: bool,
) -> (bool, usize) {
    // A subtree too deep to walk reports no data, which makes everything above it
    // read as plumbing. At sixty-four levels that is unreachable, and the rows were
    // already missing before it also changed what they were called.
    if depth >= max_depth {
        return (false, 0);
    }
    let Some(entries) = walked.get(dir) else {
        return (false, 0);
    };
    // Held back until the directory has been read to the end: whether a file beside
    // the data is worth mentioning depends on whether there is any data beside it,
    // and that is not known until the last entry.
    let mut here: Vec<PathBuf> = Vec::new();
    let mut passed_over = 0usize;
    let mut data_below = false;
    for (child, is_dir) in entries {
        let child = child.clone();
        let bookkeeping = under_bookkeeping
            || child
                .file_name()
                .map(|n| n.to_string_lossy())
                .as_deref()
                .is_some_and(crate::discover::is_bookkeeping);
        if *is_dir {
            let (below, deferred) = collect_data_files(
                walked,
                &child,
                out,
                skipped,
                depth + 1,
                max_depth,
                bookkeeping,
            );
            data_below |= below;
            // A partition of this directory that holds no data of its own hands its
            // strays up: a day that landed as CSV is part of the dataset, and only
            // the directory above can see that it is.
            passed_over += deferred;
        } else if !bookkeeping
            && crate::discover::is_parquet_key(&crate::discover::directory_and_name(&child))
        {
            // The same test the cloud listing uses, so a directory is the same table
            // wherever it is read from. It gets the directory and the name rather
            // than the whole path: one of the two shapes it knows lives in the
            // directory name — Spark and GBIF write a dataset as
            // `occurrence.parquet/part-00001`, where the part files have no extension
            // of their own — and a whole path would reach it with backslashes on
            // Windows, which that test does not split on.
            here.push(child);
        } else if !bookkeeping {
            passed_over += 1;
        } else {
            skipped.count(true);
        }
    }
    let holds_data = data_below || !here.is_empty();
    out.append(&mut here);
    // A directory whose own name carries a partition key is part of the dataset above
    // it, whether or not its files turned out to be readable. Its strays go up to
    // be judged there rather than written off here.
    let partition = dir
        .file_name()
        .map(|n| n.to_string_lossy())
        .is_some_and(|n| n.contains('='));
    // Never at the top: there is nothing above the dataset's own directory to hand
    // them to, and the caller has nowhere to put them.
    if depth > 0 && !holds_data && partition && !under_bookkeeping {
        return (false, passed_over);
    }
    // A directory with data anywhere beneath it is part of somebody's table, so what
    // else is in there is beside their data. A directory with none is somebody's
    // infrastructure — a manifest directory, a directory of images, a log under a
    // name no convention covers — and nothing in it was ever going to be in this
    // table.
    for _ in 0..passed_over {
        skipped.count(!holds_data);
    }
    (holds_data, 0)
}

/// A prefix of Parquet objects in a store, or the objects a glob names.
#[cfg(feature = "cloud")]
pub struct StoreFiles {
    store: Arc<dyn object_store::ObjectStore>,
    /// The URL as opened: where the bucket and scheme come from, and what the dataset is
    /// filed under.
    full: String,
    /// The literal prefix listed: the whole key, or the part of a glob before its star.
    prefix: String,
    /// The glob the user named, where they named one. The listing keeps only the keys
    /// it matches, so everything downstream sees a plain list of files.
    pattern: Option<globset::GlobMatcher>,
    plan: crate::cloud_hive::ListShards,
    /// The URL of the prefix: for a glob, everything before its star. The layout notes
    /// take each file's path relative to it, so a root with a star in it is a prefix of
    /// nothing and every note goes quietly empty.
    root: String,
    cloud: CloudOptions,
    runtime: tokio::runtime::Handle,
}

#[cfg(feature = "cloud")]
impl StoreFiles {
    pub(crate) fn new(
        full: &str,
        prefix: String,
        pattern: Option<globset::GlobMatcher>,
        store: Arc<dyn object_store::ObjectStore>,
        cloud: CloudOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        Self {
            root: crate::cloud_hive::url_of_key(full, &prefix).unwrap_or_else(|| full.to_string()),
            plan: crate::cloud_hive::ListShards::for_url(full),
            full: full.to_string(),
            prefix,
            pattern,
            store,
            cloud,
            runtime: runtime.clone(),
        }
    }
}

#[cfg(feature = "cloud")]
impl DatasetFiles for StoreFiles {
    fn key(&self) -> &str {
        &self.full
    }

    fn root(&self) -> &str {
        &self.root
    }

    /// One listing, in key ranges at once where the store lists from an offset itself.
    fn list(&self, listing: &Listing<'_>) -> Option<(Vec<DatasetFile>, SkippedFiles)> {
        let (store, prefix, pattern, plan) = (
            self.store.clone(),
            self.prefix.clone(),
            self.pattern.clone(),
            self.plan,
        );
        let (listed, cancelled) = (listing.counter(), listing.cancel_flag());
        crate::wait_on_runtime(&self.runtime, async move {
            crate::cloud_hive::list_dataset_files_reporting(
                &store,
                &prefix,
                pattern.as_ref(),
                plan,
                listed,
                cancelled,
            )
            .await
        })?
        .ok()
    }

    /// Always: the listing already carries every object's size, stamp and tag.
    fn fingerprints(&self, _files: usize) -> bool {
        true
    }

    fn stat(&self, _files: &mut [DatasetFile], _progress: &FooterProgress) -> bool {
        true
    }

    /// A small ranged read at the end of each object, a wave at a time.
    fn read_footers(
        &self,
        files: &Arc<Vec<DatasetFile>>,
        read: &[usize],
        progress: &Arc<FooterProgress>,
        meter: &Arc<Meter>,
    ) -> Option<Vec<Option<FileFooter>>> {
        let (store, files, read, progress, meter) = (
            self.store.clone(),
            files.clone(),
            read.to_vec(),
            progress.clone(),
            meter.clone(),
        );
        crate::wait_on_runtime(&self.runtime, async move {
            crate::cloud_hive::footers_of_files_reporting(&store, &files, &read, &progress, &meter)
                .await
        })
    }

    fn name_of(&self, file: &DatasetFile) -> Option<String> {
        crate::cloud_hive::url_of_key(&self.full, &file.key)
    }

    fn partition_path(&self, file: &DatasetFile) -> String {
        file.key.clone()
    }

    fn cloud_options(&self) -> Option<CloudOptions> {
        Some(self.cloud.clone())
    }

    fn remote_objects(&self, files: &[DatasetFile]) -> Vec<crate::local_copy::RemoteObject> {
        files
            .iter()
            .filter_map(|file| {
                Some(crate::local_copy::RemoteObject {
                    url: self.name_of(file)?,
                    size: file.size,
                    etag: file.etag.clone(),
                })
            })
            .collect()
    }

    /// The newest object's: a remote record is never held to it, but the index evicts
    /// its oldest first.
    fn modified(&self, files: &[DatasetFile]) -> u64 {
        files.iter().map(|f| f.stamp).max().unwrap_or_default()
    }

    /// The listing came back with the one object the URL names, which is what `--hive`
    /// on a single object gets. The trailing slash is not asked about: this route is
    /// entered for `--hive s3://bucket/sales` too.
    fn is_one_object(&self, files: &[DatasetFile]) -> bool {
        files.len() == 1 && self.full.trim_end_matches('/').ends_with(&files[0].key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{ParquetWriter, df};
    use std::fs;
    use std::fs::File;

    fn write_parquet(path: &Path, n: i64) {
        let mut df = df!("v" => (0..n).collect::<Vec<i64>>()).unwrap();
        let file = File::create(path).unwrap();
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }

    /// Every Parquet file under `dir`, the footers an open would read, and those
    /// footers: the open's first two steps, as it takes them.
    fn footers_of(
        dir: &Path,
        progress: &Arc<FooterProgress>,
        meter: &Arc<Meter>,
    ) -> (Vec<DatasetFile>, Vec<usize>, Vec<Option<FileFooter>>) {
        let local = LocalFiles::new(dir);
        let (files, _skipped, _fingerprint) = list(&local, progress, meter).unwrap();
        let read = footers_to_read(files.len());
        let files = Arc::new(files);
        let footers = local.read_footers(&files, &read, progress, meter).unwrap();
        (files.to_vec(), read, footers)
    }

    #[test]
    fn a_count_that_read_nothing_leaves_the_measurement_for_the_one_that_does() {
        // Counting gets one measurement, and a pass where no footer parsed settled
        // nothing. If such a pass took it, the pass that eventually succeeds is
        // declined and the row never reflects the count at all.
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("date=2024-01-01");
        std::fs::create_dir_all(&part).unwrap();
        std::fs::write(part.join("broken.parquet"), b"not parquet").unwrap();

        let meter = Meter::default();
        // As the open leaves it. A count belongs to an open this meter measured, so
        // without a listing here the count would be declined for that reason instead.
        meter.listed(std::time::Duration::from_millis(1), Some(1), false);
        assert!(
            LocalFiles::new(dir.path()).count_rows(&meter).is_err(),
            "nothing under there parses"
        );
        assert_eq!(
            meter.footers(),
            None,
            "so nothing was measured, and the one measurement is still to be had"
        );

        // The file is replaced by one that does parse, as a half-written file is once
        // its writer finishes.
        let mut frame = df!("n" => &[1i64, 2]).unwrap();
        let f = std::fs::File::create(part.join("broken.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
        assert_eq!(
            LocalFiles::new(dir.path()).count_rows(&meter).unwrap(),
            2,
            "and now it counts"
        );
        assert_eq!(
            meter.footers().and_then(|c| c.files),
            Some(1),
            "and the count that worked is the one reported"
        );
    }

    #[test]
    fn counting_a_directory_again_is_not_more_of_what_the_open_cost() {
        // Counting runs whenever the row count is invalidated, and clearing a filter does
        // it — so on a dataset somebody is exploring this function runs over and over.
        // Each run re-walks the directory and re-reads every footer, and if each one were
        // added the section headed by what opening the dataset cost would climb for as
        // long as the session lasted.
        let dir = tempfile::tempdir().unwrap();
        for day in 1..=3 {
            let d = dir.path().join(format!("date=2024-01-0{day}"));
            std::fs::create_dir_all(&d).unwrap();
            let mut frame = df!("n" => &[day as i64]).unwrap();
            let f = std::fs::File::create(d.join("data.parquet")).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
        }

        let meter = Meter::default();
        meter.listed(std::time::Duration::from_millis(1), Some(3), false);
        let first = LocalFiles::new(dir.path()).count_rows(&meter).unwrap();
        let after_one = meter.footers().expect("the first count was measured");
        assert_eq!(
            after_one.files,
            Some(3),
            "a footer read from each of the three"
        );

        for _ in 0..3 {
            let again = LocalFiles::new(dir.path()).count_rows(&meter).unwrap();
            assert_eq!(again, first, "the same count every time");
        }
        assert_eq!(
            meter.footers(),
            Some(after_one),
            "and the figures stand where the first count left them"
        );
    }

    #[test]
    fn test_count_rows_from_parquet_dir_sums_footers() {
        let dir = tempfile::tempdir().unwrap();
        // Hive-style layout: two partitions, multiple files each.
        let p1 = dir.path().join("year=2020");
        let p2 = dir.path().join("year=2021");
        fs::create_dir_all(&p1).unwrap();
        fs::create_dir_all(&p2).unwrap();
        write_parquet(&p1.join("a.parquet"), 10);
        write_parquet(&p1.join("b.parquet"), 5);
        write_parquet(&p2.join("c.parquet"), 7);

        let n = LocalFiles::new(dir.path())
            .count_rows(&Meter::default())
            .unwrap();
        assert_eq!(n, 22, "should sum footer row counts across all files");
    }

    #[test]
    fn test_count_rows_skips_non_parquet_and_corrupt_files() {
        let dir = tempfile::tempdir().unwrap();
        write_parquet(&dir.path().join("good.parquet"), 8);
        // A non-parquet file must be ignored entirely.
        fs::write(dir.path().join("notes.txt"), b"ignore me").unwrap();
        // A corrupt .parquet must be skipped, not abort the whole count.
        fs::write(dir.path().join("bad.parquet"), b"not a parquet footer").unwrap();

        let n = LocalFiles::new(dir.path())
            .count_rows(&Meter::default())
            .unwrap();
        assert_eq!(n, 8, "non-parquet and unreadable files should be skipped");
    }

    #[test]
    fn test_count_rows_errors_when_no_readable_parquet() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("only.txt"), b"nothing here").unwrap();
        assert!(
            LocalFiles::new(dir.path())
                .count_rows(&Meter::default())
                .is_err(),
            "a directory with no parquet files should error so the caller can fall back"
        );
    }

    /// The real footer pass counts real footers.
    ///
    /// The unit tests above drive the counter by hand; this is the one that says the
    /// pass is wired to it at all, and that the total is the footers it will read
    /// rather than the files there are — the two differ once a dataset is large enough
    /// to be sampled.
    #[test]
    fn the_footer_pass_counts_the_footers_it_reads() {
        use polars::prelude::{ParquetWriter, df};

        let dir = tempfile::tempdir().unwrap();
        for day in 1..=4 {
            let d = dir.path().join(format!("date=2024-01-0{day}"));
            std::fs::create_dir_all(&d).unwrap();
            let mut frame = df!("n" => [day as i64]).unwrap();
            let f = std::fs::File::create(d.join("data.parquet")).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
        }

        let progress = Arc::new(FooterProgress::default());
        let (files, read, footers) = footers_of(dir.path(), &progress, &Arc::new(Meter::default()));
        assert_eq!((files.len(), read.len(), footers.len()), (4, 4, 4));
        assert_eq!(
            progress.reading(),
            None,
            "the pass says nothing once it has landed"
        );
        assert_eq!(
            progress.last_pass().begun,
            1,
            "and it did report: the count is unobservable afterwards, so without this \
             a pass that never told anyone would look the same as one that did"
        );
        assert_eq!(
            progress.last_pass().read,
            4,
            "counting every footer it read, not just starting and stopping"
        );
    }

    /// A local open measures finding the files and reading their footers separately.
    ///
    /// Separately because they are separate costs and a directory that is slow to open is
    /// slow at one of them; a single figure over both would say a directory is slow
    /// without saying at what. Neither claims requests or bytes: a local directory is
    /// read, not requested, and a zero there would read as "nothing moved" rather than
    /// "not datui's to count".
    ///
    /// Only the counts are asserted. The times are real elapsed times on a machine
    /// doing other things, so the only claim about them that holds every time is that
    /// they were recorded at all — which `Some` already says.
    #[test]
    fn a_local_open_measures_its_listing_and_its_footers() {
        let dir = tempfile::tempdir().unwrap();
        for day in 1..=4 {
            let d = dir.path().join(format!("date=2024-01-0{day}"));
            std::fs::create_dir_all(&d).unwrap();
            let mut frame = df!("n" => &[day as i64]).unwrap();
            let f = std::fs::File::create(d.join("data.parquet")).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
        }

        let meter = Arc::new(Meter::default());
        let _ = footers_of(dir.path(), &Arc::new(FooterProgress::default()), &meter);

        let listing = meter.listing().expect("the open measured its listing");
        assert_eq!(listing.files, Some(4), "the walk found four files");
        assert!(
            listing.over_the_wire.is_none(),
            "and made no requests to find them"
        );
        let footers = meter.footers().expect("and measured its footer pass");
        assert_eq!(footers.files, Some(4), "a footer was read from each");

        assert!(
            footers.over_the_wire.is_none(),
            "off a disk, not a wire: no requests to report and no bytes to claim"
        );
    }

    /// The denominator is the footers it will read, not the files there are.
    ///
    /// The two are the same number until a dataset is large enough to be sampled, which
    /// is why this fixture is twenty thousand and one files — below that the confusion
    /// is invisible, and a test that cannot see it is not a test of it. The files need
    /// not be real Parquet: a footer that will not read is still a footer counted off,
    /// which is the other half of what this asserts.
    #[test]
    fn the_footer_count_is_over_the_footers_read_not_the_files_there_are() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("date=2024-01-01");
        std::fs::create_dir_all(&sub).unwrap();
        let files = crate::schema_union::MAX_FOOTER_READS + 1;
        for i in 0..files {
            std::fs::write(sub.join(format!("f{i:0>6}.parquet")), b"not parquet").unwrap();
        }

        let progress = Arc::new(FooterProgress::default());
        let meter = Arc::new(Meter::default());
        let (found, read, footers) = footers_of(dir.path(), &progress, &meter);
        assert_eq!(found.len(), files, "every file is listed");
        assert_eq!(
            read.len(),
            crate::schema_union::MAX_FOOTER_READS,
            "and a sample of them is read"
        );
        assert!(footers.iter().all(Option::is_none), "none of them parses");
        // The same distinction, in the measurement: the listing found every file and
        // the footer pass read a sample of them. A fixture below the sampling threshold
        // cannot tell the two numbers apart, which is why this one asserts them.
        assert_eq!(
            meter.listing().and_then(|c| c.files),
            Some(files),
            "the listing counts the files there are"
        );
        assert_eq!(
            meter.footers().and_then(|c| c.files),
            Some(crate::schema_union::MAX_FOOTER_READS),
            "while the footer pass counts the footers it read, which is fewer"
        );

        assert_eq!(
            progress.last_pass().total,
            read.len(),
            "the screen's denominator is the sample, not the {files} files there are"
        );
        assert_eq!(
            progress.last_pass().read,
            read.len(),
            "and every one of them was counted off, parse or no parse"
        );
    }

    /// A local listing counts the files it finds for the loading screen, as a cloud
    /// one does, and an abandoned open stops it before it reads a directory (#710).
    #[test]
    fn a_local_listing_counts_its_files_and_stops_when_abandoned() {
        let dir = tempfile::tempdir().unwrap();
        for day in 0..3 {
            let sub = dir.path().join(format!("day={day}"));
            std::fs::create_dir_all(&sub).unwrap();
            for i in 0..4 {
                std::fs::write(sub.join(format!("f{i}.parquet")), b"x").unwrap();
            }
        }
        let progress = FooterProgress::default();
        {
            let listing = progress.listing();
            let (files, _) = LocalFiles::new(dir.path()).walk(Some(&listing));
            assert_eq!(files.len(), 12);
            assert_eq!(progress.listed(), Some(12), "Listing files: 12");
        }
        assert_eq!(progress.listed(), None, "and says nothing once it is done");

        progress.cancel();
        let listing = progress.listing();
        let (files, _) = LocalFiles::new(dir.path()).walk(Some(&listing));
        assert!(files.is_empty(), "a cancelled listing reads no directory");
        assert_eq!(progress.listed(), Some(0));
    }

    /// A tree of `files` small Parquet files under `part=N/` directories, a hundred to a
    /// directory, three rows each.
    fn tree(dir: &Path, files: usize) {
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes)
            .finish(&mut df!("v" => [1i64, 2, 3]).unwrap())
            .unwrap();
        for i in 0..files {
            let sub = dir.join(format!("part={}", i / 100));
            fs::create_dir_all(&sub).unwrap();
            fs::write(sub.join(format!("f{i:04}.parquet")), &bytes).unwrap();
        }
    }

    /// Open `dir` as the app does, and wait for the pass behind the open.
    fn open_whole(dir: &Path, cache: &crate::cache::CacheManager) {
        let progress = Arc::new(FooterProgress::default());
        let report = OpenReport {
            progress: progress.clone(),
            meter: Arc::new(Meter::default()),
            remembered: Some(cache.clone()),
        };
        let options = crate::OpenOptions {
            hive: true,
            ..crate::OpenOptions::default()
        };
        let (_, facts) = open(Arc::new(LocalFiles::new(dir)), &options, &report).unwrap();
        if let Some(join) = facts.footers_pending {
            join(&progress).expect("the pass reads the rest");
        }
    }

    /// A local open records what the home screen shows, as a cloud one does, under the
    /// directory however it was named.
    #[test]
    fn a_local_open_records_what_the_home_screen_will_show() {
        let dir = tempfile::tempdir().unwrap();
        tree(dir.path(), 3);
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(cache_dir.path().to_path_buf());
        open_whole(dir.path(), &cache);

        let key = crate::canonical::canonicalize(dir.path()).unwrap();
        let facts = cache.dataset_facts(&key).expect("recorded");
        assert_eq!(facts.rows, Some(9));
        assert_eq!(facts.kind, Some(crate::discover::EntryKind::Hive));
        assert_eq!(facts.columns, ["part", "v"]);
        assert!(!facts.cols_sampled);
        assert!(
            facts.size > 0,
            "the size, from the files the footers came from"
        );
        assert_eq!(
            facts.mtime,
            LocalFiles::new(dir.path()).modified(&[]),
            "dated by the directory, which is what the home screen holds it to"
        );
    }

    /// The home screen measures a dataset past its footer budget from the shape an
    /// open kept, reading no footer; one that changed since is sampled again.
    #[test]
    fn the_home_screen_measures_a_large_dataset_from_the_shape_an_open_kept() {
        use crate::discover::{Entry, EntryKind};
        let dir = tempfile::tempdir().unwrap();
        tree(dir.path(), 150);
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(cache_dir.path().to_path_buf());
        let measure = |cache: Option<&crate::cache::CacheManager>| {
            let mut entry = Entry::directory(dir.path());
            entry.kind = EntryKind::Hive;
            crate::discover::enrich_with(
                &mut entry,
                &crate::schema_union::ReadAs::default(),
                cache,
            );
            entry
        };
        assert_eq!(
            measure(Some(&cache)).rows,
            None,
            "past the budget, unopened"
        );

        open_whole(dir.path(), &cache);
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = reads.clone();
        let _hook = crate::schema_union::on_local_footer_read(dir.path(), move |_| {
            counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        let entry = measure(Some(&cache));
        assert_eq!(
            entry.rows,
            Some(450),
            "every file's rows, from the open's footers"
        );
        assert_eq!(
            entry.cols,
            Some(2),
            "the partition column and the file's own"
        );
        assert!(!entry.cols_sampled);
        assert_eq!(entry.cost.row_groups, Some(150));
        assert_eq!(
            reads.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "and no footer was read for it"
        );
        assert_eq!(
            measure(None).rows,
            None,
            "without the shape, a sample as before"
        );

        // A file added since: the listing no longer matches, so the shape is not used.
        tree(dir.path(), 151);
        assert_eq!(measure(Some(&cache)).rows, None);
    }
}
