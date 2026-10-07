//! A dataset of many Parquet files, wherever they are. [`DatasetFiles`] lists, stats
//! and reads footers; [`LocalFiles`] (a directory) and [`StoreFiles`] (an object-store
//! prefix) implement it. Built once on top: opening from the two ends past one wave of
//! footers, the pass reading the rest behind, the count reading only what neither did,
//! and the shape and facts kept for the next open and home. They differ only by
//! medium: a store lists key ranges at once with sizes and tags; a directory is walked
//! level by level, stat'ed only when worth remembering, and its footers keep column
//! widths.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::io::cloud::CloudOptions;
use polars::prelude::{LazyFrame, PlSmallStr};

use crate::formats::schema_union::{
    ESTIMATE_SAMPLE, FOOTERS_AT_ONCE, FileFooter, FooterProgress, Listing, RowEstimate,
    SkippedFiles, ends_of, footers_to_read, random_sample,
};
use crate::loading::measurements::{Meter, OpenReport};
use crate::table::{
    DataTableState, DatasetAtOpen, FileCounter, FileScan, FootersFound, OpenFacts, RemoteRead,
};

/// One data file of a dataset: where it is, its size and when it was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFile {
    /// Its key in the store, or its path.
    pub key: String,
    /// Its size, where the listing or a stat said; `0` until then.
    pub size: u64,
    /// When it was last written (listing or stat): part of the fingerprint deciding whether
    /// remembered facts still apply.
    pub stamp: u64,
    /// The store's tag for this object version, if given: the strongest, free part of the
    /// fingerprint (size and whole-second mtime miss same-length rewrites within a second).
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
    /// The footers of `files` at `read`, in order, counted against `progress` and timed into
    /// `meter`. An unreadable footer is `None` (a file mid-write must not block the open);
    /// `None` overall when abandoned.
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
    fn remote_objects(
        &self,
        _files: &[DatasetFile],
    ) -> Vec<crate::cloud::local_copy::RemoteObject> {
        Vec::new()
    }
    /// The modification time the dataset index records for the dataset.
    fn modified(&self, files: &[DatasetFile]) -> u64;
    /// Whether `file`, whose footer would not read, holds nothing at all. Asked only of
    /// such a file, where the listing did not already say its size.
    fn is_empty(&self, _file: &DatasetFile) -> bool {
        false
    }
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
    /// Where the home screen's record is written, off the open's path. One write at a
    /// time, so a sample's record never lands after the whole one it would not replace.
    writes: crate::app::background::CacheWrites,
    writing: Arc<std::sync::Mutex<()>>,
}

/// A dataset as some set of its footers describes it, and the scan that reads it.
struct Opened {
    dataset: crate::formats::schema_union::DatasetSchema,
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

/// Open the dataset `source` lists from its footers; `None` when nothing readable is
/// listed (the caller falls back to the general scan). Past one wave, the two ends open
/// it and the rest join behind; up to a wave it opens whole. An unchanged listing whose
/// footers were all read before opens from memory, reading none.
pub(crate) fn open(
    source: Arc<dyn DatasetFiles>,
    options: &crate::OpenOptions,
    report: &OpenReport,
) -> Option<(DataTableState, OpenFacts)> {
    let (files, skipped, fingerprint) = list(&*source, &report.progress, &report.meter)?;
    if report.progress.is_cancelled() || files.is_empty() {
        return None;
    }
    let mut listed = Listed {
        writes: report.writes.clone(),
        ..Listed::new(
            source,
            files,
            skipped,
            fingerprint,
            report.meter.clone(),
            report.remembered.clone(),
        )?
    };
    let mut files = listed.files.clone();
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
    let mut footers = match remembered {
        Some(footers) => footers,
        None => listed
            .source
            .read_footers(&files, &read, &report.progress, &report.meter)?,
    };
    // Within a wave nothing is stat'ed, so empty files show up only as unreadable footers:
    // check those and leave them out, as a stat would.
    if !staged && !from_cache {
        let empty: Vec<bool> = footers
            .iter()
            .zip(files.iter())
            .map(|(footer, file)| footer.is_none() && listed.source.is_empty(file))
            .collect();
        if empty.contains(&true) {
            let mut kept = Vec::new();
            let mut skipped = listed.skipped;
            footers = footers
                .into_iter()
                .zip(files.iter())
                .zip(&empty)
                .filter_map(|((footer, file), &empty)| {
                    if empty {
                        skipped.empty += 1;
                        return None;
                    }
                    kept.push(file.clone());
                    Some(footer)
                })
                .collect();
            listed = Listed {
                writes: listed.writes.clone(),
                ..Listed::new(
                    listed.source.clone(),
                    kept,
                    skipped,
                    listed.fingerprint.clone(),
                    listed.meter.clone(),
                    listed.remembered.clone(),
                )?
            };
            files = listed.files.clone();
        }
    }
    let read: Vec<usize> = if read.len() == footers.len() {
        read
    } else {
        (0..files.len()).collect()
    };
    let listed = Arc::new(listed);
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
    // A shape just found needs no storing again: the lookup has dated it.
    listed.remember(&read, &footers, !from_cache);
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
        column_bytes: crate::formats::schema_union::column_bytes_per_row(&footers),
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
            // A random sample first (a quick row estimate), then the schema's spread; the ends were
            // read by the open, and no footer is read twice.
            let seed = crate::cache::stable_hash(listed.source.key().as_bytes());
            let sample = random_sample(files.len(), ESTIMATE_SAMPLE, seed);
            let first: Vec<usize> = sample
                .iter()
                .copied()
                .filter(|i| !ends.contains(i))
                .collect();
            let mut known: std::collections::BTreeMap<usize, Option<FileFooter>> =
                ends.iter().copied().zip(footers.iter().cloned()).collect();
            let fresh = if first.is_empty() {
                Vec::new()
            } else {
                listed
                    .source
                    .read_footers(&files, &first, progress, &listed.meter)?
            };
            if progress.is_cancelled() {
                return None;
            }
            known.extend(first.iter().copied().zip(fresh));
            let sampled: Vec<Option<FileFooter>> = sample
                .iter()
                .filter_map(|i| known.get(i).cloned())
                .collect();
            progress.set_estimate(RowEstimate::of(files.len(), &sampled));
            let mut read = footers_to_read(files.len());
            read.extend(sample.iter().copied());
            read.sort_unstable();
            read.dedup();
            let rest: Vec<usize> = read
                .iter()
                .copied()
                .filter(|i| !known.contains_key(i))
                .collect();
            let fresh = if rest.is_empty() {
                Vec::new()
            } else {
                listed
                    .source
                    .read_footers(&files, &rest, progress, &listed.meter)?
            };
            if progress.is_cancelled() {
                return None;
            }
            known.extend(rest.iter().copied().zip(fresh));
            let footers: Vec<Option<FileFooter>> =
                read.iter().map(|i| known.remove(i).flatten()).collect();
            let estimate = RowEstimate::of(files.len(), &footers);
            // Past `MAX_FOOTER_READS` this read a sample, and the dataset has no row
            // groups until its count reads the rest — only the rest.
            let whole = listed.dataset(&read, &footers)?;
            // This is the pass that reads a large dataset's footers, so this is where
            // a large dataset gets remembered.
            listed.remember(&read, &footers, true);
            Some(FootersFound {
                dataset: whole.dataset,
                lf: whole.lf,
                file_rows: whole.file_rows,
                // Every file listed: the dataset's per-file findings index this.
                files: listed.names.clone(),
                row_groups: whole.row_groups,
                remote: Some(whole.by_file),
                estimate,
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
    let (mut files, mut skipped) = source.list(&listing)?;
    // After the sort: the files are not found until they are in the order the scan
    // will read them in.
    meter.listed(began.elapsed(), Some(files.len()), false);
    // Taking it costs nothing in a store, whose listing carries every file's size and
    // tag; on a disk it costs a stat a file, which within a wave is not worth it.
    let stated = source.fingerprints(files.len()) && source.stat(&mut files, progress);
    if stated {
        // A name that says data over nothing at all is a write that stopped, which a
        // bucket's listing leaves out by its size: the same here once the stat has one.
        let listed = files.len();
        files.retain(|f| f.size > 0);
        skipped.empty += listed - files.len();
    }
    let fingerprint = stated.then(|| fingerprint_of(&files));
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
        // From the listing: the newest file names the columns and the ends type them, so a tree
        // reads alike from disk or bucket without rereading directories.
        let (partition_columns, values) = crate::formats::schema_union::partitions_of_listing(
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
            writes: Default::default(),
            writing: Default::default(),
        })
    }

    /// Whether the cache may know `file` by its identity: the listing or a stat said
    /// its size and when it was written, or its tag. A name alone could be any file.
    fn identifiable(file: &DatasetFile) -> bool {
        file.size > 0 && (file.stamp > 0 || file.etag.is_some())
    }

    /// The footers of the files at `read` that earlier counts read, while each file is
    /// still the one they read; `None` for the rest.
    fn cached_footers(&self, read: &[usize]) -> Vec<Option<FileFooter>> {
        let none = || vec![None; read.len()];
        let Some(kept) = self
            .remembered
            .as_ref()
            .and_then(|cache| cache.file_footers(self.source.key()))
        else {
            return none();
        };
        let by_identity: HashMap<u64, &crate::cache::CachedFooter> = kept
            .files
            .iter()
            .map(|(id, footer)| (*id, footer))
            .collect();
        let schemas: Vec<Option<Arc<polars::prelude::Schema>>> = (0..kept.schemas.len())
            .map(|at| crate::cache::DatasetShape::schema_at(&kept.schemas, at).map(Arc::new))
            .collect();
        read.iter()
            .map(|&i| {
                let file = self.files.get(i).filter(|f| Self::identifiable(f))?;
                let id = crate::cache::file_identity(
                    &file.key,
                    file.size,
                    file.stamp,
                    file.etag.as_deref(),
                );
                let footer = by_identity.get(&id)?;
                let schema = schemas.get(footer.schema?)?.clone()?;
                let column_bytes = schema
                    .iter_names()
                    .zip(&footer.column_bytes)
                    .map(|(name, bytes)| (name.to_string(), *bytes))
                    .collect();
                Some(FileFooter {
                    schema,
                    row_group_rows: footer.row_group_rows.clone(),
                    row_group_bytes: footer.row_group_bytes.clone(),
                    file_bytes: file.size as usize,
                    column_bytes,
                })
            })
            .collect()
    }

    /// Keep the footers just read at `read` with the ones kept before, for the files
    /// the dataset lists now: a file gone or written again since is forgotten.
    fn remember_footers(&self, read: &[usize], footers: &[Option<FileFooter>]) {
        let Some(cache) = self.remembered.as_ref() else {
            return;
        };
        if !footers.iter().any(Option::is_some) {
            return;
        }
        let kept = cache.file_footers(self.source.key()).unwrap_or_default();
        let mut schemas = kept.schemas.clone();
        let mut by_identity: HashMap<u64, crate::cache::CachedFooter> =
            kept.files.into_iter().collect();
        for (&i, footer) in read.iter().zip(footers) {
            let (Some(file), Some(footer)) = (self.files.get(i), footer) else {
                continue;
            };
            if !Self::identifiable(file) {
                continue;
            }
            let id =
                crate::cache::file_identity(&file.key, file.size, file.stamp, file.etag.as_deref());
            let schema = crate::cache::DatasetShape::intern_schema(&mut schemas, &footer.schema);
            by_identity.insert(
                id,
                crate::cache::CachedFooter {
                    schema: Some(schema),
                    row_group_rows: footer.row_group_rows.clone(),
                    row_group_bytes: footer.row_group_bytes.clone(),
                    column_bytes: footer.column_bytes.iter().map(|(_, b)| *b).collect(),
                },
            );
        }
        let files: Vec<(u64, crate::cache::CachedFooter)> = self
            .files
            .iter()
            .filter(|f| Self::identifiable(f))
            .filter_map(|f| {
                let id = crate::cache::file_identity(&f.key, f.size, f.stamp, f.etag.as_deref());
                by_identity.remove(&id).map(|footer| (id, footer))
            })
            .collect();
        cache.save_file_footers(
            self.source.key(),
            &crate::cache::FileFooters { schemas, files },
        );
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
        crate::formats::schema_union::footers_from_cache(&shape.files, &shape.schemas, &sizes)
    }

    /// Keep what a pass learned: home's facts and, with `shape`, every footer, only if all
    /// were read (a staged or sampled read would shrink the next open's dataset) and all
    /// parsed (a throttled or expired read would make a file vanish on every later open).
    fn remember(&self, read: &[usize], footers: &[Option<FileFooter>], shape: bool) {
        let Some(cache) = self.remembered.as_ref() else {
            return;
        };
        // The shape first: it is on the pass's path, and the record behind it would
        // otherwise hold the cache's lock while it waits on the disk.
        if let Some(fingerprint) = self.fingerprint.as_ref().filter(|_| shape)
            && read.len() == self.files.len()
            && footers.iter().all(Option::is_some)
        {
            let (cached, schemas) = crate::formats::schema_union::footers_to_cache(footers);
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
        // Home's facts are written behind the open, even from a sampled read (columns), but a
        // sample never replaces a whole read: the shape cache forgets far sooner than the
        // index.
        let path = PathBuf::from(self.source.key());
        if let Some(facts) = self.facts(read, footers) {
            let (cache, writing) = (cache.clone(), self.writing.clone());
            self.writes.spawn(move || {
                let _one = writing.lock().unwrap_or_else(|e| e.into_inner());
                let existing = cache.dataset_facts(&path);
                // A reopen learns what it knew: no write.
                let same = existing.as_ref().is_some_and(|old| {
                    (
                        old.rows,
                        old.cols,
                        old.size,
                        old.mtime,
                        &old.columns,
                        old.kind,
                    ) == (
                        facts.rows,
                        facts.cols,
                        facts.size,
                        facts.mtime,
                        &facts.columns,
                        facts.kind,
                    ) && old.classified_by == facts.classified_by
                });
                if !same && facts_worth_recording(existing.as_ref(), &facts) {
                    cache.record_dataset_facts(&[(path, facts)]);
                }
            });
        }
    }

    /// Every column the footers at `read` give, typed with the partition columns ahead.
    /// `None` when none of them could be read.
    fn schema(
        &self,
        read: &[usize],
        footers: &[Option<FileFooter>],
    ) -> Option<crate::formats::schema_union::DatasetSchema> {
        let mut dataset =
            crate::formats::schema_union::union_sampled(self.files.len(), read, footers);
        if dataset.schema.is_empty() {
            return None;
        }
        dataset.schema = Arc::new(crate::formats::schema_union::with_partition_columns(
            &dataset.schema,
            &self.partition_columns,
            &self.values,
        ));
        Some(dataset)
    }

    /// Column names as the union orders them, partition columns first, untyped, over each
    /// distinct schema once; `None` when no footer read.
    fn column_names(&self, footers: &[Option<FileFooter>]) -> Option<Vec<String>> {
        // Each distinct schema once, in the order its files first come.
        let mut schemas: Vec<&Arc<polars::prelude::Schema>> = Vec::new();
        for footer in footers.iter().flatten() {
            if !schemas
                .iter()
                .any(|s| Arc::ptr_eq(s, &footer.schema) || **s == footer.schema)
            {
                schemas.push(&footer.schema);
            }
        }
        // The newest file's columns lead, as in the union.
        let newest = &footers.iter().rev().flatten().next()?.schema;
        let mut names: Vec<String> = self.partition_columns.clone();
        let mut seen: std::collections::HashSet<String> = names.iter().cloned().collect();
        for schema in std::iter::once(newest).chain(schemas) {
            for name in schema.iter_names() {
                if seen.insert(name.to_string()) {
                    names.push(name.to_string());
                }
            }
        }
        Some(names)
    }

    /// What the home screen can say about the dataset from the footers at `read`: its
    /// columns, its rows when every footer was read, its kind and what it holds.
    fn facts(
        &self,
        read: &[usize],
        footers: &[Option<FileFooter>],
    ) -> Option<crate::cache::DatasetFacts> {
        use crate::home::discover::{CLASSIFIER_VERSION, EntryKind, Holds};
        let columns = self.column_names(footers)?;
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

    /// What the footers at `read` say of the dataset, shared by the open (maybe only the
    /// ends) and the pass reading the rest. `None` when nothing could be read.
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
        // Over readable files only, as the scan: a sampled-past or failed footer would count no
        // rows and put its rows out of a windowed scan's reach; the count reads it instead.
        let row_groups: Vec<Vec<usize>> = if every_footer && footers.iter().all(Option::is_some) {
            footers
                .iter()
                .flatten()
                .map(|f| f.row_group_rows.clone())
                .collect()
        } else {
            Vec::new()
        };
        // A file holding a column in an incompatible type is not read for it: its rows are null
        // there, tagged with its drift group to tell from real nulls.
        let drift = crate::formats::schema_union::ScanDrift::new(&self.names, &dataset, &file_rows);
        // The files that will open: an unreadable footer means Polars cannot read it either,
        // failing the first page. A staged open excludes only what it read; the pass behind
        // finds the rest and the join swaps the scan.
        let readable =
            crate::formats::schema_union::readable_paths(&self.names, &dataset.unreadable);
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
                let lf = crate::formats::schema_union::lenient_scan(
                    files,
                    schema.clone(),
                    cloud.clone(),
                    drift.as_deref(),
                    as_text,
                )?;
                Ok(crate::loading::open_scan::hoist_partition_columns(
                    lf,
                    &schema,
                    &partition_columns,
                    drift.is_some(),
                ))
            })
        };
        let lf = scan(&readable, &[]).ok()?;
        let count: FileCounter = if row_groups.is_empty() {
            // The same files as the scan: the counter answers one entry per file, matched by
            // length. Binary search: datasets can have hundreds of thousands of files.
            let counted: Vec<usize> = (0..files.len())
                .filter(|index| dataset.unreadable.binary_search(index).is_err())
                .collect();
            if counted.len() != readable.len() {
                return None;
            }
            self.counter(crate::formats::schema_union::FooterCount::new(
                files.len(),
                counted,
                read.iter().copied().zip(footers.iter().cloned()),
            ))
        } else {
            // The rows are in the footers, so the counter answers without reading.
            let counted = row_groups.clone();
            Arc::new(move |_: &Arc<FooterProgress>| Ok(counted.clone()))
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
        count: crate::formats::schema_union::FooterCount<FileFooter>,
    ) -> FileCounter {
        let (listed, count) = (self.clone(), Arc::new(count));
        Arc::new(move |progress: &Arc<FooterProgress>| {
            let counted = count
                .count(
                    |missing| {
                        // Footers an earlier count read of unchanged files are reused; this one's are kept even
                        // if stopped partway.
                        let mut found = listed.cached_footers(missing);
                        let to_read: Vec<usize> = missing
                            .iter()
                            .zip(&found)
                            .filter(|(_, footer)| footer.is_none())
                            .map(|(i, _)| *i)
                            .collect();
                        let fresh = footers_for_count(
                            &*listed.source,
                            &listed.files,
                            &to_read,
                            &listed.meter,
                            progress,
                        )?;
                        listed.remember_footers(&to_read, &fresh);
                        if progress.is_cancelled() {
                            return None;
                        }
                        let mut fresh = fresh.into_iter();
                        for footer in found.iter_mut().filter(|f| f.is_none()) {
                            *footer = fresh.next().flatten();
                        }
                        Some(found)
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

/// The footers of `files` at `read` for the count pass, timed into `meter` once and only
/// when something parsed. Counted on its own meter first, so repeated passes (each
/// time the count is invalidated) do not inflate the open's figures.
pub(crate) fn footers_for_count(
    source: &dyn DatasetFiles,
    files: &Arc<Vec<DatasetFile>>,
    read: &[usize],
    meter: &Meter,
    progress: &Arc<FooterProgress>,
) -> Option<Vec<Option<FileFooter>>> {
    if read.is_empty() {
        return Some(Vec::new());
    }
    let counting = Arc::new(Meter::default());
    let began = std::time::Instant::now();
    let footers = source.read_footers(files, read, progress, &counting)?;
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
    listed.counter(crate::formats::schema_union::FooterCount::new(
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

/// Every footer of local `dir` as the last full read left them, if its files are
/// unchanged. Only remembered directories are listed for this; others cost a stat.
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
    let footers =
        crate::formats::schema_union::footers_from_cache(&shape.files, &shape.schemas, &sizes)?;
    Some((files, footers))
}

/// Whether a record should replace the index's: anything replaces nothing, a whole read
/// replaces anything, a sample replaces only a sample.
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
    /// counted against `listing`. Directories are read many at once (each a round trip on a
    /// network mount) and classified afterwards, as a serial walk would. A cancelled listing
    /// returns early, and the caller drops the result.
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
        // Load-bearing: Polars takes the hive schema from the first file, so order decides
        // whether disagreeing partition keys open with a null partition column or fail (see the
        // two `directories_that` integration tests).
        files.sort();
        (files, skipped)
    }

    /// The directory's first files, sorted, down `levels` levels: more than `enough` when
    /// there are (telling a too-large dataset). For home, which looks rather than opens.
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

    /// The directory's row count from every footer, for a dataset opened as one scan without
    /// listing its files; `Err` when no footer reads. Timed from before the walk (it must
    /// find the files again); only the first such pass counts (`Meter::counted_rows`).
    pub fn count_rows(
        &self,
        meter: &Meter,
        progress: &Arc<FooterProgress>,
    ) -> color_eyre::Result<usize> {
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
            .read_footers(&files, &every, progress, &Arc::new(Meter::default()))
            .unwrap_or_default();
        if progress.is_cancelled() {
            return Err(color_eyre::eyre::eyre!("The count was stopped"));
        }
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
        let stats = each_at_once(files.len(), FOOTERS_AT_ONCE, |i| {
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
            Some((meta.len(), modified, local_tag(&meta)))
        });
        let mut whole = true;
        for (file, stat) in files.iter_mut().zip(stats) {
            match stat {
                Some((size, stamp, tag)) => {
                    (file.size, file.stamp, file.etag) = (size, stamp, tag);
                }
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
        let footers = each_at_once(read.len(), progress.reads_at_once(), |i| {
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

    fn is_empty(&self, file: &DatasetFile) -> bool {
        std::fs::metadata(&file.key).is_ok_and(|m| m.len() == 0)
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

/// A local file's tag, like an ETag: its inode and inode change time, which catch a
/// same-second rewrite coarse mtimes miss. `None` where unavailable.
#[cfg(unix)]
fn local_tag(meta: &std::fs::Metadata) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    Some(format!(
        "{}:{}:{}.{}",
        meta.dev(),
        meta.ino(),
        meta.ctime(),
        meta.ctime_nsec()
    ))
}

#[cfg(not(unix))]
fn local_tag(_meta: &std::fs::Metadata) -> Option<String> {
    None
}

/// One local Parquet file's footer, with each column's width.
pub(crate) fn local_footer(path: &Path) -> Option<FileFooter> {
    use polars::prelude::{ParquetReader, Schema, SchemaExt, SerReader};
    crate::formats::schema_union::before_local_footer_read(path);
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

/// Read every directory under `root` to `max_depth`, a level at a time, many at once;
/// unclassified, so the classifying walk sees what a serial read would. With `listing`,
/// files are counted against it and an abandoned load stops the walk (its result
/// unused). With `enough`, stop after the level that exceeds it, reading at most
/// [`MAX_NAMES_PER_DIR`] entries per directory: a look, not a listing.
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
        let read = each_at_once(level.len(), FOOTERS_AT_ONCE, |i| {
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
                            && crate::home::discover::is_parquet_key(
                                &crate::home::discover::directory_and_name(path),
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

/// Entries read per directory by a looking walk; past it the sample covers only names
/// seen, and the row count is out of reach anyway.
const MAX_NAMES_PER_DIR: usize = 20_000;

/// `work` for each index below `n` on up to a wave of threads, answers in index order.
/// Sized for waiting on disk or network, not cores; a panicking worker answers `None`
/// for its indices.
fn each_at_once<T: Send>(
    n: usize,
    at_once: usize,
    work: impl Fn(usize) -> Option<T> + Sync,
) -> Vec<Option<T>> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let next = AtomicUsize::new(0);
    let workers = n.min(at_once.max(1));
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

/// The data files `walked` found under `dir` into `out`, counting what was passed over:
/// `_SUCCESS` is a writer's bookkeeping, but CSVs beside Parquet were likely meant as
/// data.
fn collect_data_files(
    walked: &WalkedDirs,
    dir: &Path,
    out: &mut Vec<PathBuf>,
    skipped: &mut crate::formats::schema_union::SkippedFiles,
    depth: usize,
    max_depth: usize,
    // Carried down: every file under a writer's directory (`_delta_log`) is the writer's,
    // which a `.json`'s own name cannot say.
    under_bookkeeping: bool,
) -> (bool, usize) {
    // Past the depth limit (sixty-four, unreachable in practice) a subtree reports no data.
    if depth >= max_depth {
        return (false, 0);
    }
    let Some(entries) = walked.get(dir) else {
        return (false, 0);
    };
    // Held until the directory is fully read: a stray is worth mentioning only beside data.
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
                .is_some_and(crate::home::discover::is_bookkeeping);
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
            // A partition with no data of its own hands its strays up (a day landed as CSV is part
            // of the dataset).
            passed_over += deferred;
        } else if !bookkeeping
            && crate::home::discover::is_parquet_key(&crate::home::discover::directory_and_name(
                &child,
            ))
        {
            // The cloud listing's test, so a directory is the same table anywhere. Given the
            // directory and name, not the whole path: part files in `occurrence.parquet/` are known
            // by the directory, and Windows backslashes would not split.
            here.push(child);
        } else if !bookkeeping {
            passed_over += 1;
        } else {
            skipped.count(true);
        }
    }
    let holds_data = data_below || !here.is_empty();
    out.append(&mut here);
    // A directory named with a partition key belongs to the dataset above; its strays go up
    // to be judged there.
    let partition = dir
        .file_name()
        .map(|n| n.to_string_lossy())
        .is_some_and(|n| n.contains('='));
    // Never at the top: there is nothing above the dataset's own directory to hand
    // them to, and the caller has nowhere to put them.
    if depth > 0 && !holds_data && partition && !under_bookkeeping {
        return (false, passed_over);
    }
    // With data beneath it, other files are beside someone's data; with none, the directory
    // is infrastructure and nothing in it was meant for the table.
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
    plan: crate::cloud::cloud_hive::ListShards,
    /// The prefix URL (a glob's part before the star): layout notes take paths relative to
    /// it, so a starred root would empty them all.
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
            root: crate::cloud::cloud_hive::url_of_key(full, &prefix)
                .unwrap_or_else(|| full.to_string()),
            plan: crate::cloud::cloud_hive::ListShards::for_url(full),
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
            crate::cloud::cloud_hive::list_dataset_files_reporting(
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
            crate::cloud::cloud_hive::footers_of_files_reporting(
                &store, &files, &read, &progress, &meter,
            )
            .await
        })
    }

    fn name_of(&self, file: &DatasetFile) -> Option<String> {
        crate::cloud::cloud_hive::url_of_key(&self.full, &file.key)
    }

    fn partition_path(&self, file: &DatasetFile) -> String {
        file.key.clone()
    }

    fn cloud_options(&self) -> Option<CloudOptions> {
        Some(self.cloud.clone())
    }

    fn remote_objects(&self, files: &[DatasetFile]) -> Vec<crate::cloud::local_copy::RemoteObject> {
        files
            .iter()
            .filter_map(|file| {
                Some(crate::cloud::local_copy::RemoteObject {
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

    /// The listing returned only the object the URL names (`--hive` on a single object,
    /// with or without a trailing slash).
    fn is_one_object(&self, files: &[DatasetFile]) -> bool {
        files.len() == 1 && self.full.trim_end_matches('/').ends_with(&files[0].key)
    }
}

#[cfg(test)]
mod tests;
