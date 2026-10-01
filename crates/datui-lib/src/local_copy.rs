//! A local copy of a remote dataset's objects, for Data Quality's full scan.
//!
//! A full scan is several passes over its scope, and over an object store each pass
//! reads the objects again. Within a byte budget the run fetches each object once
//! into the cache directory and makes its passes over the copy instead. The copy is
//! a session snapshot: it is kept for later full scans of the same dataset until it
//! is released, the dataset is opened again or replaced, or datui exits. A copy
//! that did not finish is removed when the fetch stops.

use crate::sampling::ReadWatch;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::lazy::dsl::{DslPlan, ScanSources};
use polars::prelude::{LazyFrame, PlRefPath};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Where copies live under the cache directory.
pub const COPIES_DIR: &str = "quality-copies";

/// Held, locked, by the session that owns a copy; [`sweep`] leaves a locked copy alone.
const HELD: &str = ".held";

/// A copy whose lock file is missing is left this long before a sweep removes it: the
/// window between creating its directory and taking its lock.
const UNHELD_GRACE: std::time::Duration = std::time::Duration::from_secs(60);

/// One remote object a copy fetches, as the open found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteObject {
    pub url: String,
    pub size: u64,
    /// The store's tag for the version listed, where it gave one.
    pub etag: Option<String>,
}

/// Objects copied from a remote dataset, each at the path its URL maps to. Removed
/// from disk when the last holder drops it: the app's retained copies and any run
/// reading it.
#[derive(Debug)]
pub struct LocalCopy {
    dir: tempfile::TempDir,
    _held: std::fs::File,
    paths: HashMap<String, PathBuf>,
    bytes: u64,
}

impl LocalCopy {
    /// Bytes on disk.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Objects copied.
    pub fn objects(&self) -> usize {
        self.paths.len()
    }

    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// Whether this copy holds the object at `url`.
    pub fn covers(&self, url: &str) -> bool {
        self.paths.contains_key(url)
    }

    /// Copy `objects` under `root`, one at a time. `get` streams one object's bytes
    /// into the writer it is given, in order; the writer refuses once `stop` is set,
    /// so a cancel ends the fetch within a chunk. On any failure or cancel the partial
    /// copy is removed before this returns.
    ///
    /// An object whose bytes differ in length from its listing changed since the
    /// dataset opened: the copy would not be the dataset on screen, so it fails.
    pub fn fetch(
        root: &Path,
        objects: &[RemoteObject],
        stop: &ReadWatch,
        mut get: impl FnMut(&RemoteObject, &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()>,
    ) -> Result<LocalCopy> {
        std::fs::create_dir_all(root).map_err(unwritable)?;
        sweep(root);
        let dir = tempfile::Builder::new()
            .prefix("copy-")
            .tempdir_in(root)
            .map_err(unwritable)?;
        let held = hold(dir.path())?;
        let mut copy = LocalCopy {
            dir,
            _held: held,
            paths: HashMap::with_capacity(objects.len()),
            bytes: 0,
        };
        for object in objects {
            stop.check()?;
            let path = copy.dir.path().join(relative_path(&object.url));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(unwritable)?;
            }
            // Never over an object already copied: on a disk that ignores case, two
            // keys can name one file.
            let mut file = std::fs::File::create_new(&path).map_err(unwritable)?;
            let mut written = 0u64;
            get(object, &mut |chunk: &[u8]| {
                stop.check()?;
                file.write_all(chunk).map_err(unwritable)?;
                written += chunk.len() as u64;
                Ok(())
            })?;
            file.flush().map_err(unwritable)?;
            if written != object.size {
                return Err(eyre!(
                    "{} is {written} bytes, not the {} listed when it opened: \
                     it changed. Open the dataset again",
                    object.url,
                    object.size
                ));
            }
            copy.bytes += written;
            copy.paths.insert(object.url.clone(), path);
        }
        Ok(copy)
    }

    /// `lf` reading this copy wherever it read a remote object. `None` when it reads
    /// a remote object the copy does not hold, or a plan shape this cannot follow:
    /// the caller then reads the source as before.
    pub fn redirect(&self, lf: &LazyFrame) -> Option<LazyFrame> {
        let mut plan = lf.logical_plan.clone();
        if !redirect_plan(&mut plan, &self.paths) || reads_remote(&plan) {
            return None;
        }
        Some(LazyFrame::from(plan).with_optimizations(lf.get_current_optimizations()))
    }
}

/// A local write that failed, and the way around it.
fn unwritable(error: std::io::Error) -> color_eyre::Report {
    eyre!(
        "Could not write the local copy: {error}. \
         quality_local_copy_mb = 0 reads the source instead"
    )
}

/// Whether the tag a fetch was answered with is the one listed: stores quote it in
/// one answer and not the other, and mark a weak one.
pub fn same_etag(listed: &str, fetched: &str) -> bool {
    let bare = |tag: &str| {
        tag.trim()
            .trim_start_matches("W/")
            .trim_matches('"')
            .to_string()
    };
    bare(listed) == bare(fetched)
}

/// Lock `dir` as held by this session.
fn hold(dir: &Path) -> Result<std::fs::File> {
    use fs2::FileExt;
    let file = std::fs::File::create(dir.join(HELD)).map_err(unwritable)?;
    file.try_lock_exclusive().map_err(unwritable)?;
    Ok(file)
}

/// Remove copies under `root` no session holds: left by a datui that did not exit
/// cleanly, or quit while a run read one. A copy another running datui holds is
/// locked and stays.
pub fn sweep(root: &Path) {
    use fs2::FileExt;
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let ours = path.is_dir()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("copy-"));
        if !ours {
            continue;
        }
        let orphaned = match std::fs::File::open(path.join(HELD)) {
            Ok(file) => {
                let free = file.try_lock_exclusive().is_ok();
                if free {
                    let _ = fs2::FileExt::unlock(&file);
                }
                free
            }
            Err(_) => entry
                .metadata()
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > UNHELD_GRACE),
        };
        if orphaned {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

/// Free bytes on the file system holding `dir`, or its nearest existing parent.
pub fn free_space(dir: &Path) -> Option<u64> {
    let existing = dir.ancestors().find(|path| path.exists())?;
    fs2::available_space(existing).ok()
}

/// Where `url` lands inside a copy: its bucket and key as directories, so a hive
/// path keeps its `key=value` parts and Polars reads the same partition columns.
/// No part climbs out of the copy.
fn relative_path(url: &str) -> PathBuf {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.split('/')
        .filter(|part| !part.is_empty())
        .map(|part| match part {
            "." | ".." => "_".to_string(),
            part => safe_component(part),
        })
        .collect()
}

#[cfg(windows)]
fn safe_component(part: &str) -> String {
    part.chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*' => '_',
            c => c,
        })
        .collect()
}

#[cfg(not(windows))]
fn safe_component(part: &str) -> String {
    part.to_string()
}

fn is_remote(path: &str) -> bool {
    crate::source::is_remote_url(Path::new(path))
}

/// Every path a plan's scans read.
pub fn scan_paths(lf: &LazyFrame) -> Vec<String> {
    let mut paths = Vec::new();
    for node in &lf.logical_plan {
        if let DslPlan::Scan {
            sources: ScanSources::Paths(sources),
            ..
        } = node
        {
            paths.extend(sources.iter().map(|path| path.as_str().to_string()));
        }
    }
    paths
}

fn reads_remote(plan: &DslPlan) -> bool {
    plan.into_iter().any(|node| match node {
        DslPlan::Scan {
            sources: ScanSources::Paths(sources),
            ..
        } => sources.iter().any(|path| is_remote(path.as_str())),
        _ => false,
    })
}

/// Point every scan of a remote object in `plan` at its copy in `paths`. False when a
/// scan reads a remote object `paths` does not hold.
fn redirect_plan(plan: &mut DslPlan, paths: &HashMap<String, PathBuf>) -> bool {
    let into = |input: &mut Arc<DslPlan>| redirect_plan(Arc::make_mut(input), paths);
    let each = |inputs: &mut [DslPlan]| inputs.iter_mut().all(|input| redirect_plan(input, paths));
    match plan {
        DslPlan::Scan {
            sources,
            unified_scan_args,
            cached_ir,
            ..
        } => {
            let ScanSources::Paths(urls) = sources else {
                return true;
            };
            if !urls.iter().any(|url| is_remote(url.as_str())) {
                return true;
            }
            let mut local = Vec::with_capacity(urls.len());
            for url in urls.iter() {
                let Some(path) = paths.get(url.as_str()).and_then(|path| path.to_str()) else {
                    return false;
                };
                local.push(PlRefPath::new(path));
            }
            *sources = ScanSources::Paths(local.into_iter().collect());
            unified_scan_args.cloud_options = None;
            // The IR cached for the remote paths would be reused as is.
            *cached_ir = Default::default();
            true
        }
        // A plan already turned into IR keeps the plan it came from: rewrite that, and
        // leave the IR behind.
        DslPlan::IR { dsl, .. } => {
            let mut inner = Arc::unwrap_or_clone(dsl.clone());
            let ok = redirect_plan(&mut inner, paths);
            *plan = inner;
            ok
        }
        DslPlan::Select { input, .. }
        | DslPlan::GroupBy { input, .. }
        | DslPlan::Filter { input, .. }
        | DslPlan::Distinct { input, .. }
        | DslPlan::Sort { input, .. }
        | DslPlan::Slice { input, .. }
        | DslPlan::HStack { input, .. }
        | DslPlan::MatchToSchema { input, .. }
        | DslPlan::MapFunction { input, .. }
        | DslPlan::Sink { input, .. }
        | DslPlan::Cache { input, .. }
        | DslPlan::Pivot { input, .. } => into(input),
        DslPlan::Union { inputs, .. }
        | DslPlan::HConcat { inputs, .. }
        | DslPlan::SinkMultiple { inputs } => each(inputs),
        DslPlan::PipeWithSchema { input, .. } => {
            let mut inputs = input.to_vec();
            let ok = each(&mut inputs);
            *input = inputs.into();
            ok
        }
        DslPlan::Join {
            input_left,
            input_right,
            ..
        } => into(input_left) & into(input_right),
        DslPlan::Gather { input, idxs, .. } => into(input) & into(idxs),
        DslPlan::ExtContext { input, contexts } => into(input) & each(contexts),
        // In-memory frames read nothing; any other shape is checked by `reads_remote`
        // after, and a remote scan left inside it sends the run back to the source.
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    /// Two Parquet files under `dir`, rows 0..10 and 10..20, in a `part=` directory
    /// each so the copy has to keep hive paths working.
    fn files(dir: &Path) -> Vec<RemoteObject> {
        (0..2)
            .map(|part| {
                let mut df =
                    df!("id" => ((part * 10)..(part * 10 + 10)).collect::<Vec<i64>>()).unwrap();
                let path = dir.join(format!("part={part}")).join("data.parquet");
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                ParquetWriter::new(std::fs::File::create(&path).unwrap())
                    .finish(&mut df)
                    .unwrap();
                let size = std::fs::metadata(&path).unwrap().len();
                RemoteObject {
                    url: format!("s3://lake/events/part={part}/data.parquet"),
                    size,
                    etag: None,
                }
            })
            .collect()
    }

    /// The URL's bytes from the "bucket" under `source`.
    fn bytes_of(source: &Path, url: &str) -> Vec<u8> {
        let key = url.trim_start_matches("s3://lake/events/");
        std::fs::read(source.join(key)).unwrap()
    }

    fn remote_scan(urls: &[String]) -> LazyFrame {
        let sources = ScanSources::Paths(urls.iter().map(PlRefPath::new).collect());
        let args = polars::lazy::dsl::UnifiedScanArgs {
            hive_options: polars::io::HiveOptions::new_enabled(),
            ..Default::default()
        };
        DslBuilder::scan_parquet(sources, Default::default(), args)
            .unwrap()
            .build()
            .into()
    }

    /// A fetched copy serves a remote plan, filters and aggregates included, with the
    /// same rows and the hive column; no remote path is left in the plan.
    #[test]
    fn a_copy_reads_as_the_remote_scan_would() {
        let source = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let objects = files(source.path());
        let copy = LocalCopy::fetch(
            root.path(),
            &objects,
            &ReadWatch::default(),
            |object, write| {
                for chunk in bytes_of(source.path(), &object.url).chunks(7) {
                    write(chunk)?;
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(copy.objects(), 2);
        assert_eq!(
            copy.bytes(),
            objects.iter().map(|object| object.size).sum::<u64>()
        );

        let urls = objects
            .iter()
            .map(|object| object.url.clone())
            .collect::<Vec<_>>();
        let remote = remote_scan(&urls)
            .filter(col("id").gt(lit(3)))
            .group_by([col("part")])
            .agg([len().alias("rows")])
            .sort(["part"], Default::default());
        // A frame asked for its schema holds its plan as IR, wrapping the plan it
        // came from.
        let remote: LazyFrame = DslPlan::IR {
            dsl: Arc::new(remote.logical_plan),
            version: 0,
            node: None,
            opt_flags: None,
        }
        .into();
        let local = copy.redirect(&remote).expect("every object is in the copy");
        assert!(scan_paths(&local).iter().all(|path| !is_remote(path)));
        let df = local.collect().unwrap();
        assert_eq!(df.column("rows").unwrap().u32().unwrap().get(0), Some(6));
        assert_eq!(df.column("rows").unwrap().u32().unwrap().get(1), Some(10));
        assert_eq!(df.height(), 2, "the partition column survives the copy");
    }

    #[test]
    fn a_plan_reading_an_object_not_copied_is_left_alone() {
        let source = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let objects = files(source.path());
        let copy = LocalCopy::fetch(
            root.path(),
            &objects[..1],
            &ReadWatch::default(),
            |object, write| write(&bytes_of(source.path(), &object.url)),
        )
        .unwrap();
        let urls = objects
            .iter()
            .map(|object| object.url.clone())
            .collect::<Vec<_>>();
        assert!(copy.redirect(&remote_scan(&urls)).is_none());
    }

    /// A cancel between chunks and a failed object both leave nothing behind.
    #[test]
    fn a_stopped_or_failed_fetch_leaves_no_files() {
        let source = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let objects = files(source.path());
        let entries = || std::fs::read_dir(root.path()).unwrap().count();

        let stop = ReadWatch::default();
        let stopped = LocalCopy::fetch(root.path(), &objects, &stop, |object, write| {
            let bytes = bytes_of(source.path(), &object.url);
            write(&bytes[..10])?;
            stop.stop();
            write(&bytes[10..])
        });
        assert!(stopped.is_err());
        assert_eq!(entries(), 0, "the partial copy is gone");

        let failed = LocalCopy::fetch(
            root.path(),
            &objects,
            &ReadWatch::default(),
            |object, write| {
                if object.url.contains("part=1") {
                    return Err(eyre!("404"));
                }
                write(&bytes_of(source.path(), &object.url))
            },
        );
        assert!(failed.is_err());
        assert_eq!(entries(), 0, "the first object went with it");

        let short = LocalCopy::fetch(
            root.path(),
            &objects,
            &ReadWatch::default(),
            |object, write| write(&bytes_of(source.path(), &object.url)[1..]),
        );
        assert!(short.unwrap_err().to_string().contains("changed"));
        assert_eq!(entries(), 0);
    }

    /// A finished copy stays while held and goes when dropped; a sweep removes a
    /// copy no one holds and keeps one that is held.
    #[test]
    fn a_copy_lives_while_held_and_a_sweep_clears_orphans() {
        let source = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let objects = files(source.path());
        let fetch = || {
            LocalCopy::fetch(
                root.path(),
                &objects,
                &ReadWatch::default(),
                |object, write| write(&bytes_of(source.path(), &object.url)),
            )
            .unwrap()
        };
        let held = fetch();
        let dir = held.dir().to_path_buf();
        // A copy left by a session that died: its lock is released with the process.
        let orphan = root.path().join("copy-orphan");
        std::fs::create_dir_all(orphan.join("lake")).unwrap();
        std::fs::write(orphan.join(HELD), b"").unwrap();
        sweep(root.path());
        assert!(dir.exists(), "a held copy stays");
        assert!(!orphan.exists(), "an orphan goes");
        drop(held);
        assert!(!dir.exists(), "dropped, the copy is removed");
    }

    /// Two keys that land on one file, as on a disk that ignores case, fail the
    /// fetch rather than one overwriting the other.
    #[test]
    fn two_objects_never_share_a_file() {
        let source = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let mut objects = files(source.path());
        objects[1].url = objects[0].url.replace("s3://lake/", "s3://lake//");
        let error = LocalCopy::fetch(
            root.path(),
            &objects,
            &ReadWatch::default(),
            // Asked only for the first: the second has nowhere to go.
            |object, write| write(&bytes_of(source.path(), &object.url)),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("quality_local_copy_mb = 0"),
            "{error}"
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn urls_map_to_their_bucket_and_key() {
        assert_eq!(
            relative_path("s3://lake/events/region=North/part-0.parquet"),
            PathBuf::from("lake/events/region=North/part-0.parquet")
        );
        assert_eq!(
            relative_path("gs://b/../x.parquet"),
            PathBuf::from("b/_/x.parquet")
        );
        assert_eq!(
            relative_path("s3:///../../etc/passwd"),
            PathBuf::from("_/_/etc/passwd")
        );
    }

    #[test]
    fn etags_compare_unquoted() {
        assert!(same_etag("\"abc\"", "abc"));
        assert!(same_etag("W/\"abc\"", "\"abc\""));
        assert!(!same_etag("\"abc\"", "\"abd\""));
    }
}
