//! A local copy of a remote dataset's objects, for Data Quality's full scan.
//!
//! A full scan is several passes over its scope, and over an object store each pass
//! reads the objects again. Within a byte budget the run fetches each object once
//! into the cache directory and makes its passes over the copy instead. The copy is
//! a session snapshot: it is kept for later full scans of the same dataset until it
//! is released, the dataset is opened again or replaced, or datui exits. A copy
//! that did not finish is removed when the fetch stops.

use crate::analysis::sampling::ReadWatch;
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

/// A sweep leaves a copy younger than this alone, locked or not: until its lock is
/// taken, a live session's copy looks like a dead one's.
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
    // Closed before the directory goes: Windows will not remove a directory holding
    // an open file.
    _held: std::fs::File,
    dir: tempfile::TempDir,
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

    #[cfg(test)]
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
            _held: held,
            dir,
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
         quality_local_copy = 0 reads the source instead"
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
/// locked and stays, and so does one too new to judge.
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
        let old = |meta: std::io::Result<std::fs::Metadata>| {
            meta.and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > UNHELD_GRACE)
        };
        let orphaned = match std::fs::File::open(path.join(HELD)) {
            Ok(file) => {
                let free = file.try_lock_exclusive().is_ok();
                if free {
                    let _ = fs2::FileExt::unlock(&file);
                }
                free && old(file.metadata())
            }
            Err(_) => old(entry.metadata()),
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
            part => safe_component(part, cfg!(windows)),
        })
        .collect()
}

/// `part` as one file name. Windows refuses some characters in one: they are
/// percent-encoded, which Polars decodes in a hive value, so the partition reads as
/// the source's.
fn safe_component(part: &str, windows: bool) -> String {
    if !windows {
        return part.to_string();
    }
    part.chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*' => format!("%{:02X}", c as u32),
            c => c.to_string(),
        })
        .collect()
}

fn is_remote(path: &str) -> bool {
    crate::cloud::source::is_remote_url(Path::new(path))
}

/// Every path a plan's scans read.
#[cfg(test)]
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
            // Each copy is one named file, and an object key may hold `[` or `*`.
            unified_scan_args.glob = false;
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
mod tests;
