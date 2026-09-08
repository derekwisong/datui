//! Recursive search for datasets below a directory.
//!
//! The home screen's filter is a fuzzy match over rows that are already listed. This
//! module is what puts more rows in front of it: one bounded walk of the working
//! directory, run once in the background, whose result is then filtered in memory
//! like everything else. Nothing here is repeated per keystroke — a walk per
//! character is how a file finder becomes slow on exactly the trees where it matters.
//!
//! Every limit exists because some real directory violates it. See [`Limits`].

use crate::config::SearchConfig;
use crate::discover::{is_data_file, Entry, EntryKind};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How far a walk got, and why it stopped.
///
/// A search that quietly returned less than the truth would be worse than no search:
/// "not found here" is a thing people act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Outcome {
    /// Directory entries examined, whether or not they were data.
    pub scanned: usize,
    /// Stopped at `max_results`.
    pub hit_result_limit: bool,
    /// Stopped at `time_budget_ms`.
    pub hit_time_limit: bool,
    /// Stopped at `max_depth` somewhere; deeper datasets may exist.
    pub hit_depth_limit: bool,
}

impl Outcome {
    pub fn complete(&self) -> bool {
        !self.hit_result_limit && !self.hit_time_limit && !self.hit_depth_limit
    }

    /// A short phrase for the section heading, or `None` when the walk saw everything.
    pub fn note(&self) -> Option<&'static str> {
        if self.hit_time_limit {
            Some("partial · out of time")
        } else if self.hit_result_limit {
            Some("partial · too many")
        } else if self.hit_depth_limit {
            Some("partial · too deep")
        } else {
            None
        }
    }
}

/// How often the walker hands back what it has found so far.
///
/// Small enough that a cold tree fills the screen while it is still working, large
/// enough that a warm one does not spend its time sending messages.
const BATCH: usize = 64;
const BATCH_INTERVAL: Duration = Duration::from_millis(120);

/// Walk `root` for datasets, handing batches to `emit` as they are found.
///
/// `emit` returns `false` to abandon the walk — the caller has moved on, and there is
/// no reason to keep reading a filesystem for an answer nobody is waiting for.
///
/// This blocks and touches the filesystem, so it must never be called from the thread
/// drawing the screen.
pub fn walk<F>(root: &Path, config: &SearchConfig, mut emit: F) -> Outcome
where
    F: FnMut(Vec<Entry>, Outcome) -> bool,
{
    let mut outcome = Outcome::default();
    if !config.enabled {
        return outcome;
    }

    let deadline = Instant::now() + Duration::from_millis(config.time_budget_ms);
    let skip = config.skipped_dirs();
    let extensions: Vec<String> = config
        .extensions
        .iter()
        .map(|e| e.trim_start_matches('.').to_ascii_lowercase())
        .collect();

    let mut builder = ignore::WalkBuilder::new(root);
    builder
        // Hidden directories are skipped for the same reason `scan_dir` skips them,
        // and it does most of this module's work: `.git`, `.venv`, `.tox`, the caches.
        .hidden(true)
        // Off on purpose. See `SearchConfig::follow_gitignore`.
        .git_ignore(config.follow_gitignore)
        .git_global(config.follow_gitignore)
        .git_exclude(config.follow_gitignore)
        .ignore(config.follow_gitignore)
        .parents(config.follow_gitignore)
        // Without this, `.gitignore` is consulted only inside a git repository.
        // Someone who turned the option on meant the file, not the repository.
        .require_git(false)
        // A symlink can point at its own parent, or at a mount that is not answering.
        // Neither is worth the risk for a convenience feature.
        .follow_links(false)
        // The limit that matters most: it is what keeps a walk from wandering onto a
        // network share, and on autofs, from mounting one merely by looking.
        .same_file_system(!config.cross_filesystems)
        .max_depth(Some(config.max_depth))
        // One thread. The walk is bounded and usually finishes in milliseconds warm;
        // a thread pool competing with the load that opens a dataset is a worse trade
        // than the milliseconds it would save.
        .threads(1);

    if !skip.is_empty() {
        let mut over = ignore::overrides::OverrideBuilder::new(root);
        for name in &skip {
            // A leading `!` makes this an exclusion; matching both the bare name and
            // any depth catches `node_modules` wherever it appears.
            let _ = over.add(&format!("!**/{name}"));
            let _ = over.add(&format!("!{name}"));
        }
        if let Ok(over) = over.build() {
            builder.overrides(over);
        }
    }

    let mut batch: Vec<Entry> = Vec::with_capacity(BATCH);
    let mut found = 0usize;
    let mut last_emit = Instant::now();

    for result in builder.build() {
        outcome.scanned += 1;

        // Checked per entry rather than per batch: one enormous directory can burn
        // the whole budget without ever completing a batch.
        if Instant::now() >= deadline {
            outcome.hit_time_limit = true;
            break;
        }

        let Ok(dir_entry) = result else {
            // A directory that cannot be read is not an error worth reporting here —
            // permissions on someone else's tree are normal.
            continue;
        };

        if dir_entry.depth() >= config.max_depth {
            // Reaching the limit is only worth reporting if there was more below it.
            if dir_entry.file_type().is_some_and(|t| t.is_dir()) {
                outcome.hit_depth_limit = true;
            }
            continue;
        }

        let Some(file_type) = dir_entry.file_type() else {
            continue;
        };
        // Directories are traversed, not offered: a search result is something you
        // can open. Anything that is not a regular file — a FIFO, a socket, a device
        // — is never opened, which is the rule the rest of datui already follows.
        if !file_type.is_file() {
            continue;
        }

        let path = dir_entry.path();
        if !matches_extension(path, &extensions) {
            continue;
        }

        let mut entry = Entry::new(path.to_path_buf(), EntryKind::File);
        if let Ok(meta) = dir_entry.metadata() {
            entry = entry.with_fs_metadata(&meta);
        }
        // The name carries the path relative to where the search started, because
        // "sales.parquet" three times over says nothing about which one you want.
        entry.name = relative_label(root, path);
        batch.push(entry);
        found += 1;

        if found >= config.max_results {
            outcome.hit_result_limit = true;
            break;
        }

        // Either condition, not both. A full batch bounds the work done between
        // checkpoints; the interval covers the opposite case, a walk crossing
        // thousands of entries that match nothing, where the caller still wants a
        // progress count and still needs somewhere to say "stop".
        if batch.len() >= BATCH || last_emit.elapsed() >= BATCH_INTERVAL {
            last_emit = Instant::now();
            if !emit(std::mem::take(&mut batch), outcome) {
                return outcome;
            }
            batch.reserve(BATCH);
        }
    }

    emit(batch, outcome);
    outcome
}

/// Whether `path` is a format the search is looking for.
fn matches_extension(path: &Path, extensions: &[String]) -> bool {
    if extensions.is_empty() {
        return is_data_file(path);
    }
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .is_some_and(|e| extensions.contains(&e))
}

/// A label naming the dataset by where it sits under the search root.
fn relative_label(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// Where a search should start, given where the user is.
///
/// `None` when there is nothing sensible to search: no working directory, or one on a
/// filesystem that must not be walked.
pub fn search_root(
    browsing: Option<&PathBuf>,
    network_check: fn(&Path) -> bool,
) -> Option<PathBuf> {
    let root = match browsing {
        Some(dir) => dir.clone(),
        None => std::env::current_dir().ok()?,
    };
    // A remote root is listed by its probe, one directory at a time, precisely so that
    // a share which stops answering cannot take the interface with it. Recursively
    // walking one would undo that.
    if network_check(&root) {
        return None;
    }
    Some(root)
}
