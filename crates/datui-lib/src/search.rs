//! Recursive search for datasets below a directory: one bounded background walk of the
//! working directory, its results filtered in memory like listed rows; nothing repeats
//! per keystroke. Every limit exists because some real directory needs it (see
//! `Limits`). The walk keeps every data file, scored off the UI thread ([`score`]); the
//! listing cap counts matches, so a match is never lost behind non-matches.

use crate::config::SearchConfig;
use crate::discover::{Entry, EntryKind, is_data_file};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The most files one walk keeps. The time budget bounds a walk first in practice; this
/// bounds its memory on a tree fast enough to list a million names inside it.
pub const MAX_INDEXED: usize = 100_000;

/// How far a walk got and why it stopped: "not found here" is acted on, so a short
/// search must say it was short.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Outcome {
    /// Directory entries examined, whether or not they were data.
    pub scanned: usize,
    /// Stopped at [`MAX_INDEXED`] files.
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
            Some("partial · too many files")
        } else if self.hit_depth_limit {
            Some("partial · too deep")
        } else {
            None
        }
    }
}

/// How often the walker reports: often enough that a cold tree fills the screen while
/// working, rarely enough that a warm one is not busy messaging.
const BATCH: usize = 64;
const BATCH_INTERVAL: Duration = Duration::from_millis(120);

/// Walk `root` for datasets, handing batches to `emit`; `emit` returns `false` to
/// abandon the walk. Blocks on the filesystem: never call from the drawing thread.
pub fn walk<F>(root: &Path, config: &SearchConfig, emit: F) -> Outcome
where
    F: FnMut(Vec<Entry>, Outcome) -> bool,
{
    walk_up_to(root, config, MAX_INDEXED, emit)
}

/// [`walk`], keeping the files `formats` reads as well, as the listing names them: by a
/// spec's glob, or by its magic in the first bytes of a file whose name says nothing,
/// at most `crate::discover::MAX_SNIFFS_PER_DIR` of them a directory.
pub fn walk_with_specs<F>(
    root: &Path,
    config: &SearchConfig,
    formats: &crate::formats::Registry,
    emit: F,
) -> Outcome
where
    F: FnMut(Vec<Entry>, Outcome) -> bool,
{
    walk_inner(root, config, MAX_INDEXED, formats, emit)
}

/// [`walk`], keeping at most `cap` files.
pub fn walk_up_to<F>(root: &Path, config: &SearchConfig, cap: usize, emit: F) -> Outcome
where
    F: FnMut(Vec<Entry>, Outcome) -> bool,
{
    walk_inner(
        root,
        config,
        cap,
        &crate::formats::Registry::default(),
        emit,
    )
}

fn walk_inner<F>(
    root: &Path,
    config: &SearchConfig,
    cap: usize,
    formats: &crate::formats::Registry,
    mut emit: F,
) -> Outcome
where
    F: FnMut(Vec<Entry>, Outcome) -> bool,
{
    let mut outcome = Outcome::default();
    if !config.enabled {
        return outcome;
    }

    let deadline = Instant::now() + config.time_budget.duration();
    let skip = config.skipped_dirs();
    let extensions: Vec<String> = config
        .extensions
        .iter()
        .map(|e| e.trim_start_matches('.').to_ascii_lowercase())
        .collect();

    let mut builder = ignore::WalkBuilder::new(root);
    builder
        // Hidden directories are skipped for the same reason the listing skips them,
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
    // Where the files live, for the row's storage glyph (#547 D10). One filesystem unless the walk
    // may cross into others, and then asked per file.
    let mounts = crate::locality::Mounts::cached();
    let root_source = mounts.describe(root).fstype;
    // Specs name files only when no extension filter narrows the search.
    let specs = extensions.is_empty() && !formats.is_empty();
    // The directory being walked and how many of its files have been looked inside.
    let mut sniffed_in: (PathBuf, usize) = (PathBuf::new(), 0);

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
        let mut spec = None;
        if !matches_extension(path, &extensions) {
            if !specs {
                continue;
            }
            spec = spec_of(path, formats, &mut sniffed_in);
            if spec.is_none() {
                continue;
            }
        }

        let mut entry = Entry::new(path.to_path_buf(), EntryKind::File);
        if let Some(spec) = spec {
            crate::discover::name_spec_file(&mut entry, &spec);
        }
        if let Ok(meta) = dir_entry.metadata() {
            entry = entry.with_fs_metadata(&meta);
        }
        entry.cost.source = Some(if config.cross_filesystems {
            mounts.describe(path).fstype
        } else {
            root_source.clone()
        });
        // The name carries the path relative to where the search started, because
        // "sales.parquet" three times over says nothing about which one you want.
        entry.name = relative_label(root, path);
        batch.push(entry);
        found += 1;

        if found >= cap {
            outcome.hit_result_limit = true;
            break;
        }

        // Either condition: a full batch bounds work between checkpoints; the interval covers
        // long stretches of non-matching entries, which still need progress and a way to stop.
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

/// The spec that reads `path`, a file no extension names, as the listing finds it: by
/// glob, else by its first bytes when its name says nothing, within the per-directory
/// cap `sniffed_in` counts.
fn spec_of(
    path: &Path,
    formats: &crate::formats::Registry,
    sniffed_in: &mut (PathBuf, usize),
) -> Option<Arc<crate::formats::Spec>> {
    if let Some(spec) = formats.by_glob(path, false).into_iter().next() {
        return Some(spec);
    }
    if !crate::discover::worth_sniffing(path) {
        return None;
    }
    let dir = path.parent().unwrap_or(path);
    if sniffed_in.0 != dir {
        *sniffed_in = (dir.to_path_buf(), 0);
    }
    if sniffed_in.1 >= crate::discover::MAX_SNIFFS_PER_DIR {
        return None;
    }
    sniffed_in.1 += 1;
    match crate::discover::sniff_listed(path, formats)? {
        crate::discover::Sniffed::Spec(spec) => Some(spec),
        crate::discover::Sniffed::Format => None,
    }
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

/// A label naming the dataset by its place under the search root, always with forward
/// slashes (a display choice; the row keeps its real `path`).
fn relative_label(root: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(root).unwrap_or(path);
    relative
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Where a search should start from where the user is; `None` without a working
/// directory or on a filesystem that must not be walked.
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

/// What the filter matched among the files a walk kept.
#[derive(Debug, Clone, Default)]
pub struct Matches {
    /// The filter these are matches for.
    pub query: String,
    /// How many files of the index were looked at: the first `upto`.
    pub upto: usize,
    /// Every match, by its place in the index, in index order. Kept whole so the next,
    /// longer query only has to look at these.
    pub ids: Vec<u32>,
    /// The best matches, best first, at most `max_results` of them: what is listed.
    pub top: Vec<Entry>,
    /// The score of each of `top`, so listing them does not score them again.
    pub scores: Vec<i32>,
}

impl Matches {
    /// Whether these can be narrowed to `query` rather than looked for again: every
    /// match of a longer query is a match of its prefix, for a subsequence of the name
    /// and for a substring of a column alike.
    pub fn narrows_to(&self, query: &str) -> bool {
        !self.query.is_empty() && query.to_lowercase().starts_with(&self.query.to_lowercase())
    }
}

impl Matches {
    /// Score files the walk found since, `start` being the first one's place in the
    /// index, and fold them in. Whether any of them is now among the best listed.
    pub fn extend(&mut self, files: &[Entry], start: usize, limit: usize) -> bool {
        let mut changed = false;
        for (i, entry) in files.iter().enumerate() {
            let Some(score) = crate::home::match_score(&self.query, entry) else {
                continue;
            };
            self.ids.push((start + i) as u32);
            // After every listed match that ranks above or level with it: the ones found
            // first win a tie, as in a whole scoring.
            let at = self
                .scores
                .iter()
                .zip(&self.top)
                .position(|(&s, e)| s < score || (s == score && e.name.len() > entry.name.len()))
                .unwrap_or(self.top.len());
            if at < limit {
                self.top.insert(at, entry.clone());
                self.scores.insert(at, score);
                self.top.truncate(limit);
                self.scores.truncate(limit);
                changed = true;
            }
        }
        self.upto = start + files.len();
        changed
    }
}

/// Score `query` against `index`, keeping the best `limit`. With `base` from a prefix
/// of `query`, only its matches and newer files are scored. Off the UI thread: on a large
/// tree this made every keystroke wait.
pub fn score(index: &[Arc<[Entry]>], query: &str, base: Option<&Matches>, limit: usize) -> Matches {
    let all: Vec<&Entry> = index.iter().flat_map(|batch| batch.iter()).collect();
    let base = base.filter(|b| b.narrows_to(query) && b.upto <= all.len());
    let candidates: Box<dyn Iterator<Item = usize>> = match base {
        Some(b) => Box::new(b.ids.iter().map(|&id| id as usize).chain(b.upto..all.len())),
        None => Box::new(0..all.len()),
    };
    let mut hits: Vec<(i32, usize)> = candidates
        .filter_map(|id| crate::home::match_score(query, all[id]).map(|s| (s, id)))
        .collect();
    let ids: Vec<u32> = hits.iter().map(|&(_, id)| id as u32).collect();
    // Best first; ties to the shorter name, as the listing ranks them.
    let order = |a: &(i32, usize), b: &(i32, usize)| {
        b.0.cmp(&a.0)
            .then_with(|| all[a.1].name.len().cmp(&all[b.1].name.len()))
            .then_with(|| a.1.cmp(&b.1))
    };
    if hits.len() > limit && limit > 0 {
        hits.select_nth_unstable_by(limit - 1, order);
        hits.truncate(limit);
    } else if limit == 0 {
        hits.clear();
    }
    hits.sort_unstable_by(order);
    Matches {
        query: query.to_string(),
        upto: all.len(),
        ids,
        scores: hits.iter().map(|&(score, _)| score).collect(),
        top: hits.into_iter().map(|(_, id)| all[id].clone()).collect(),
    }
}
