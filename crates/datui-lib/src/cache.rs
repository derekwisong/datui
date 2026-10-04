use crate::logging::LogFailure;
use color_eyre::Result;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

mod store;
pub(crate) use store::{Kind, Store};
pub use store::{StableHasher, stable_hash};

/// Manages cache directory and cache file operations
#[derive(Clone, Debug)]
pub struct CacheManager {
    pub(crate) cache_dir: PathBuf,
}

impl CacheManager {
    /// Create a new CacheManager for the given app name
    /// Create a CacheManager rooted at an explicit directory (primarily for testing).
    pub fn with_dir(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    /// Create a CacheManager for the given app name.
    ///
    /// `DATUI_CACHE_DIR` overrides the location. The test suite sets it, because
    /// opening a dataset records it as recent — without the override a test run
    /// writes its fixtures into the developer's own recent-files list.
    pub fn new(app_name: &str) -> Result<Self> {
        #[cfg(test)]
        isolate_cache();
        if let Some(dir) = std::env::var_os("DATUI_CACHE_DIR") {
            return Ok(Self {
                cache_dir: PathBuf::from(dir),
            });
        }
        // A test that reaches the real cache writes its fixtures into the developer's
        // own recents, and a dozen `/tmp/.tmp*` paths were found there. Refusing here
        // is what makes it impossible to do by accident: every test binary cargo
        // builds lives under `target/<profile>/deps/`, and the binary someone runs
        // never does.
        if running_as_a_cargo_test() {
            panic!(
                "DATUI_CACHE_DIR is not set: a test would write to the real cache. \
                 Call common::isolate_cache() (or take the runtime from \
                 common::test_runtime(), which does) before building an App or a \
                 CacheManager."
            );
        }

        let cache_dir = dirs::cache_dir()
            .ok_or_else(|| color_eyre::eyre::eyre!("Could not determine cache directory"))?
            .join(app_name);

        Ok(Self { cache_dir })
    }

    /// Get the cache directory path
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Get path to a specific cache file
    pub fn cache_file(&self, filename: &str) -> PathBuf {
        self.cache_dir.join(filename)
    }

    /// Ensure the cache directory exists
    pub fn ensure_cache_dir(&self) -> Result<()> {
        if !self.cache_dir.exists() {
            fs::create_dir_all(&self.cache_dir)?;
        }
        Ok(())
    }

    /// Clear a specific cache file
    pub fn clear_file(&self, filename: &str) -> Result<()> {
        let file_path = self.cache_file(filename);
        if file_path.exists() {
            fs::remove_file(&file_path)?;
        }
        Ok(())
    }

    /// Remove every cache kind's directory and every line-file list (recents,
    /// histories, hidden sources, remembered places).
    ///
    /// Nothing else: lock files may be held by another instance, the log is not a
    /// cache, and Data Quality's copies belong to the sessions holding them. Views live
    /// in the config directory.
    pub fn clear_all(&self) -> Result<()> {
        for dir in [Shapes::DIR, Facts::DIR, CloudListings::DIR] {
            match fs::remove_dir_all(self.cache_file(dir)) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    log::warn!(target: "datui", "remove the {dir} cache: {e}");
                }
                _ => {}
            }
        }
        let Ok(entries) = fs::read_dir(&self.cache_dir) else {
            return Ok(());
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // The list, and any temp file a writer of it left.
            let list = entry.file_name().to_string_lossy().contains(HISTORY_SUFFIX);
            if list && path.is_file() {
                fs::remove_file(&path).or_log(&format!("remove {}", path.display()));
            }
        }
        Ok(())
    }

    /// Load history from a history file.
    ///
    /// A file that cannot be read is an error, never an empty list: an empty list
    /// would be written back by the next push, and every entry would be gone. A line
    /// that is not UTF-8 is skipped alone, so one bad byte costs one entry.
    pub fn load_history_file(&self, history_id: &str) -> Result<Vec<String>> {
        let history_file = self.cache_file(&format!("{history_id}{HISTORY_SUFFIX}"));

        let bytes = match fs::read(&history_file) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut history = Vec::new();
        for line in bytes.split(|&b| b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            match std::str::from_utf8(line) {
                Ok(line) if !line.trim().is_empty() => history.push(line.to_string()),
                Ok(_) => {}
                Err(e) => {
                    log::warn!(target: "datui", "{history_id} history: skipped a line: {e}")
                }
            }
        }

        Ok(history)
    }

    /// A history file, empty when it is missing or cannot be read; the latter is logged.
    fn load_history_or_log(&self, history_id: &str) -> Vec<String> {
        self.load_history_file(history_id)
            .inspect_err(|e| log::warn!(target: "datui", "read {history_id} history: {e:#}"))
            .unwrap_or_default()
    }

    /// Apply `update` to a history file, with the whole read-modify-write held under
    /// an exclusive lock.
    ///
    /// Writing atomically stops two instances producing a *corrupt* file, but not a
    /// lost one: both read `[x, y]`, one writes `[a, x, y]` and the other
    /// `[b, x, y]`, and whichever lands second wins outright. Opening two datasets at
    /// once is ordinary — a launcher, a file manager, two terminals — so the read and
    /// the write have to be one operation.
    ///
    /// The lock is waited for, but only briefly. The critical section is reading and
    /// rewriting a fifty-line file, so even a dozen contending instances clear in a
    /// few milliseconds; a deadline well beyond that loses nothing in practice while
    /// still guaranteeing an interactive action is never held up by a peer that has
    /// wedged. Past the deadline the update is dropped — history is a convenience,
    /// and it is never worth delaying what the user actually asked for.
    ///
    /// Giving up after a couple of quick attempts is *not* enough: with several opens
    /// landing together, some are then dropped, which is the very loss this exists to
    /// prevent.
    pub fn update_history_file<F>(&self, history_id: &str, update: F) -> Result<HistoryUpdate>
    where
        F: FnOnce(&mut Vec<String>),
    {
        use fs2::FileExt;

        self.ensure_cache_dir()?;
        let lock_path = self.cache_file(&format!("{}_history.lock", history_id));
        let Some(lock) = lock_file(&lock_path, LOCK_TIMEOUT)? else {
            log::info!(target: "datui", "{history_id} history not updated: its lock is busy");
            return Ok(HistoryUpdate::SkippedBusy);
        };

        // Read, modify and write all inside the lock; the whole point is that another
        // instance cannot land between the read and the write. A file that cannot be
        // read is left as it is: rewriting it from nothing would lose every entry.
        let mut entries = self.load_history_file(history_id)?;
        update(&mut entries);
        let result = self.save_history_file(history_id, &entries);

        // Released explicitly, though dropping the file would do it too.
        let _ = FileExt::unlock(&lock);
        result.map(|()| HistoryUpdate::Written)
    }

    /// Save history to a history file
    pub fn save_history_file(&self, history_id: &str, history: &[String]) -> Result<()> {
        self.ensure_cache_dir()?;
        let history_file = self.cache_file(&format!("{history_id}{HISTORY_SUFFIX}"));

        // Oldest first, but we keep the most recent entries.
        let mut text = String::new();
        for entry in history {
            text.push_str(entry);
            text.push('\n');
        }
        // Truncating in place leaves the file readable half-written, and two instances
        // writing at once interleave into one corrupt file.
        atomic_write(&history_file, text.as_bytes())?;
        Ok(())
    }
}

/// What a line-file list's name ends in: `recents_history.txt`.
const HISTORY_SUFFIX: &str = "_history.txt";

/// Write `bytes` to `path` through a sibling temp file, synced, then renamed over it,
/// so a reader, another instance or a crash mid-write sees the old file or the new
/// one, never part of either. Every write into the cache, and every view, goes
/// through here.
///
/// The temp name is unique to the write: the process id, then a counter shared by
/// every thread of the process. It ends in `.tmp`, which nothing reads and a cache
/// sweep removes once it is stale.
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let serial = SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(format!(".{}.{serial}.tmp", std::process::id()));
    let temp = path.with_file_name(name);
    let written = (|| {
        let mut file = fs::File::create(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)
    })();
    if written.is_err() {
        let _ = fs::remove_file(&temp);
    }
    written
}

/// Take an exclusive lock on the file at `path` (created if missing), waiting up to
/// `timeout` for another instance to let it go. `None` when it stayed busy; the lock
/// is held until the returned file is dropped.
pub(crate) fn lock_file(
    path: &Path,
    timeout: std::time::Duration,
) -> std::io::Result<Option<fs::File>> {
    take_lock(path, timeout, false)
}

/// [`lock_file`], shared: readers hold it together, and never while a writer does.
pub(crate) fn lock_file_shared(
    path: &Path,
    timeout: std::time::Duration,
) -> std::io::Result<Option<fs::File>> {
    take_lock(path, timeout, true)
}

fn take_lock(
    path: &Path,
    timeout: std::time::Duration,
    shared: bool,
) -> std::io::Result<Option<fs::File>> {
    use fs2::FileExt;

    let lock = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let taken = if shared {
            FileExt::try_lock_shared(&lock)
        } else {
            FileExt::try_lock_exclusive(&lock)
        };
        if taken.is_ok() {
            return Ok(Some(lock));
        }
        if std::time::Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

/// How long to wait for another instance to finish rewriting a history file.
///
/// The critical section is a read and an atomic rewrite of a small file, so under any
/// realistic number of concurrent datui instances this is never approached. It exists
/// to bound the wait if a peer wedges, not to be reached.
///
/// It was 250ms, which is ample on Linux and not on Windows: sixteen writers
/// contending, each paying a slower `LockFileEx` and a slower rename, serialised past
/// the deadline and the last one gave up. Giving up means silently dropping someone's
/// entry, so the deadline has to clear realistic contention by a wide margin. Nothing
/// waits on this write -- see `push_recent`'s caller -- so a longer bound costs no
/// latency anywhere.
const LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Maximum number of recently opened paths kept. Enough to span a few days of work;
/// small enough that the home screen never has to paginate it.
pub const MAX_RECENTS: usize = 50;

/// Whether a history update actually happened.
///
/// A contended update is abandoned rather than waited on, because nothing should
/// delay what the user asked for in order to record that they asked for it. That
/// is the right trade, but it means "no error" and "it was written" are different
/// claims, and for a long time the API could only make the weaker one.
///
/// Saying which happened is worth the extra type. A caller that cares can retry
/// or report, and a test can assert something exact instead of hoping the
/// scheduler was kind: every update that reported `Written` is in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryUpdate {
    /// The lock was taken and the new contents are on disk.
    Written,
    /// The lock stayed busy past the deadline, so nothing was written.
    SkippedBusy,
}

impl CacheManager {
    /// Recently opened dataset paths, most recent first.
    ///
    /// This is the *only* thing datui remembers about your data between runs. It is
    /// a convenience, not a record: deleting it loses nothing but ordering.
    pub fn load_recents(&self) -> Vec<std::path::PathBuf> {
        self.load_recents_with_visits().0
    }

    /// Recents, most recent first, and how often and how lately each was opened, by
    /// its path as recorded: one read of the list.
    pub fn load_recents_with_visits(
        &self,
    ) -> (Vec<PathBuf>, std::collections::HashMap<PathBuf, Visits>) {
        let mut recents = Vec::new();
        let mut visits = std::collections::HashMap::new();
        for line in self.load_history_or_log("recents") {
            let (path, seen) = parse_recent(&line);
            recents.push(PathBuf::from(path));
            if seen.count > 0 {
                visits.insert(PathBuf::from(path), seen);
            }
        }
        (recents, visits)
    }

    /// Which home-screen sections the user folded or opened, by title.
    ///
    /// One line per section, `title<TAB>1` for folded and `title<TAB>0` for opened.
    /// A section not listed takes its own default.
    pub fn load_folds(&self) -> std::collections::HashMap<String, bool> {
        self.load_history_or_log("home_folds")
            .into_iter()
            .filter_map(|line| {
                let (title, state) = line.rsplit_once('\t')?;
                Some((title.to_string(), state.trim() == "1"))
            })
            .collect()
    }

    /// Remember the fold state. Failing to write it loses nothing but a preference.
    pub fn save_folds(&self, folds: &std::collections::HashMap<String, bool>) {
        let mut lines: Vec<String> = folds
            .iter()
            .map(|(title, folded)| format!("{title}\t{}", if *folded { 1 } else { 0 }))
            .collect();
        lines.sort();
        self.save_history_file("home_folds", &lines)
            .or_log("save home folds");
    }

    /// Forget a single recently opened path.
    ///
    /// A recents list you cannot edit is one people stop trusting: an experiment, a
    /// file that would not open, something private — all land there, and clearing the
    /// whole cache to remove one is too blunt.
    pub fn forget_recent(&self, path: &std::path::Path) {
        let target = path.to_string_lossy().into_owned();
        self.update_history_file("recents", |recents| {
            recents.retain(|line| parse_recent(line).0 != target);
        })
        .or_log("forget a recent");
    }

    /// Forget several recently opened paths at once: every recent under one place.
    pub fn forget_recents(&self, paths: &[std::path::PathBuf]) {
        let targets: Vec<String> = paths
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        self.update_history_file("recents", |recents| {
            recents.retain(|line| !targets.iter().any(|t| t == parse_recent(line).0));
        })
        .or_log("forget recents");
    }

    /// Forget every recently opened path, leaving other caches alone.
    pub fn clear_recents(&self) {
        self.update_history_file("recents", |recents| recents.clear())
            .or_log("clear recents");
    }

    /// Whether a recorded path is still worth offering.
    ///
    /// Recents are written on open and read on the home screen, and nothing used to take
    /// entries out again except the fifty-entry cap. A dataset in a directory that has
    /// since been deleted therefore stayed in the file, and while the home screen does
    /// not list the entry itself, it does derive a *root* from the directory — which
    /// then sits there marked `unavailable` with nothing in it, until fifty more opens
    /// push it off the end.
    ///
    /// Two deliberate narrownesses:
    ///
    /// The test is on the containing directory, not the file. A file that is gone from a
    /// directory that is still there is an ordinary deletion, and forgetting it the
    /// moment it disappears would be wrong for anything regenerated in place — a nightly
    /// export, a file being rewritten as datui looks at it. Only a directory that has
    /// gone entirely takes its contents with it.
    ///
    /// Remote paths are never checked at all. `exists` stats the path, and on an
    /// object-store URL or a share that has stopped answering that is the call that
    /// hangs. A share being down is also precisely when its recents matter most, so
    /// pruning them would throw away the list exactly when it is needed.
    fn recent_is_worth_keeping(path: &str, mounts: &crate::locality::Mounts) -> bool {
        let path = std::path::Path::new(path);
        // The mount table is passed in rather than read here. `is_remote_path` reads
        // /proc/self/mountinfo every time it is called, and this runs once per entry, so
        // asking it directly meant fifty reads of the same file on every open.
        if crate::locality::object_scheme(path).is_some() || mounts.is_network(path) {
            return true;
        }
        match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.exists(),
            _ => true,
        }
    }

    /// Record a path as most recently opened, de-duplicating and capping the list.
    ///
    /// Failures are ignored: not being able to write a convenience list must never
    /// interfere with opening data. The return value distinguishes a write from a
    /// contended skip for callers and tests that care; the normal caller does not.
    pub fn push_recent(&self, path: &std::path::Path) -> HistoryUpdate {
        // A URL is recorded exactly as given: canonicalising one is meaningless, and
        // it would also stat a path that does not exist locally.
        let looks_like_url = path.to_string_lossy().contains("://");
        let stored = if looks_like_url {
            path.to_path_buf()
        } else {
            // A table inside a file of tables is no file of its own: its file is made
            // absolute and the name kept.
            crate::canonical::canonicalize(path)
                .or_else(|e| match crate::members::split(path) {
                    Some((db, table)) => crate::canonical::canonicalize(&db)
                        .map(|db| crate::members::place(&db, &table)),
                    None => Err(e),
                })
                .unwrap_or_else(|_| path.to_path_buf())
        };
        let entry = stored.to_string_lossy().into_owned();

        // One read of the mount table for the whole prune. It is a kernel-generated
        // file, so reading it cannot block on the filesystems it describes.
        let mounts = crate::locality::Mounts::current();

        let now = unix_now();
        self.update_history_file("recents", |recents| {
            let mut visits = Visits::default();
            if let Some(at) = recents.iter().position(|l| parse_recent(l).0 == entry) {
                visits = parse_recent(&recents.remove(at)).1;
            }
            visits.count = visits.count.saturating_add(1);
            visits.last = now;
            recents.insert(0, format!("{entry}\t{}\t{}", visits.count, visits.last));
            recents.retain(|line| Self::recent_is_worth_keeping(parse_recent(line).0, &mounts));
            recents.truncate(MAX_RECENTS);
        })
        .inspect_err(|e| log::warn!(target: "datui", "record a recent: {e:#}"))
        .unwrap_or(HistoryUpdate::SkippedBusy)
    }

    /// How often and how lately each recent was opened, by its path as recorded.
    pub fn load_visits(&self) -> std::collections::HashMap<PathBuf, Visits> {
        self.load_recents_with_visits().1
    }
}

/// One line of the recents list: `path<TAB>opens<TAB>last opened`, its visits kept
/// with it so forgetting a recent forgets them too. A bare path has none.
fn parse_recent(line: &str) -> (&str, Visits) {
    let mut parts = line.rsplitn(3, '\t');
    if let (Some(last), Some(count), Some(path)) = (parts.next(), parts.next(), parts.next())
        && let (Ok(last), Ok(count)) = (last.parse(), count.parse())
    {
        return (path, Visits { count, last });
    }
    (line, Visits::default())
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// How often and how lately a dataset was opened: zoxide's frecency, which ranks
/// Recent and lifts often-opened matches (#547 M9).
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Visits {
    pub count: u32,
    /// Seconds since the epoch.
    pub last: u64,
}

impl Visits {
    /// Opens, weighted by how lately: four times within the hour, twice within the
    /// day, half within the week, a quarter after.
    pub fn frecency(&self, now: u64) -> f64 {
        let age = now.saturating_sub(self.last);
        let weight = match age {
            a if a < 3_600 => 4.0,
            a if a < 86_400 => 2.0,
            a if a < 604_800 => 0.5,
            _ => 0.25,
        };
        f64::from(self.count) * weight
    }
}

/// `recents`, most recent first, reordered by frecency. Ties, and recents opened
/// before visits were counted, keep the order they had.
pub fn by_frecency(
    mut recents: Vec<PathBuf>,
    visits: &std::collections::HashMap<PathBuf, Visits>,
) -> Vec<PathBuf> {
    let now = unix_now();
    let score = |p: &PathBuf| visits.get(p).map_or(0.0, |v| v.frecency(now));
    recents.sort_by(|a, b| score(b).total_cmp(&score(a)));
    recents
}

/// What datui remembers about a dataset it has already measured.
///
/// This is a **cache, not a catalogue**. Every field is re-derivable by reading the
/// dataset again, and each entry carries the size and modification time it was taken
/// from, so a changed dataset invalidates itself. Deleting the file costs speed and
/// nothing else — there is nothing here a user curated, and nothing that cannot be
/// rebuilt by looking again.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DatasetFacts {
    /// Modification time in seconds since the epoch, as a fingerprint.
    pub mtime: u64,
    /// Size in bytes, the other half of the fingerprint.
    pub size: u64,
    pub rows: Option<usize>,
    pub cols: Option<usize>,
    /// Whether `cols` is a floor rather than a total: the directory was too large to read
    /// every footer of, so it was sampled. Restored with the count, or the row would
    /// present a sample as a total the next time it is listed.
    #[serde(default)]
    pub cols_sampled: bool,
    /// Column names, which is what makes searching by column possible before
    /// anything has been read this run.
    #[serde(default)]
    pub columns: Vec<String>,
    /// What the dataset turned out to be. Recorded rather than re-derived, because a
    /// remote path cannot be classified without reading it, and a row that says
    /// `hive` under its own directory should not say something else under Recent.
    #[serde(default)]
    pub kind: Option<crate::discover::EntryKind>,
    /// Which build's rules `kind` came from. See [`crate::discover::CLASSIFIER_VERSION`].
    /// Absent in records written before this existed, which is what `0` means.
    #[serde(default)]
    pub classified_by: u32,
    /// What opening it will cost: compression, layout, partitioning. Worth keeping
    /// for the same reason the row count is — it came from a footer read that a
    /// remote dataset may not get a second chance at.
    #[serde(default)]
    pub cost: crate::discover::Cost,
    /// What one listing of the directory found in it, which is what its label says.
    /// Restored beside `kind` and gated by the same classifier version: both are what
    /// looking into the directory produced, and a build that classified differently
    /// counted differently too.
    #[serde(default, skip_serializing_if = "crate::discover::Holds::is_empty")]
    pub holds: crate::discover::Holds,
}

/// What an open learned about a dataset's files, kept so the next one can show its
/// columns and its row count without reading a single footer.
///
/// Reading the footers is what opening a large dataset costs: one read per file, and a
/// prefix of a few thousand objects spends seconds there every time it is opened. The
/// listing is one request and has to happen anyway — it is how datui knows what the
/// dataset is now — so it is the listing that decides whether this is still true, and
/// the footers that it saves.
///
/// A **cache, not a catalogue**, in the same sense as [`DatasetFacts`]: every field is
/// re-derivable by reading the dataset again, a fingerprint that no longer matches is
/// ignored, and deleting the file costs speed and nothing else.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetShape {
    /// What the dataset's files looked like when this was taken. A listing that comes
    /// back with a different one describes a dataset that has changed, and everything
    /// below it is then about a dataset that no longer exists.
    pub fingerprint: String,
    /// One entry per file, in the order the listing returned them, which is the order a
    /// scan reads them in.
    pub files: Vec<CachedFooter>,
    /// The distinct schemas the files have, as name and type in order.
    ///
    /// Held apart and referred to by index because a dataset of ten thousand files
    /// usually has one schema, sometimes three, and never ten thousand. Writing each
    /// file's columns out in full would make the cache larger than the footers it
    /// saves reading.
    ///
    /// The types are Polars' own, serialised as Polars serialises them, rather than
    /// their printed names parsed back. A name is not enough to rebuild a type — a
    /// nested or parametrised one prints as something no parser here could take apart
    /// again — and a type rebuilt slightly wrong would seat the wrong schema under a
    /// dataset that reads fine. Should that representation change under a Polars
    /// upgrade, the entries stop parsing and the cache is simply empty, which is the
    /// failure this is allowed to have.
    pub schemas: Vec<Vec<(String, polars::prelude::DataType)>>,
    /// Seconds since the Unix epoch, for a human reading the file.
    pub taken_at: u64,
}

/// What one file's footer said, as much of it as a reopen needs.
///
/// Everything here comes back out as a `FileFooter`, so a dataset rebuilt from the
/// cache goes through exactly the same code as one read from the store — the union, the
/// drift groups, the row numbering and the notes are all computed the same way from the
/// same shapes. A cache that took a shortcut past that would be a second
/// implementation of the dataset, and the two would drift.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CachedFooter {
    /// Which of [`DatasetShape::schemas`] this file has. `None` for a file whose footer
    /// would not read, which a reopen must remember as unreadable rather than quietly
    /// reading it again and getting a different dataset.
    pub schema: Option<usize>,
    /// The rows in each of its row groups, in order.
    pub row_group_rows: Vec<usize>,
    /// The compressed bytes of each, in the same order. Kept because a note is built
    /// from it, and a note that appears on a first open and not on a reopen is a worse
    /// bug than a slow open.
    pub row_group_bytes: Vec<usize>,
    /// Uncompressed bytes of each of its schema's columns, in the schema's order. A
    /// local footer carries them and a binary column's width is known nowhere else;
    /// a cloud footer does not, and leaves this empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_bytes: Vec<usize>,
}

impl DatasetShape {
    /// The fingerprint of a listing: which files, how many, how much they weigh, when
    /// each was last written, and the store's own tag for each where it gave one.
    ///
    /// Everything a listing can see without opening anything, which is the point — this
    /// has to be cheap enough to be worth taking, and all of it arrives in the one
    /// response that had to happen anyway. A file added, removed, renamed, resized or
    /// rewritten changes it; a dataset that has not been touched does not.
    ///
    /// The names are in it because the cache is a positional join: the remembered
    /// footers are lined up against a freshly listed dataset by position, so the
    /// identity of what is being joined belongs in the thing that says the join is
    /// still valid. The ETag is in it because size and a whole-second timestamp cannot
    /// see a file overwritten within the same second at the same length.
    pub fn fingerprint_of<'a>(
        files: impl IntoIterator<Item = (&'a str, u64, u64, Option<&'a str>)>,
    ) -> String {
        let mut hasher = StableHasher::default();
        let mut count = 0usize;
        let mut bytes = 0u64;
        for (key, size, stamp, etag) in files {
            hasher.bytes(key.as_bytes()).u64(size).u64(stamp);
            match etag {
                Some(etag) => hasher.u64(1).bytes(etag.as_bytes()),
                None => hasher.u64(0),
            };
            count += 1;
            bytes = bytes.saturating_add(size);
        }
        format!("{count}-{bytes}-{:016x}", hasher.finish())
    }

    /// Where `schema` sits in `schemas`, added on the end if it is not there yet.
    pub fn intern_schema(
        schemas: &mut Vec<Vec<(String, polars::prelude::DataType)>>,
        schema: &polars::prelude::Schema,
    ) -> usize {
        let columns: Vec<(String, polars::prelude::DataType)> = schema
            .iter()
            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
            .collect();
        schemas
            .iter()
            .position(|s| *s == columns)
            .unwrap_or_else(|| {
                schemas.push(columns);
                schemas.len() - 1
            })
    }

    /// The schema at `at` in `schemas`, or `None` when the table has no such entry.
    pub fn schema_at(
        schemas: &[Vec<(String, polars::prelude::DataType)>],
        at: usize,
    ) -> Option<polars::prelude::Schema> {
        let columns = schemas.get(at)?;
        let mut schema = polars::prelude::Schema::with_capacity(columns.len());
        for (name, dtype) in columns {
            schema.with_column(name.as_str().into(), dtype.clone());
        }
        Some(schema)
    }
}

/// Point the cache and config at scratch directories of this test process's own.
///
/// Library tests need not call this: `CacheManager::new` and `ConfigManager::new`
/// do, so no unit test reaches either without it, whatever order the tests run in
/// and however few share the process. Opening a dataset records it in recents and
/// saving a view writes under the config directory; left alone, both land in
/// the developer's own. The variables are process-wide, so this runs once.
///
/// The directories are named at random, not by process id: ids are reused, and a run
/// that landed on a finished run's id inherited its recents and views. They are
/// removed when the process exits.
#[cfg(test)]
pub(crate) fn isolate_cache() {
    // Held for the life of the process. A static is never dropped, so they are removed
    // by an exit handler instead.
    static SCRATCH: std::sync::Mutex<Vec<tempfile::TempDir>> = std::sync::Mutex::new(Vec::new());
    unsafe extern "C" {
        fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
    }
    extern "C" fn remove_scratch_dirs() {
        if let Ok(mut held) = SCRATCH.lock() {
            held.clear();
        }
    }
    let scratch_dir = |prefix: &str| {
        tempfile::Builder::new()
            .prefix(prefix)
            .tempdir()
            .expect("a scratch directory for the test process")
    };

    static ISOLATE: std::sync::Once = std::sync::Once::new();
    ISOLATE.call_once(|| {
        let dir = scratch_dir("datui-unit-cache-");
        let config_dir = scratch_dir("datui-unit-config-");
        // SAFETY: test-only. Tests run on parallel threads, so this can race another test
        // reading the environment; accepted in tests and never done outside them.
        unsafe { std::env::set_var("DATUI_CACHE_DIR", dir.path()) };
        unsafe { std::env::set_var("DATUI_CONFIG_DIR", config_dir.path()) };
        let mut held = SCRATCH.lock().unwrap_or_else(|e| e.into_inner());
        held.push(dir);
        held.push(config_dir);
        // SAFETY: the C runtime's `atexit`, present on every platform std runs on; the
        // callback only drops the directories above.
        unsafe { atexit(remove_scratch_dirs) };
    });
}

/// Whether this process is a test binary cargo built, which is where `cargo test`
/// and `cargo bench` put everything: `target/<profile>/deps/<crate>-<hash>`, or
/// `target/<triple>/<profile>/deps/` for a cross build. The program itself is
/// `target/<profile>/datui`, and an installed one is nowhere near.
pub(crate) fn running_as_a_cargo_test() -> bool {
    std::env::current_exe()
        .is_ok_and(|exe| cargo_test_layout(&exe, std::env::var_os("CARGO_TARGET_DIR").as_deref()))
}

/// The directory is `deps`, and it sits under a `target` at most three levels up, or
/// under the target directory cargo was told to use. Asked of the shape rather than of
/// any `deps` anywhere in the path, so a program installed under some `deps` directory
/// of the user's own is not mistaken for a test.
fn cargo_test_layout(exe: &Path, target_dir: Option<&std::ffi::OsStr>) -> bool {
    let Some(deps) = exe.parent() else {
        return false;
    };
    if deps.file_name().is_none_or(|name| name != "deps") {
        return false;
    }
    if target_dir.is_some_and(|dir| exe.starts_with(dir)) {
        return true;
    }
    deps.ancestors()
        .skip(1)
        .take(3)
        .any(|dir| dir.file_name().is_some_and(|name| name == "target"))
}

/// The buckets a cloud source listed on an earlier run, shown straight away on the next
/// one while a fresh listing is out.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloudListing {
    /// What the source pointed at when it was listed. A listing whose fingerprint no
    /// longer matches describes another server and is ignored.
    pub fingerprint: String,
    pub buckets: Vec<String>,
    /// Seconds since the Unix epoch.
    pub listed_at: u64,
}

/// Dataset shapes, by dataset path, at their listing's fingerprint.
///
/// Bounded by bytes: a shape's size follows its dataset's file count — NOAA's
/// by_station, 842k files, is a few megabytes — so a count alone would let a handful of
/// huge datasets hold the disk while a small one pushed out a shape still being opened.
pub(crate) struct Shapes;

impl Kind for Shapes {
    const DIR: &'static str = "shapes";
    const EXT: &'static str = "shape";
    const VERSION: u16 = 1;
    const BUDGET: u64 = 128 << 20;
    type Value = DatasetShape;

    fn encode(shape: &DatasetShape) -> Result<Vec<u8>> {
        encode_shape(shape)
    }

    fn decode(payload: &[u8]) -> Option<DatasetShape> {
        decode_shape(payload)
    }
}

/// What home has measured of each dataset, by path. Each record carries the size and
/// mtime it was taken at, so the store's fingerprint is empty.
pub(crate) struct Facts;

impl Kind for Facts {
    const DIR: &'static str = "facts";
    const EXT: &'static str = "facts";
    const VERSION: u16 = 1;
    const BUDGET: u64 = 16 << 20;
    type Value = DatasetFacts;

    fn encode(facts: &DatasetFacts) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(facts)?)
    }

    fn decode(payload: &[u8]) -> Option<DatasetFacts> {
        serde_json::from_slice(payload).ok()
    }
}

/// Each cloud source's last bucket listing, by source ID, at the source's fingerprint.
pub(crate) struct CloudListings;

impl Kind for CloudListings {
    const DIR: &'static str = "cloud_listings";
    const EXT: &'static str = "listing";
    const VERSION: u16 = 1;
    const BUDGET: u64 = 4 << 20;
    type Value = CloudListing;

    fn encode(listing: &CloudListing) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(listing)?)
    }

    fn decode(payload: &[u8]) -> Option<CloudListing> {
        serde_json::from_slice(payload).ok()
    }
}

/// The part of a shape read before its footers: its fingerprint, its schemas and when
/// it was taken. JSON because the schemas are Polars types, serialised as Polars
/// serialises them; the per-file footers, which are most of the bytes, follow as varints.
#[derive(serde::Serialize, serde::Deserialize)]
struct ShapeHeader {
    fingerprint: String,
    schemas: Vec<Vec<(String, polars::prelude::DataType)>>,
    taken_at: u64,
}
fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn take_varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let (&b, rest) = bytes.split_first()?;
        *bytes = rest;
        n |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(n);
        }
    }
    None
}

fn put_list(out: &mut Vec<u8>, values: &[usize]) {
    put_varint(out, values.len() as u64);
    for &v in values {
        put_varint(out, v as u64);
    }
}

fn take_list(bytes: &mut &[u8]) -> Option<Vec<usize>> {
    let len = usize::try_from(take_varint(bytes)?).ok()?;
    // Each value is at least a byte, so a length past what is left is a broken file,
    // not an allocation to attempt.
    if len > bytes.len() {
        return None;
    }
    (0..len)
        .map(|_| take_varint(bytes).and_then(|v| usize::try_from(v).ok()))
        .collect()
}

fn encode_shape(shape: &DatasetShape) -> Result<Vec<u8>> {
    let header = serde_json::to_vec(&ShapeHeader {
        fingerprint: shape.fingerprint.clone(),
        schemas: shape.schemas.clone(),
        taken_at: shape.taken_at,
    })?;
    let mut out = Vec::with_capacity(8 + header.len() + shape.files.len() * 8);
    out.extend_from_slice(&u32::try_from(header.len())?.to_le_bytes());
    out.extend_from_slice(&header);
    put_varint(&mut out, shape.files.len() as u64);
    for file in &shape.files {
        put_varint(&mut out, file.schema.map_or(0, |s| s as u64 + 1));
        put_list(&mut out, &file.row_group_rows);
        put_list(&mut out, &file.row_group_bytes);
        put_list(&mut out, &file.column_bytes);
    }
    Ok(out)
}

/// The shape in `bytes`, or `None` for one that does not hold together.
fn decode_shape(bytes: &[u8]) -> Option<DatasetShape> {
    let (len, rest) = bytes.split_first_chunk::<4>()?;
    let len = usize::try_from(u32::from_le_bytes(*len)).ok()?;
    let (header, mut body) = (rest.get(..len)?, rest.get(len..)?);
    let header: ShapeHeader = serde_json::from_slice(header).ok()?;
    let count = usize::try_from(take_varint(&mut body)?).ok()?;
    if count > body.len() {
        return None;
    }
    let mut files = Vec::with_capacity(count);
    // Totals over the whole dataset must fit, so no sum a reader makes later can
    // overflow on a damaged file.
    let (mut rows, mut bytes_total) = (0usize, 0usize);
    for _ in 0..count {
        let schema = match take_varint(&mut body)? {
            0 => None,
            at => Some(usize::try_from(at - 1).ok()?),
        };
        let footer = CachedFooter {
            schema,
            row_group_rows: take_list(&mut body)?,
            row_group_bytes: take_list(&mut body)?,
            column_bytes: take_list(&mut body)?,
        };
        if let Some(at) = footer.schema
            && at >= header.schemas.len()
        {
            return None;
        }
        for &n in &footer.row_group_rows {
            rows = rows.checked_add(n)?;
        }
        for &n in footer.row_group_bytes.iter().chain(&footer.column_bytes) {
            bytes_total = bytes_total.checked_add(n)?;
        }
        files.push(footer);
    }
    body.is_empty().then_some(DatasetShape {
        fingerprint: header.fingerprint,
        files,
        schemas: header.schemas,
        taken_at: header.taken_at,
    })
}

impl CacheManager {
    /// How many dataset shapes are kept.
    pub fn dataset_shapes_kept(&self) -> usize {
        Store::<Shapes>::new(self).len()
    }

    /// What datui remembers about one dataset, if the fingerprint still matches.
    ///
    /// Taking the fingerprint as an argument rather than returning the entry and
    /// letting the caller check is deliberate: an entry whose fingerprint has moved on
    /// describes a dataset that no longer exists, and there is no use for it that is
    /// not a mistake. A hit counts as use, so the datasets that never change, with the
    /// most to gain, are not the first to age out.
    pub fn dataset_shape(&self, path: &str, fingerprint: &str) -> Option<DatasetShape> {
        Store::<Shapes>::new(self).get(path, fingerprint)
    }

    /// Whether a shape is kept for `path` at all, whatever it was taken against: one
    /// stat, so a caller can tell whether listing the dataset could find it.
    pub fn has_dataset_shape(&self, path: &str) -> bool {
        Store::<Shapes>::new(self).file(path).exists()
    }

    /// Remember one dataset's shape, keeping the others while they fit the budget.
    pub fn save_dataset_shape(&self, path: &str, shape: DatasetShape) {
        Store::<Shapes>::new(self).put(path, &shape.fingerprint, &shape);
    }

    /// The last listing of cloud source `id`, if it was taken at `fingerprint`.
    pub fn cloud_listing(&self, id: &str, fingerprint: &str) -> Option<CloudListing> {
        Store::<CloudListings>::new(self).get(id, fingerprint)
    }

    /// Every source's last listing, by source ID.
    pub fn load_cloud_listings(&self) -> std::collections::HashMap<String, CloudListing> {
        Store::<CloudListings>::new(self)
            .scan()
            .into_iter()
            .collect()
    }

    /// Record one source's listing, keeping the others.
    pub fn save_cloud_listing(&self, id: &str, listing: CloudListing) {
        Store::<CloudListings>::new(self).put(id, &listing.fingerprint.clone(), &listing);
    }

    /// Source IDs hidden from the home screen with Delete.
    pub fn load_hidden_cloud_sources(&self) -> Vec<String> {
        self.load_history_or_log("cloud_hidden")
    }

    /// Hide a source from the home screen until the cache is cleared.
    pub fn hide_cloud_source(&self, id: &str) {
        let id = id.to_string();
        self.update_history_file("cloud_hidden", |hidden| {
            if !hidden.contains(&id) {
                hidden.push(id.clone());
            }
        })
        .or_log("hide a cloud source");
    }

    /// The directories Ctrl+D kept here before 0.4.0, which keeps them in
    /// `catalog.toml`, in the order they were added.
    pub fn load_remembered_places(&self) -> Vec<PathBuf> {
        let file = self.cache_file(&format!("home_remembered{HISTORY_SUFFIX}"));
        if !file.exists() {
            return Vec::new();
        }
        self.load_history_or_log("home_remembered")
            .into_iter()
            .map(PathBuf::from)
            .collect()
    }

    /// Drop the list [`Self::load_remembered_places`] reads, once it is moved.
    pub fn clear_remembered_places(&self) {
        let file = self.cache_file(&format!("home_remembered{HISTORY_SUFFIX}"));
        if let Err(e) = std::fs::remove_file(&file)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            log::warn!(target: "datui", "remove {}: {e}", file.display());
        }
    }

    /// Write `places` where Ctrl+D kept them before 0.4.0, for the migration's tests.
    pub fn save_remembered_places(&self, places: &[PathBuf]) -> Result<()> {
        let places: Vec<String> = places
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        self.save_history_file("home_remembered", &places)
    }
}

impl CacheManager {
    /// Everything datui knows about datasets it has measured before, without counting
    /// any of it as use. Unreadable records are absent: this is a cache, and failing to
    /// read it must never be worse than not having it.
    pub fn load_dataset_facts(&self) -> std::collections::HashMap<PathBuf, DatasetFacts> {
        Store::<Facts>::new(self)
            .scan()
            .into_iter()
            .map(|(path, facts)| (PathBuf::from(path), facts))
            .collect()
    }

    /// What datui knows about one dataset, counted as use.
    pub fn dataset_facts(&self, path: &Path) -> Option<DatasetFacts> {
        Store::<Facts>::new(self).get(path.to_str()?, "")
    }

    /// Count these datasets' records as used: shown on the home screen, say. The
    /// least recently used go first once the records pass their budget.
    pub fn touch_dataset_facts<'a>(&self, paths: impl IntoIterator<Item = &'a Path>) {
        let store = Store::<Facts>::new(self);
        for path in paths.into_iter().filter_map(Path::to_str) {
            store.touch(path);
        }
    }

    /// Record newly measured datasets, one file each. A path that is not UTF-8 is
    /// not recorded.
    pub fn record_dataset_facts(&self, facts: &[(PathBuf, DatasetFacts)]) {
        Store::<Facts>::new(self).put_all(
            facts
                .iter()
                .filter_map(|(path, facts)| Some((path.to_str()?, "", facts))),
        );
    }

    /// Run `work` holding the named cache lock, or skip it if the lock is contended
    /// past the deadline.
    fn with_cache_lock<F>(&self, name: &str, work: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        use fs2::FileExt;

        self.ensure_cache_dir()?;
        let Some(lock) = lock_file(&self.cache_file(&format!("{name}.lock")), LOCK_TIMEOUT)? else {
            log::info!(target: "datui", "{name} cache not updated: its lock is busy");
            return Ok(());
        };

        let result = work();
        let _ = FileExt::unlock(&lock);
        result
    }
}

#[cfg(test)]
mod harness_tests {
    /// This test binary is one cargo built into `deps`, so the refusal in
    /// `CacheManager::new` is armed here. It guards the integration tests, which link
    /// the library without `cfg(test)`: one that reaches `new` without
    /// `DATUI_CACHE_DIR` set stops instead of writing its fixtures into the
    /// developer's own recents.
    #[test]
    fn a_cargo_test_binary_is_recognized() {
        assert!(super::running_as_a_cargo_test());
    }

    /// A unit test that builds a manager with nothing set up, alone in its process
    /// as nextest runs it, gets scratch directories: not the developer's own, and not
    /// the refusal.
    #[test]
    fn a_unit_test_needs_no_setup_to_isolate() {
        let cache = super::CacheManager::new(crate::APP_NAME).unwrap();
        let config = crate::config::ConfigManager::new(crate::APP_NAME).unwrap();
        let scratch = std::env::temp_dir();
        assert!(cache.cache_dir().starts_with(&scratch), "{cache:?}");
        let config = config.config_dir();
        assert!(config.starts_with(&scratch), "{config:?}");
    }

    /// Only cargo's own layout counts. A program someone installed under a directory
    /// called `deps` must not refuse to start with a message about the test harness.
    #[test]
    fn only_cargos_layout_is_a_test() {
        use std::ffi::OsStr;
        use std::path::Path;
        let layout = |exe: &str| super::cargo_test_layout(Path::new(exe), None);
        assert!(layout(
            "/home/x/src/datui/target/debug/deps/home_test-1a2b3c"
        ));
        assert!(layout(
            "/home/x/src/datui/target/x86_64-unknown-linux-gnu/release/deps/datui-1a2b"
        ));
        assert!(!layout("/home/x/src/datui/target/debug/datui"));
        assert!(!layout("/opt/deps/bin/datui"));
        assert!(!layout("/home/x/deps/datui-0.4/bin/datui"));
        assert!(!layout("/home/x/src/datui/target/debug/examples/demo"));
        // A target directory of another name, when cargo was told about it.
        assert!(!layout("/home/x/build/datui/debug/deps/home_test-1a2b3c"));
        assert!(super::cargo_test_layout(
            Path::new("/home/x/build/datui/debug/deps/home_test-1a2b3c"),
            Some(OsStr::new("/home/x/build/datui"))
        ));
        assert!(!super::cargo_test_layout(
            Path::new("/home/x/build/deps/datui"),
            Some(OsStr::new("/home/x/other"))
        ));
    }
}

#[cfg(test)]
mod recents_pruning_tests {
    use super::*;

    fn cache() -> (CacheManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("temp dir");
        (CacheManager::with_dir(dir.path().to_path_buf()), dir)
    }

    #[test]
    fn a_recent_whose_directory_is_gone_is_forgotten() {
        // The case that filled a real recents file with fifty dead /tmp paths: a test
        // harness opening a fixture in a temp directory, over and over. Nothing took
        // them out again, and each one left an unavailable root on the home screen.
        let (cache, _keep) = cache();
        let scratch = tempfile::tempdir().expect("scratch");
        let dataset = scratch.path().join("people.csv");
        std::fs::write(&dataset, b"a,b\n1,2\n").expect("write");
        // Recents store the canonical path, which is not the one tempdir hands out
        // everywhere: /var is /private/var on macOS, and Windows adds a \\?\ prefix.
        let dataset = crate::canonical::canonicalize(&dataset).expect("canonicalize");

        cache.push_recent(&dataset);
        assert!(cache.load_recents().iter().any(|p| p == &dataset));

        // A second directory of its own, not a fixed name in the system temp directory:
        // that would be one path shared by every concurrent run of this suite.
        let elsewhere = tempfile::tempdir().expect("elsewhere");
        let survivor = elsewhere.path().join("still-here.csv");
        std::fs::write(&survivor, b"a\n1\n").expect("write");

        // The directory goes away, as a temp directory does.
        drop(scratch);

        // The next write is what cleans up. Recents are rewritten on open, so the list
        // heals as datui is used rather than needing a maintenance pass.
        cache.push_recent(&survivor);
        let recents = cache.load_recents();
        assert!(
            !recents.iter().any(|p| p == &dataset),
            "the dead path should be gone; got {recents:?}"
        );
        assert!(
            recents
                .iter()
                .any(|p| p.file_name() == survivor.file_name()),
            "the live path should remain; got {recents:?}"
        );
    }

    #[test]
    fn a_deleted_file_in_a_directory_that_still_exists_is_kept() {
        // Deliberate. Anything regenerated in place -- a nightly export, a file being
        // rewritten while datui looks at it -- is briefly absent, and forgetting it for
        // that is worse than showing it.
        let (cache, _keep) = cache();
        let scratch = tempfile::tempdir().expect("scratch");
        let dataset = scratch.path().join("nightly.parquet");
        std::fs::write(&dataset, b"x").expect("write");
        // Canonical, as recents store it; see the test above. Taken now, while the
        // file still exists to be resolved.
        let dataset = crate::canonical::canonicalize(&dataset).expect("canonicalize");
        cache.push_recent(&dataset);

        std::fs::remove_file(&dataset).expect("remove");
        let other = scratch.path().join("other.csv");
        std::fs::write(&other, b"a\n1\n").expect("write");
        cache.push_recent(&other);

        assert!(
            cache.load_recents().iter().any(|p| p == &dataset),
            "a missing file in a live directory should stay"
        );
    }

    #[test]
    fn a_remote_recent_is_never_stated_let_alone_dropped() {
        // A share being down is exactly when its recents matter most, and an
        // object-store URL has no local existence to check. Neither may be pruned.
        let (cache, _keep) = cache();
        let scratch = tempfile::tempdir().expect("scratch");
        let local = scratch.path().join("local.csv");
        std::fs::write(&local, b"a\n1\n").expect("write");

        for url in [
            "s3://bucket/warehouse/events.parquet",
            "gs://bucket/data.csv",
            "https://example.com/data.csv",
        ] {
            cache.push_recent(std::path::Path::new(url));
        }
        cache.push_recent(&local);

        let recents = cache.load_recents();
        for url in [
            "s3://bucket/warehouse/events.parquet",
            "gs://bucket/data.csv",
            "https://example.com/data.csv",
        ] {
            assert!(
                recents.iter().any(|p| p.to_string_lossy() == url),
                "{url} should have survived; got {recents:?}"
            );
        }
    }
}

#[cfg(test)]
mod dataset_shape_tests {
    use super::*;

    fn shape(fingerprint: &str, taken_at: u64) -> DatasetShape {
        DatasetShape {
            fingerprint: fingerprint.to_string(),
            files: vec![
                CachedFooter {
                    schema: Some(0),
                    row_group_rows: vec![10],
                    row_group_bytes: vec![1_000],
                    column_bytes: Vec::new(),
                },
                CachedFooter {
                    schema: Some(0),
                    row_group_rows: vec![20],
                    row_group_bytes: vec![2_000],
                    column_bytes: Vec::new(),
                },
            ],
            schemas: vec![vec![("id".into(), polars::prelude::DataType::Int64)]],
            taken_at,
        }
    }

    /// A payload that frames fine but does not hold together is refused, never a
    /// panic: a schema index past the table, or totals that overflow.
    #[test]
    fn a_shape_that_does_not_hold_together_is_refused() {
        let good = shape("fp", 1);
        assert_eq!(
            decode_shape(&encode_shape(&good).unwrap()),
            Some(good.clone())
        );
        let mut wild = good.clone();
        wild.files[0].schema = Some(5);
        assert!(decode_shape(&encode_shape(&wild).unwrap()).is_none());
        let mut huge = good.clone();
        huge.files[0].row_group_rows = vec![usize::MAX, 1];
        assert!(decode_shape(&encode_shape(&huge).unwrap()).is_none());
        let bytes = encode_shape(&good).unwrap();
        for cut in 0..bytes.len() {
            assert!(decode_shape(&bytes[..cut]).is_none(), "cut {cut}");
        }
    }

    /// A dataset that has not changed is remembered; one that has is not.
    ///
    /// The whole cache turns on this: the listing is cheap and happens anyway, and what
    /// it fingerprints decides whether thousands of footer reads can be skipped. An
    /// entry returned for a dataset that has moved on would seat the wrong row counts
    /// under the right name, which is worse than reading the footers again.
    #[test]
    fn a_shape_comes_back_only_for_the_dataset_it_was_taken_from() {
        let dir = tempfile::tempdir().unwrap();
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        cache.save_dataset_shape("s3://b/events/", shape("2-30-abc", 100));

        assert_eq!(
            cache.dataset_shape("s3://b/events/", "2-30-abc"),
            Some(shape("2-30-abc", 100)),
            "the same dataset, unchanged"
        );
        assert_eq!(
            cache.dataset_shape("s3://b/events/", "3-40-def"),
            None,
            "a file added, removed or rewritten since"
        );
        assert_eq!(
            cache.dataset_shape("s3://b/other/", "2-30-abc"),
            None,
            "and a different dataset that happens to weigh the same"
        );
    }

    /// The fingerprint sees everything a listing can see, and nothing it cannot.
    #[test]
    fn the_fingerprint_moves_when_the_files_do() {
        let base = DatasetShape::fingerprint_of([
            ("a.parquet", 100, 7, Some("e1")),
            ("b.parquet", 200, 8, Some("e2")),
        ]);
        assert_eq!(
            base,
            DatasetShape::fingerprint_of([
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 200, 8, Some("e2")),
            ]),
            "the same listing twice is the same fingerprint"
        );
        for (changed, why) in [
            (vec![("a.parquet", 100, 7, Some("e1"))], "a file removed"),
            (
                vec![
                    ("a.parquet", 100, 7, Some("e1")),
                    ("b.parquet", 200, 8, Some("e2")),
                    ("c.parquet", 50, 9, Some("e3")),
                ],
                "a file added",
            ),
            (
                vec![
                    ("a.parquet", 100, 7, Some("e1")),
                    ("b.parquet", 201, 8, Some("e2")),
                ],
                "a file resized",
            ),
            (
                vec![
                    ("a.parquet", 100, 7, Some("e1")),
                    ("b.parquet", 200, 9, Some("e2")),
                ],
                "a file rewritten, which the stamp catches",
            ),
            (
                vec![
                    ("a.parquet", 100, 7, Some("e1")),
                    ("b.parquet", 200, 8, Some("e9")),
                ],
                "rewritten within the same second at the same length: only the tag sees it",
            ),
            (
                vec![
                    ("a.parquet", 100, 7, Some("e1")),
                    ("b.parquet", 200, 8, None),
                ],
                "a tag gone",
            ),
            (
                vec![
                    ("a.parquet", 100, 7, Some("e1")),
                    ("renamed.parquet", 200, 8, Some("e2")),
                ],
                "a file renamed, which reorders the positional join the cache is",
            ),
        ] {
            assert_ne!(base, DatasetShape::fingerprint_of(changed), "{why}");
        }
    }

    /// The same in every build: a file named or fingerprinted by a hash that moved with
    /// the Rust release would be orphaned by the next upgrade.
    #[test]
    fn the_stable_hash_is_pinned() {
        assert_eq!(stable_hash(b"123456789"), 0x995d_c9bb_df19_39fa);
        assert_eq!(
            DatasetShape::fingerprint_of([("a.parquet", 100, 7, Some("e1"))]),
            "1-100-7bb7c5da2f222965"
        );
    }

    /// Every field a reopen uses comes back as it was stored, an unreadable footer and
    /// a footer's column widths among them.
    #[test]
    fn a_shape_comes_back_as_it_was_stored() {
        let dir = tempfile::tempdir().unwrap();
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        let mut stored = shape("f", 7);
        stored.files.push(CachedFooter {
            schema: None,
            row_group_rows: Vec::new(),
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        });
        stored.files.push(CachedFooter {
            schema: Some(1),
            row_group_rows: vec![0, 300, u32::MAX as usize + 5],
            row_group_bytes: vec![1, 2, 3],
            column_bytes: vec![128, 1 << 40],
        });
        stored.schemas.push(vec![
            ("id".into(), polars::prelude::DataType::Int64),
            (
                "tags".into(),
                polars::prelude::DataType::List(Box::new(polars::prelude::DataType::String)),
            ),
        ]);
        cache.save_dataset_shape("s3://b/events/", stored.clone());
        assert_eq!(cache.dataset_shape("s3://b/events/", "f"), Some(stored));
    }

    /// What a dataset the size of NOAA's by_station costs to keep and find again.
    /// Run by hand: `scripts/dev/test.sh unit shape_cache_timings -- --ignored --nocapture`.
    #[test]
    #[ignore = "a timing, not a check"]
    fn shape_cache_timings() {
        use polars::prelude::DataType;
        const FILES: usize = 842_000;
        let dir = tempfile::tempdir().unwrap();
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        let big = DatasetShape {
            fingerprint: "big".into(),
            files: (0..FILES)
                .map(|i| CachedFooter {
                    schema: Some(0),
                    row_group_rows: vec![1_000 + (i * 7919) % 90_000],
                    row_group_bytes: vec![10_000 + (i * 104_729) % 900_000],
                    column_bytes: Vec::new(),
                })
                .collect(),
            schemas: vec![vec![
                ("ID".into(), DataType::String),
                ("DATE".into(), DataType::String),
                ("ELEMENT".into(), DataType::String),
                ("DATA_VALUE".into(), DataType::Int32),
                ("M_FLAG".into(), DataType::String),
                ("Q_FLAG".into(), DataType::String),
                ("S_FLAG".into(), DataType::String),
                ("OBS_TIME".into(), DataType::String),
            ]],
            taken_at: 1,
        };
        let time = |what: &str, work: &mut dyn FnMut()| {
            let began = std::time::Instant::now();
            work();
            eprintln!("{what}: {:.1?}", began.elapsed());
        };
        time("store by_station", &mut || {
            cache.save_dataset_shape("s3://noaa/by_station/", big.clone())
        });
        time("store a small one beside it", &mut || {
            cache.save_dataset_shape("s3://b/small/", shape("small", 2))
        });
        time("lookup + touch by_station", &mut || {
            assert!(
                cache
                    .dataset_shape("s3://noaa/by_station/", "big")
                    .is_some()
            )
        });
        time("lookup by_station, fingerprint moved", &mut || {
            assert!(
                cache
                    .dataset_shape("s3://noaa/by_station/", "moved")
                    .is_none()
            )
        });
        time("lookup + touch the small one", &mut || {
            assert!(cache.dataset_shape("s3://b/small/", "small").is_some())
        });
        let on_disk = Store::<Shapes>::new(&cache)
            .dir()
            .read_dir()
            .unwrap()
            .flatten()
            .map(|e| e.metadata().unwrap().len())
            .sum::<u64>();
        eprintln!("on disk: {:.1} MB", on_disk as f64 / 1e6);
    }
}

/// The same checks for every kind the [`Store`] holds.
#[cfg(test)]
mod store_harness_tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// What a kind's checks store: two values of one encoded size, and one larger than
    /// any budget a check sets.
    trait Sample: Kind + Send + Sync + 'static {
        fn sample(variant: u8) -> Self::Value;
        fn big() -> Self::Value;
    }

    impl Sample for Shapes {
        fn sample(variant: u8) -> DatasetShape {
            DatasetShape {
                fingerprint: "fp".into(),
                files: vec![CachedFooter {
                    schema: Some(0),
                    row_group_rows: vec![usize::from(variant)],
                    row_group_bytes: vec![100],
                    column_bytes: vec![8],
                }],
                schemas: vec![vec![("id".into(), polars::prelude::DataType::Int64)]],
                taken_at: 1,
            }
        }
        fn big() -> DatasetShape {
            let mut shape = Self::sample(1);
            shape.files = vec![shape.files[0].clone(); 2_000];
            shape
        }
    }

    impl Sample for Facts {
        fn sample(variant: u8) -> DatasetFacts {
            DatasetFacts {
                mtime: 1,
                size: 2,
                rows: Some(usize::from(variant)),
                cols: Some(1),
                columns: vec!["a".into()],
                ..Default::default()
            }
        }
        fn big() -> DatasetFacts {
            DatasetFacts {
                columns: (0..2_000).map(|i| format!("column_{i}")).collect(),
                ..Self::sample(1)
            }
        }
    }

    impl Sample for CloudListings {
        fn sample(variant: u8) -> CloudListing {
            CloudListing {
                fingerprint: "fp".into(),
                buckets: vec![format!("bucket-{variant}")],
                listed_at: 1,
            }
        }
        fn big() -> CloudListing {
            CloudListing {
                buckets: (0..2_000).map(|i| format!("bucket-{i}")).collect(),
                ..Self::sample(1)
            }
        }
    }

    fn encoded<K: Kind>(value: &K::Value) -> Vec<u8> {
        K::encode(value).unwrap()
    }

    fn same<K: Kind>(a: Option<K::Value>, b: &K::Value) -> bool {
        a.is_some_and(|a| encoded::<K>(&a) == encoded::<K>(b))
    }

    fn store<K: Kind>() -> (Store<K>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (
            Store::new(&CacheManager::with_dir(dir.path().to_path_buf())),
            dir,
        )
    }

    fn age(file: &Path, secs: u64) {
        fs::OpenOptions::new()
            .write(true)
            .open(file)
            .unwrap()
            .set_modified(UNIX_EPOCH + Duration::from_secs(secs))
            .unwrap();
    }

    fn modified(file: &Path) -> SystemTime {
        fs::metadata(file).unwrap().modified().unwrap()
    }

    fn round_trip<K: Sample>() {
        let (store, _dir) = store::<K>();
        let value = K::sample(1);
        store.put("s3://b/a/", "fp", &value);
        assert!(same::<K>(store.get("s3://b/a/", "fp"), &value), "a hit");
        assert!(
            store.get("s3://b/a/", "moved").is_none(),
            "another fingerprint"
        );
        assert!(store.get("s3://b/z/", "fp").is_none(), "another key");
        let scanned = store.scan();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].0, "s3://b/a/");
        // A key whose hash collides costs only its own entry: the frame holds the key.
        let other = K::sample(2);
        fs::copy(store.file("s3://b/a/"), store.file("s3://b/z/")).unwrap();
        assert!(
            store.get("s3://b/z/", "fp").is_none(),
            "a collision is a miss"
        );
        store.put("s3://b/a/", "fp", &other);
        assert!(same::<K>(store.get("s3://b/a/", "fp"), &other), "replaced");
    }

    fn damage_is_a_miss<K: Sample>() {
        let (store, _dir) = store::<K>();
        store.put("k", "fp", &K::sample(1));
        let file = store.file("k");
        let good = fs::read(&file).unwrap();
        for cut in 0..good.len() {
            fs::write(&file, &good[..cut]).unwrap();
            assert!(store.get("k", "fp").is_none(), "cut at {cut}");
        }
        for at in 0..good.len() {
            for bit in 0..8 {
                let mut bad = good.clone();
                bad[at] ^= 1 << bit;
                fs::write(&file, &bad).unwrap();
                assert!(store.get("k", "fp").is_none(), "bit {bit} of byte {at}");
            }
        }
        assert!(store.scan().is_empty(), "nor does a scan see it");
        fs::write(&file, &good).unwrap();
        assert!(
            store.get("k", "fp").is_some(),
            "and the good bytes still read"
        );
    }

    fn evicts_by_bytes_in_lru_order<K: Sample>() {
        let (store, _dir) = store::<K>();
        for (i, key) in ["k/a", "k/b", "k/c"].into_iter().enumerate() {
            store.put(key, "fp", &K::sample(1));
            age(&store.file(key), 1_000 + i as u64);
        }
        let one = fs::metadata(store.file("k/a")).unwrap().len();
        // `a` is the oldest, but using it makes `b` the one used longest ago.
        assert!(store.get("k/a", "fp").is_some());

        let store = store.with_budget(3 * one);
        store.put("k/d", "fp", &K::sample(1));
        assert_eq!(store.len(), 3, "kept to its budget in bytes");
        assert!(store.get("k/b", "fp").is_none(), "b went");
        for kept in ["k/a", "k/c", "k/d"] {
            assert!(store.get(kept, "fp").is_some(), "{kept} stayed");
        }

        // One entry larger than the whole budget is still kept: it is the one in use.
        store.put("k/huge", "fp", &K::big());
        assert_eq!(store.len(), 1, "everything else made room");
        assert!(store.get("k/huge", "fp").is_some());
    }

    fn a_hit_only_touches<K: Sample>() {
        let (store, _dir) = store::<K>();
        store.put("k/a", "fp", &K::sample(1));
        store.put("k/b", "fp", &K::sample(1));
        let (a, b) = (store.file("k/a"), store.file("k/b"));
        age(&a, 1_000);
        age(&b, 1_000);
        let before = fs::read(&a).unwrap();

        assert!(store.get("k/a", "fp").is_some());
        assert!(
            modified(&a) > UNIX_EPOCH + Duration::from_secs(1_000),
            "dated now"
        );
        assert_eq!(fs::read(&a).unwrap(), before, "and not rewritten");
        assert_eq!(
            modified(&b),
            UNIX_EPOCH + Duration::from_secs(1_000),
            "b untouched"
        );

        age(&a, 1_000);
        assert!(store.get("k/a", "moved").is_none());
        assert_eq!(
            modified(&a),
            UNIX_EPOCH + Duration::from_secs(1_000),
            "a miss is not use"
        );
        assert_eq!(store.scan().len(), 2);
        assert_eq!(
            modified(&a),
            UNIX_EPOCH + Duration::from_secs(1_000),
            "nor is a scan"
        );

        // Storing what is already there dates it, and writes nothing.
        let inode = fs::metadata(&a).unwrap();
        store.put("k/a", "fp", &K::sample(1));
        assert!(modified(&a) > UNIX_EPOCH + Duration::from_secs(1_000));
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(fs::metadata(&a).unwrap().ino(), inode.ino(), "not replaced");
        }
        let _ = inode;
    }

    fn sweeps_stale_temp_files<K: Sample>() {
        let (store, _dir) = store::<K>();
        store.put("k/a", "fp", &K::sample(1));
        let stale = store.dir().join("x.shape.1.0.tmp");
        let fresh = store.dir().join("x.shape.1.1.tmp");
        fs::write(&stale, b"half").unwrap();
        fs::write(&fresh, b"half").unwrap();
        age(&stale, 1_000);
        store.put("k/b", "fp", &K::sample(1));
        assert!(!stale.exists(), "a dead writer's temp file goes");
        assert!(fresh.exists(), "a live writer's stays");
    }

    fn clear_all_removes_it<K: Sample>() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::with_dir(dir.path().to_path_buf());
        let store = Store::<K>::new(&cache);
        store.put("k", "fp", &K::sample(1));
        assert!(store.get("k", "fp").is_some());
        cache.clear_all().unwrap();
        assert!(store.get("k", "fp").is_none());
        assert!(!store.dir().exists(), "the kind's directory is gone");
        assert!(
            cache.cache_file(&format!("{}.lock", K::DIR)).exists(),
            "its lock, which another instance may hold, is not"
        );
    }

    fn two_writers_race<K: Sample>() {
        let (store, _dir) = store::<K>();
        let store = std::sync::Arc::new(store);
        let writers: Vec<_> = (1..=2u8)
            .map(|variant| {
                let store = store.clone();
                std::thread::spawn(move || {
                    for i in 0..25 {
                        store.put("shared", "fp", &K::sample(variant));
                        store.put(&format!("own/{variant}/{i}"), "fp", &K::sample(variant));
                        assert!(store.get("shared", "fp").is_some(), "never torn");
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        let shared = encoded::<K>(&store.get("shared", "fp").unwrap());
        assert!(
            shared == encoded::<K>(&K::sample(1)) || shared == encoded::<K>(&K::sample(2)),
            "one writer's value, whole"
        );
        assert_eq!(store.len(), 51, "every entry landed");
        let temps = fs::read_dir(store.dir())
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
            .count();
        assert_eq!(temps, 0, "and no temp file is left");
    }

    macro_rules! suite {
        ($name:ident, $kind:ty) => {
            mod $name {
                use super::*;
                #[test]
                fn round_trip() {
                    super::round_trip::<$kind>();
                }
                #[test]
                fn damage_is_a_miss() {
                    super::damage_is_a_miss::<$kind>();
                }
                #[test]
                fn evicts_by_bytes_in_lru_order() {
                    super::evicts_by_bytes_in_lru_order::<$kind>();
                }
                #[test]
                fn a_hit_only_touches() {
                    super::a_hit_only_touches::<$kind>();
                }
                #[test]
                fn sweeps_stale_temp_files() {
                    super::sweeps_stale_temp_files::<$kind>();
                }
                #[test]
                fn clear_all_removes_it() {
                    super::clear_all_removes_it::<$kind>();
                }
                #[test]
                fn two_writers_race() {
                    super::two_writers_race::<$kind>();
                }
            }
        };
    }

    suite!(shapes, Shapes);
    suite!(facts, Facts);
    suite!(cloud_listings, CloudListings);

    /// The files earlier builds wrote are removed, not read.
    #[test]
    fn the_old_files_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::with_dir(dir.path().to_path_buf());
        let old = [
            "datasets.json",
            "dataset_shapes.json",
            "cloud_sources.json",
            "visits.json",
        ];
        for name in old {
            fs::write(dir.path().join(name), b"{}").unwrap();
        }
        fs::create_dir(dir.path().join("dataset_shapes")).unwrap();
        fs::write(dir.path().join("dataset_shapes/0.shape"), b"x").unwrap();
        cache.record_dataset_facts(&[(PathBuf::from("/d"), Facts::sample(1))]);
        for name in old {
            assert!(!dir.path().join(name).exists(), "{name}");
        }
        assert!(!dir.path().join("dataset_shapes").exists());
    }
}

#[cfg(test)]
mod facts_compat_tests {
    use super::DatasetFacts;
    use crate::discover::EntryKind;

    /// A kind this build does not recognize costs its own row, not the whole index.
    ///
    /// The dataset index is one JSON map read with `unwrap_or_default`, so a value that
    /// fails to parse discards every fact datui had learned about every dataset — not
    /// the one row it could not read. `EntryKind` gains variants as datui learns to
    /// recognize more (a Delta root, an Iceberg root), so an older build reading a newer
    /// cache is an ordinary event rather than a corruption.
    #[test]
    fn an_unknown_kind_costs_only_its_own_row() {
        let json = r#"{"mtime":1,"size":2,"rows":3,"cols":4,"columns":[],"kind":"quicksand"}"#;
        let facts: DatasetFacts = serde_json::from_str(json).expect("the record still parses");
        assert_eq!(
            facts.kind,
            Some(EntryKind::Unknown),
            "a kind from the future reads as unexamined"
        );
        assert_eq!(facts.rows, Some(3), "and the measurements survive with it");
        assert_eq!(facts.classified_by, 0, "recorded before that existed");

        // And the map around it survives too, which is the point.
        let index = r#"{"/a":{"mtime":1,"size":2,"rows":3,"cols":4,"columns":[],"kind":"quicksand"},
                        "/b":{"mtime":1,"size":2,"rows":9,"cols":1,"columns":[],"kind":"hive"}}"#;
        let map: std::collections::HashMap<std::path::PathBuf, DatasetFacts> =
            serde_json::from_str(index).expect("the index still parses");
        assert_eq!(map.len(), 2, "both rows, not none of them");
    }

    /// `datui cache clear` clears every cache kind and every list, and nothing else:
    /// not a lock another instance may hold, not the log, not a foreign file.
    #[test]
    fn clear_all_removes_the_caches_and_lists_only() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        for name in [
            "query_history.txt",
            "fuzzy_history.txt",
            "recents_history.txt",
            "recents_history.txt.12.0.tmp",
            "cloud_hidden_history.txt",
        ] {
            std::fs::write(dir.path().join(name), b"x").expect("write");
        }
        cache.record_dataset_facts(&[("/d".into(), DatasetFacts::default())]);
        let kept = [
            "facts.lock",
            "recents_history.lock",
            "datui.log",
            "keep.parquet",
            "subdir",
        ];
        for name in kept {
            if name == "subdir" {
                std::fs::create_dir(dir.path().join(name)).expect("mkdir");
            } else {
                std::fs::write(dir.path().join(name), b"x").expect("write");
            }
        }

        cache.clear_all().expect("clear");

        let mut left: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        let mut kept = kept.map(String::from).to_vec();
        kept.sort();
        assert_eq!(left, kept);
    }
}
