use crate::logging::LogFailure;
use color_eyre::Result;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

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

    /// Clear all registered cache files
    /// Note: Views are stored in config directory, not cache, so they are not cleared here.
    /// Note: History files (e.g., `{id}_history.txt`) are dynamic and excluded from `clear_all()`.
    /// They can be cleared individually via `clear_file()` if needed.
    pub fn clear_all(&self) -> Result<()> {
        // Everything datui writes here, not a fixed list: the list rotted — it held
        // two names while the directory grew histories, measurements, cloud sources
        // and the hidden-source file, so `datui cache clear` kept most of the cache and
        // broke the documented way to unhide a source. Files only, by the extensions
        // datui writes, so a stray directory or foreign file is left alone.
        match fs::remove_dir_all(self.dataset_shape_dir()) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                log::warn!(target: "datui", "remove the dataset shapes: {e}");
            }
            _ => {}
        }
        let Ok(entries) = fs::read_dir(&self.cache_dir) else {
            return Ok(());
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let log = crate::logging::LOG_FILE_NAME;
            let ours = path.is_file()
                && (matches!(
                    path.extension().and_then(|e| e.to_str()),
                    Some("json" | "txt" | "lock")
                ) || path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n == log || n.strip_prefix(log) == Some(".1")));
            if ours {
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
        let history_file = self.cache_file(&format!("{}_history.txt", history_id));

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
        let history_file = self.cache_file(&format!("{}_history.txt", history_id));

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

/// A JSON cache file. Missing is empty; unreadable or malformed is logged and empty,
/// since a cache that cannot be read must never be worse than not having it.
fn read_json_cache<T: serde::de::DeserializeOwned + Default>(path: &Path) -> T {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return T::default(),
        Err(e) => {
            log::warn!(target: "datui", "read {}: {e}", path.display());
            return T::default();
        }
    };
    serde_json::from_str(&text).unwrap_or_else(|e| {
        log::warn!(target: "datui", "{} is malformed, ignoring it: {e}", path.display());
        T::default()
    })
}

/// Write `bytes` to `path` through a sibling temp file renamed over it, so a reader,
/// another instance or a crash mid-write sees the old file or the new one, never part
/// of either. The temp name ends in `.tmp`, so nothing that lists `.json` or `.txt`
/// files ever sees it.
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
        self.load_history_or_log("recents")
            .into_iter()
            .map(std::path::PathBuf::from)
            .collect()
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
            recents.retain(|p| p != &target);
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
            recents.retain(|p| !targets.contains(p));
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

        let mut kept = Vec::new();
        let update = self
            .update_history_file("recents", |recents| {
                recents.retain(|p| p != &entry);
                recents.insert(0, entry.clone());
                recents.retain(|p| Self::recent_is_worth_keeping(p, &mounts));
                recents.truncate(MAX_RECENTS);
                kept = recents.clone();
            })
            .inspect_err(|e| log::warn!(target: "datui", "record a recent: {e:#}"))
            .unwrap_or(HistoryUpdate::SkippedBusy);
        if update == HistoryUpdate::Written {
            self.record_visit(&entry, &kept);
        }
        update
    }

    /// How often and how lately each recent was opened, by its path as recorded.
    pub fn load_visits(&self) -> std::collections::HashMap<PathBuf, Visits> {
        read_json_cache(&self.cache_file("visits.json"))
    }

    /// Count an open of `entry`, keeping visits only for the recents still listed.
    fn record_visit(&self, entry: &str, recents: &[String]) {
        let now = unix_now();
        self.with_cache_lock("visits", || {
            let mut visits = self.load_visits();
            let visit = visits.entry(PathBuf::from(entry)).or_default();
            visit.count = visit.count.saturating_add(1);
            visit.last = now;
            visits.retain(|path, _| recents.iter().any(|r| Path::new(r) == path));
            let json = serde_json::to_string(&visits)?;
            let temp = self.cache_file(&format!("visits.{}.tmp", std::process::id()));
            fs::write(&temp, json)?;
            fs::rename(&temp, self.cache_file("visits.json")).inspect_err(|_| {
                let _ = fs::remove_file(&temp);
            })?;
            Ok(())
        })
        .or_log("record a visit");
    }
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
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut count = 0usize;
        let mut bytes = 0u64;
        for (key, size, stamp, etag) in files {
            key.hash(&mut hasher);
            size.hash(&mut hasher);
            stamp.hash(&mut hasher);
            etag.hash(&mut hasher);
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

/// Entries kept in the dataset index.
///
/// Large enough to cover everywhere someone actually works, small enough that the
/// file stays trivial to read and rewrite.
pub const MAX_DATASET_FACTS: usize = 4096;

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

/// Bytes of dataset shapes kept, all of them together. A shape's size follows its
/// dataset's file count — NOAA's by_station, 842k files, is a few megabytes — so a
/// count alone would let a handful of huge datasets hold the disk while a small one
/// pushed out a shape still being opened. The one just stored is always kept.
pub const MAX_DATASET_SHAPE_BYTES: u64 = 128 << 20;

/// The first bytes of a shape file, which also say how the rest is laid out. A file
/// that does not start with them is from another build and is ignored.
const SHAPE_MAGIC: &[u8; 8] = b"DTSHAPE1";

/// The part of a shape file read before deciding whether the rest is wanted: which
/// dataset it is, the fingerprint it was taken at, and its schemas. JSON because the
/// schemas are Polars types, serialised as Polars serialises them; the per-file
/// footers, which are most of the bytes, follow as varints.
#[derive(serde::Serialize, serde::Deserialize)]
struct ShapeHeader {
    path: String,
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

fn encode_shape(path: &str, shape: &DatasetShape) -> Result<Vec<u8>> {
    let header = serde_json::to_vec(&ShapeHeader {
        path: path.to_string(),
        fingerprint: shape.fingerprint.clone(),
        schemas: shape.schemas.clone(),
        taken_at: shape.taken_at,
    })?;
    let mut out = Vec::with_capacity(16 + header.len() + shape.files.len() * 8);
    out.extend_from_slice(SHAPE_MAGIC);
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

/// The shape in `bytes` if it is `path`'s and was taken at `fingerprint`. The footers
/// are decoded only once the header says they are wanted.
fn decode_shape(bytes: &[u8], path: &str, fingerprint: &str) -> Option<DatasetShape> {
    let rest = bytes.strip_prefix(SHAPE_MAGIC)?;
    let (len, rest) = rest.split_first_chunk::<4>()?;
    let len = usize::try_from(u32::from_le_bytes(*len)).ok()?;
    let (header, mut body) = (rest.get(..len)?, rest.get(len..)?);
    let header: ShapeHeader = serde_json::from_slice(header).ok()?;
    if header.path != path || header.fingerprint != fingerprint {
        return None;
    }
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
    /// One file per dataset, so a lookup reads its own dataset and nothing else. They
    /// were one JSON map, and by_station alone made it 51 MB that every lookup of any
    /// dataset parsed and every hit rewrote.
    fn dataset_shape_dir(&self) -> PathBuf {
        self.cache_file("dataset_shapes")
    }

    /// The file `path`'s shape lives in: named by a hash of the path, which the file
    /// repeats, so two paths that hash alike only cost each other their entries.
    fn dataset_shape_file(&self, path: &str) -> PathBuf {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut hasher);
        self.dataset_shape_dir()
            .join(format!("{:016x}.shape", hasher.finish()))
    }

    /// How many dataset shapes are kept.
    pub fn dataset_shapes_kept(&self) -> usize {
        fs::read_dir(self.dataset_shape_dir()).map_or(0, |entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == "shape"))
                .count()
        })
    }

    /// What datui remembers about one dataset, if the fingerprint still matches.
    ///
    /// Taking the fingerprint as an argument rather than returning the entry and
    /// letting the caller check is deliberate: an entry whose fingerprint has moved on
    /// describes a dataset that no longer exists, and there is no use for it that is
    /// not a mistake.
    ///
    /// Unreadable means absent — a cache that cannot be read is one that has nothing
    /// to say, not an error worth stopping an open for.
    pub fn dataset_shape(&self, path: &str, fingerprint: &str) -> Option<DatasetShape> {
        let file = self.dataset_shape_file(path);
        let bytes = fs::read(&file).ok()?;
        let shape = decode_shape(&bytes, path, fingerprint)?;
        // A hit counts as use. Without this the clock only moves on a miss, so the
        // entries that age out first are the datasets that never change — the ones with
        // a perfect hit rate and the most to gain — while a dataset rewritten every day
        // keeps resetting its own and stays forever. The file's mtime is the clock, so
        // marking it costs nothing like rewriting it.
        Self::touch(&file);
        Some(shape)
    }

    /// Mark a file as used just now. Best effort: a shape that cannot be re-dated is
    /// still a shape that can be used.
    fn touch(file: &Path) {
        fs::OpenOptions::new()
            .write(true)
            .open(file)
            .and_then(|f| f.set_modified(std::time::SystemTime::now()))
            .or_log("re-date a dataset shape");
    }

    /// Remember one dataset's shape, keeping the others while they fit in
    /// [`MAX_DATASET_SHAPE_BYTES`].
    pub fn save_dataset_shape(&self, path: &str, shape: DatasetShape) {
        self.save_dataset_shape_within(path, &shape, MAX_DATASET_SHAPE_BYTES);
    }

    fn save_dataset_shape_within(&self, path: &str, shape: &DatasetShape, max_bytes: u64) {
        let file = self.dataset_shape_file(path);
        let write = || -> Result<()> {
            fs::create_dir_all(self.dataset_shape_dir())?;
            let bytes = encode_shape(path, shape)?;
            let temp = file.with_extension(format!("{}.tmp", std::process::id()));
            fs::write(&temp, bytes)?;
            fs::rename(&temp, &file).inspect_err(|_| {
                let _ = fs::remove_file(&temp);
            })?;
            Ok(())
        };
        write().or_log("save a dataset shape");
        // Under the lock only to keep two evictions from racing each other; writes
        // land by rename and need none.
        self.with_cache_lock("dataset_shapes", || {
            // The single map this replaced, which nothing reads any more.
            let _ = fs::remove_file(self.cache_file("dataset_shapes.json"));
            self.evict_dataset_shapes(&file, max_bytes);
            Ok(())
        })
        .or_log("evict dataset shapes");
    }

    /// Drop the least recently used shapes until what is left fits in `max_bytes`,
    /// never `keep`, which was just stored.
    fn evict_dataset_shapes(&self, keep: &Path, max_bytes: u64) {
        let Ok(entries) = fs::read_dir(self.dataset_shape_dir()) else {
            return;
        };
        let mut kept: Vec<(std::time::SystemTime, u64, PathBuf)> = entries
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "shape"))
            .filter_map(|e| {
                let meta = e.metadata().ok()?;
                Some((meta.modified().ok()?, meta.len(), e.path()))
            })
            .collect();
        let mut total: u64 = kept.iter().map(|(_, len, _)| len).sum();
        if total <= max_bytes {
            return;
        }
        kept.sort();
        for (_, len, file) in kept {
            if total <= max_bytes {
                break;
            }
            if file != keep && fs::remove_file(&file).is_ok() {
                total -= len;
            }
        }
    }

    fn cloud_listing_path(&self) -> PathBuf {
        self.cache_file("cloud_sources.json")
    }

    /// Every source's last listing, by source ID. Unreadable means empty.
    pub fn load_cloud_listings(&self) -> std::collections::HashMap<String, CloudListing> {
        read_json_cache(&self.cloud_listing_path())
    }

    /// Record one source's listing, keeping the others.
    pub fn save_cloud_listing(&self, id: &str, listing: CloudListing) {
        self.with_cache_lock("cloud_sources", || {
            self.ensure_cache_dir()?;
            let mut all = self.load_cloud_listings();
            all.insert(id.to_string(), listing);
            let json = serde_json::to_string(&all)?;
            let temp = self.cache_file(&format!("cloud_sources.{}.tmp", std::process::id()));
            fs::write(&temp, json)?;
            fs::rename(&temp, self.cloud_listing_path()).inspect_err(|_| {
                let _ = fs::remove_file(&temp);
            })?;
            Ok(())
        })
        .or_log("save a cloud listing");
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

    /// Directories kept on the home screen with Ctrl+D, in the order they were added.
    ///
    /// The one-keystroke twin of `[home] directories`. Kept here rather than written
    /// into the config: that file is the user's, comments and all, and may be one of
    /// several merged together.
    pub fn load_remembered_places(&self) -> Vec<PathBuf> {
        self.load_history_or_log("home_remembered")
            .into_iter()
            .map(PathBuf::from)
            .collect()
    }

    /// Keep a directory on the home screen until it is forgotten or the cache cleared.
    pub fn remember_place(&self, path: &std::path::Path) {
        let target = path.to_string_lossy().into_owned();
        self.update_history_file("home_remembered", |places| {
            if !places.contains(&target) {
                places.push(target.clone());
            }
        })
        .or_log("remember a place");
    }

    /// Stop keeping a directory on the home screen.
    pub fn forget_place(&self, path: &std::path::Path) {
        let target = path.to_string_lossy().into_owned();
        self.update_history_file("home_remembered", |places| {
            places.retain(|p| p != &target);
        })
        .or_log("forget a place");
    }
}

impl CacheManager {
    fn dataset_index_path(&self) -> PathBuf {
        self.cache_file("datasets.json")
    }

    /// What datui already knows about datasets it has measured before.
    ///
    /// A malformed or unreadable file yields an empty index: this is a cache, and
    /// failing to read it must never be worse than not having it.
    pub fn load_dataset_facts(&self) -> std::collections::HashMap<PathBuf, DatasetFacts> {
        read_json_cache(&self.dataset_index_path())
    }

    /// Merge newly measured datasets into the index.
    ///
    /// Locked and written atomically for the same reason history is: two datui
    /// instances measuring at once must not produce a torn file or lose each other's
    /// work.
    pub fn record_dataset_facts(&self, facts: &[(PathBuf, DatasetFacts)]) {
        if facts.is_empty() {
            return;
        }
        self.with_cache_lock("datasets", || {
            let mut index = self.load_dataset_facts();
            for (path, entry) in facts {
                index.insert(path.clone(), entry.clone());
            }

            // Keep the newest by modification time; an index that grows without limit
            // eventually costs more to read than the reads it saves.
            if index.len() > MAX_DATASET_FACTS {
                let mut kept: Vec<_> = index.into_iter().collect();
                kept.sort_by_key(|(_, f)| std::cmp::Reverse(f.mtime));
                kept.truncate(MAX_DATASET_FACTS);
                index = kept.into_iter().collect();
            }

            let json = serde_json::to_string(&index)?;
            let temp = self.cache_file(&format!("datasets.{}.tmp", std::process::id()));
            fs::write(&temp, json)?;
            fs::rename(&temp, self.dataset_index_path()).inspect_err(|_| {
                let _ = fs::remove_file(&temp);
            })?;
            Ok(())
        })
        .or_log("save dataset facts");
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

    /// A damaged shape file is a miss, never a panic: every truncation, and a flipped
    /// bit at every byte, of a real entry.
    #[test]
    fn a_damaged_shape_file_is_a_miss() {
        let shape = DatasetShape {
            fingerprint: "fp".to_string(),
            files: vec![
                CachedFooter {
                    schema: Some(0),
                    row_group_rows: vec![3, 4],
                    row_group_bytes: vec![100, 200],
                    column_bytes: vec![],
                },
                CachedFooter {
                    schema: None,
                    row_group_rows: vec![],
                    row_group_bytes: vec![],
                    column_bytes: vec![],
                },
            ],
            schemas: vec![vec![("a".to_string(), polars::prelude::DataType::Int64)]],
            taken_at: 1,
        };
        let good = encode_shape("s3://b/d/", &shape).unwrap();
        assert!(decode_shape(&good, "s3://b/d/", "fp").is_some());
        for cut in 0..good.len() {
            assert!(
                decode_shape(&good[..cut], "s3://b/d/", "fp").is_none(),
                "cut {cut}"
            );
        }
        for at in 0..good.len() {
            for bit in 0..8 {
                let mut bad = good.clone();
                bad[at] ^= 1 << bit;
                // Decoding may succeed with other numbers; it must not panic, and what
                // it returns must be self-consistent.
                if let Some(back) = decode_shape(&bad, "s3://b/d/", "fp") {
                    assert!(
                        back.files
                            .iter()
                            .all(|f| f.schema.is_none_or(|s| s < back.schemas.len()))
                    );
                }
            }
        }
        // A schema index past the table, and totals that overflow, are refused.
        let mut wild = shape.clone();
        wild.files[0].schema = Some(5);
        let bytes = encode_shape("s3://b/d/", &wild).unwrap();
        assert!(decode_shape(&bytes, "s3://b/d/", "fp").is_none());
        let mut huge = shape.clone();
        huge.files[0].row_group_rows = vec![usize::MAX, 1];
        let bytes = encode_shape("s3://b/d/", &huge).unwrap();
        assert!(decode_shape(&bytes, "s3://b/d/", "fp").is_none());
    }
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
        assert_ne!(
            base,
            DatasetShape::fingerprint_of([("a.parquet", 100, 7, Some("e1"))]),
            "a file removed"
        );
        assert_ne!(
            base,
            DatasetShape::fingerprint_of([
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 200, 8, Some("e2")),
                ("c.parquet", 50, 9, Some("e3")),
            ]),
            "a file added"
        );
        assert_ne!(
            base,
            DatasetShape::fingerprint_of([
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 201, 8, Some("e2")),
            ]),
            "a file resized"
        );
        assert_ne!(
            base,
            DatasetShape::fingerprint_of([
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 200, 9, Some("e2")),
            ]),
            "a file rewritten, which the stamp catches"
        );
        assert_ne!(
            base,
            DatasetShape::fingerprint_of([
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 200, 8, Some("e9")),
            ]),
            "a file rewritten within the same second at the same length, which only \
             the store's own tag catches"
        );
        assert_ne!(
            base,
            DatasetShape::fingerprint_of([
                ("a.parquet", 100, 7, Some("e1")),
                ("renamed.parquet", 200, 8, Some("e2")),
            ]),
            "and a file renamed, which reorders the positional join the cache is"
        );
    }

    /// Set a shape file's clock, so eviction order does not hang on how fast the test ran.
    fn age(cache: &super::CacheManager, path: &str, secs: u64) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(cache.dataset_shape_file(path))
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
            .unwrap();
    }

    fn shape_bytes(cache: &super::CacheManager, path: &str) -> u64 {
        std::fs::metadata(cache.dataset_shape_file(path))
            .unwrap()
            .len()
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

    /// A lookup reads its own dataset's file and no other: a small dataset beside a
    /// huge one costs what the small one weighs.
    #[test]
    fn a_lookup_reads_only_its_own_dataset() {
        let dir = tempfile::tempdir().unwrap();
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        cache.save_dataset_shape("s3://b/big/", shape("big", 1));
        cache.save_dataset_shape("s3://b/small/", shape("small", 1));
        age(&cache, "s3://b/big/", 1_000);
        let big = cache.dataset_shape_file("s3://b/big/");
        // Unreadable, so a lookup that went through it would find nothing.
        std::fs::write(&big, b"not a shape").unwrap();
        age(&cache, "s3://b/big/", 1_000);

        assert!(cache.dataset_shape("s3://b/small/", "small").is_some());
        assert_eq!(
            std::fs::metadata(&big).unwrap().modified().unwrap(),
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000),
            "the big one's file was not touched"
        );
        assert_eq!(
            cache.dataset_shape("s3://b/big/", "big"),
            None,
            "and a broken file is a miss, not an error"
        );
    }

    /// A hit dates the shape as used without rewriting it.
    #[test]
    fn a_hit_counts_as_use() {
        let dir = tempfile::tempdir().unwrap();
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        cache.save_dataset_shape("s3://b/events/", shape("f", 1));
        age(&cache, "s3://b/events/", 1_000);
        let file = cache.dataset_shape_file("s3://b/events/");
        let before = std::fs::read(&file).unwrap();

        assert!(cache.dataset_shape("s3://b/events/", "f").is_some());
        let used = std::fs::metadata(&file).unwrap().modified().unwrap();
        assert!(
            used > std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000),
            "dated now"
        );
        assert_eq!(std::fs::read(&file).unwrap(), before, "and not rewritten");

        age(&cache, "s3://b/events/", 1_000);
        assert!(cache.dataset_shape("s3://b/events/", "moved").is_none());
        assert_eq!(
            std::fs::metadata(&file).unwrap().modified().unwrap(),
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000),
            "a miss is not use"
        );
    }

    /// The cache is bounded by bytes, and what goes is what was used longest ago.
    #[test]
    fn the_least_recently_used_shapes_go_once_they_pass_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        for (i, name) in ["s3://b/a/", "s3://b/b/", "s3://b/c/"].iter().enumerate() {
            cache.save_dataset_shape(name, shape("f", 1));
            age(&cache, name, 1_000 + i as u64);
        }
        let one = shape_bytes(&cache, "s3://b/a/");
        // `a` is the oldest, but opening it makes `b` the one used longest ago.
        assert!(cache.dataset_shape("s3://b/a/", "f").is_some());

        cache.save_dataset_shape_within("s3://b/d/", &shape("f", 1), 3 * one);
        assert_eq!(cache.dataset_shapes_kept(), 3, "kept to its bound in bytes");
        assert!(cache.dataset_shape("s3://b/b/", "f").is_none(), "b went");
        for kept in ["s3://b/a/", "s3://b/c/", "s3://b/d/"] {
            assert!(cache.dataset_shape(kept, "f").is_some(), "{kept} stayed");
        }

        // One shape larger than the whole bound is still kept: it is the one in use.
        let mut huge = shape("f", 1);
        huge.files = vec![huge.files[0].clone(); 1_000];
        cache.save_dataset_shape_within("s3://b/huge/", &huge, 3 * one);
        assert_eq!(cache.dataset_shapes_kept(), 1, "everything else made room");
        assert!(cache.dataset_shape("s3://b/huge/", "f").is_some());
    }

    /// The single map the cache used to be is dropped, not read.
    #[test]
    fn the_old_single_file_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        std::fs::write(dir.path().join("dataset_shapes.json"), b"{}").unwrap();
        cache.save_dataset_shape("s3://b/events/", shape("f", 1));
        assert!(!dir.path().join("dataset_shapes.json").exists());
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
        let on_disk: u64 = walk_bytes(dir.path());
        eprintln!("on disk: {:.1} MB", on_disk as f64 / 1e6);
    }

    fn walk_bytes(dir: &std::path::Path) -> u64 {
        std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| {
                let meta = e.metadata().unwrap();
                if meta.is_dir() {
                    walk_bytes(&e.path())
                } else {
                    meta.len()
                }
            })
            .sum()
    }

    /// `datui cache clear` takes it with everything else.
    #[test]
    fn clearing_the_cache_forgets_the_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        cache.save_dataset_shape("s3://b/events/", shape("f", 1));
        assert!(cache.dataset_shape("s3://b/events/", "f").is_some());

        cache.clear_all().unwrap();
        assert_eq!(
            cache.dataset_shape("s3://b/events/", "f"),
            None,
            "nothing kept here survives being told to forget"
        );
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

    /// `datui cache clear` clears the cache — all of it. The old fixed list held two
    /// names while the directory grew histories, measurements and the hidden-source
    /// file, so the documented promises ("clears everything", "hidden until
    /// --clear-cache") were both broken.
    #[test]
    fn clear_all_removes_every_file_datui_writes() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
        for name in [
            "query_history.txt",
            "fuzzy_history.txt",
            "recents_history.txt",
            "cloud_hidden_history.txt",
            "datasets.json",
            "dataset_shapes.json",
            "cloud_sources.json",
            "datasets.lock",
            "datui.log",
            "datui.log.1",
        ] {
            std::fs::write(dir.path().join(name), b"x").expect("write");
        }
        // A foreign file and a directory are not datui's to delete.
        std::fs::write(dir.path().join("keep.parquet"), b"x").expect("write");
        std::fs::create_dir(dir.path().join("subdir")).expect("mkdir");

        cache.clear_all().expect("clear");

        let left: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            left.len(),
            2,
            "only the foreign file and the directory: {left:?}"
        );
        assert!(left.contains(&"keep.parquet".to_string()));
        assert!(left.contains(&"subdir".to_string()));
    }
}
