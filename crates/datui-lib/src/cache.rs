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
    /// Each kind's bytes on disk as its last sweep found them, plus what this session
    /// wrote since; shared by clones. See [`store::Store::put_all`].
    pub(crate) swept:
        std::sync::Arc<std::sync::Mutex<std::collections::HashMap<&'static str, u64>>>,
}

impl CacheManager {
    /// Create a CacheManager rooted at an explicit directory (primarily for testing).
    pub fn with_dir(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            swept: Default::default(),
        }
    }

    /// Create a CacheManager for `app_name`. `DATUI_CACHE_DIR` overrides the location;
    /// the test suite sets it so opens never record fixtures in the developer's recents.
    pub fn new(app_name: &str) -> Result<Self> {
        #[cfg(test)]
        isolate_cache();
        if let Some(dir) = std::env::var_os("DATUI_CACHE_DIR") {
            return Ok(Self::with_dir(PathBuf::from(dir)));
        }
        // A test reaching the real cache would write fixtures into the developer's recents:
        // refuse. Test binaries live under `target/<profile>/deps/`; the real binary never.
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

        Ok(Self::with_dir(cache_dir))
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

    /// Remove every cache kind's directory and line-file list (recents, histories,
    /// hidden sources, remembered places). Not lock files (another instance may hold
    /// them), the log, Data Quality copies (owned by their sessions), or views (config).
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

    /// Load a history file. An unreadable file is an error, never an empty list (the next
    /// push would write it back and lose every entry); a non-UTF-8 line is skipped alone.
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

    /// Apply `update` to a history file with the read-modify-write under an exclusive
    /// lock: atomic writes prevent corruption but not lost updates from instances
    /// opening datasets at once. The lock is awaited up to `LOCK_TIMEOUT`, far past
    /// realistic contention, then the update is dropped: history never delays what the
    /// user asked for.
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

        // Read, modify and write inside the lock. An unreadable file is left alone:
        // rewriting it from nothing would lose every entry.
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
        // Not truncate-in-place: readers would see it half-written, and concurrent writers
        // would interleave.
        atomic_write(&history_file, text.as_bytes())?;
        Ok(())
    }
}

/// What a line-file list's name ends in: `recents_history.txt`.
const HISTORY_SUFFIX: &str = "_history.txt";

/// The line-file of each terminal's last answer about its background.
const TERMINAL_MODES: &str = "terminal_modes";

/// Write `bytes` to `path` via a synced sibling temp file renamed over it, so readers,
/// other instances and crashes see the old file or the new, never part. Every cache
/// and view write goes through here. The temp name (pid plus a process-wide counter,
/// ending `.tmp`) is unique per write; sweeps remove stale ones.
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

/// Take an exclusive lock on `path` (created if missing), waiting up to `timeout`;
/// `None` if it stayed busy. Held until the returned file drops.
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

/// How long to wait for another instance rewriting a history file: a bound on a
/// wedged peer, not something realistic contention reaches. Generous because giving
/// up drops an entry and Windows locks and renames are slow (250ms failed with
/// sixteen writers); nothing waits on this write.
const LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Maximum recent paths kept: a few days of work, never needing pagination.
pub const MAX_RECENTS: usize = 50;

/// Whether a history update happened. Contended updates are abandoned rather than
/// awaited, so "no error" and "written" differ; callers and tests can tell exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryUpdate {
    /// The lock was taken and the new contents are on disk.
    Written,
    /// The lock stayed busy past the deadline, so nothing was written.
    SkippedBusy,
}

impl CacheManager {
    /// Recently opened dataset paths, most recent first: the only thing datui remembers
    /// about your data between runs, and deleting it loses only ordering.
    pub fn load_recents(&self) -> Vec<std::path::PathBuf> {
        self.load_recents_with_visits().0
    }

    /// Recents, most recent first, with each one's visits, in one read.
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

    /// Home sections the user folded (`title<TAB>1`) or opened (`title<TAB>0`), by title;
    /// unlisted sections take their default.
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

    /// The background mode `terminal` last reported, for the next start's first frame
    /// under `theme.mode = "auto"`. One line per terminal: `key<TAB>dark`.
    pub fn terminal_mode(&self, terminal: &str) -> Option<crate::config::ThemeMode> {
        self.load_history_or_log(TERMINAL_MODES)
            .iter()
            .find_map(|line| match line.split_once('\t')? {
                (key, "dark") if key == terminal => Some(crate::config::ThemeMode::Dark),
                (key, "light") if key == terminal => Some(crate::config::ThemeMode::Light),
                _ => None,
            })
    }

    /// Remember what `terminal` answered, when it differs from what is remembered.
    pub fn remember_terminal_mode(&self, terminal: &str, mode: crate::config::ThemeMode) {
        if self.terminal_mode(terminal) == Some(mode) {
            return;
        }
        let word = match mode {
            crate::config::ThemeMode::Light => "light",
            _ => "dark",
        };
        self.update_history_file(TERMINAL_MODES, |lines| {
            lines.retain(|line| line.split_once('\t').is_none_or(|(key, _)| key != terminal));
            lines.push(format!("{terminal}\t{word}"));
        })
        .or_log("remember the terminal's background");
    }

    /// Forget one recent path: an editable recents list is one people trust.
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

    /// Whether a recorded path is still worth offering. Judged by its directory: a file
    /// missing from an existing directory may be regenerated in place, but a deleted
    /// directory takes its recents with it (else a dead root lingers until fifty opens
    /// push it off). Remote paths are never checked: stat'ing one can hang, and a down
    /// share is when its recents matter most.
    fn recent_is_worth_keeping(path: &str, mounts: &crate::home::locality::Mounts) -> bool {
        let path = std::path::Path::new(path);
        // The mount table is passed in: reading it per entry meant fifty reads per open.
        if crate::home::locality::object_scheme(path).is_some() || mounts.is_network(path) {
            return true;
        }
        match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.exists(),
            _ => true,
        }
    }

    /// Record a path as most recently opened, deduplicated and capped. Failures are
    /// ignored: a convenience list must never hinder opening data. The result tells a
    /// write from a contended skip.
    pub fn push_recent(&self, path: &std::path::Path) -> HistoryUpdate {
        // A URL is recorded as given: canonicalizing is meaningless and would stat a
        // nonlocal path.
        let looks_like_url = path.to_string_lossy().contains("://");
        let stored = if looks_like_url {
            path.to_path_buf()
        } else {
            // A table inside a file of tables has no file of its own: the file is made absolute,
            // the name kept.
            crate::canonical::canonicalize(path)
                .or_else(|e| match crate::formats::members::split(path) {
                    Some((db, table)) => crate::canonical::canonicalize(&db)
                        .map(|db| crate::formats::members::place(&db, &table)),
                    None => Err(e),
                })
                .unwrap_or_else(|_| path.to_path_buf())
        };
        let entry = stored.to_string_lossy().into_owned();

        // One mount table read for the whole prune; kernel-generated, so it cannot block.
        let mounts = crate::home::locality::Mounts::current();

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
}

/// One recents line: `path<TAB>opens<TAB>last opened`, visits kept with the path so
/// forgetting it forgets them. A bare path has none.
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

/// How often and how lately a dataset was opened: zoxide-style frecency, ranking
/// Recent and lifting often-opened matches.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Visits {
    pub count: u32,
    /// Seconds since the epoch.
    pub last: u64,
}

impl Visits {
    /// Opens weighted by recency: ×4 within the hour, ×2 within the day, ×½ within the
    /// week, ×¼ after.
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

/// `recents` reordered by frecency; ties and pre-visit-count recents keep their order.
pub fn by_frecency(
    mut recents: Vec<PathBuf>,
    visits: &std::collections::HashMap<PathBuf, Visits>,
) -> Vec<PathBuf> {
    let now = unix_now();
    let score = |p: &PathBuf| visits.get(p).map_or(0.0, |v| v.frecency(now));
    recents.sort_by(|a, b| score(b).total_cmp(&score(a)));
    recents
}

/// What datui remembers about a measured dataset: a cache, not a catalog. Every field
/// is re-derivable, and the recorded size and mtime invalidate a changed dataset.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DatasetFacts {
    /// Modification time in seconds since the epoch, as a fingerprint.
    pub mtime: u64,
    /// Size in bytes, the other half of the fingerprint.
    pub size: u64,
    pub rows: Option<usize>,
    pub cols: Option<usize>,
    /// Whether `cols` is a floor (a sampled large directory), restored with the count so
    /// a sample is never shown as a total.
    #[serde(default)]
    pub cols_sampled: bool,
    /// Column names, enabling search by column before anything is read this run.
    #[serde(default)]
    pub columns: Vec<String>,
    /// What the dataset turned out to be: recorded since a remote path cannot be
    /// classified without reading, and a row should read the same in every section.
    #[serde(default)]
    pub kind: Option<crate::home::discover::EntryKind>,
    /// The build rules `kind` came from (see [`crate::home::discover::CLASSIFIER_VERSION`]); `0`
    /// in records older than this field.
    #[serde(default)]
    pub classified_by: u32,
    /// What opening costs (compression, layout, partitioning), from a footer read a
    /// remote dataset may not get twice.
    #[serde(default)]
    pub cost: crate::home::discover::Cost,
    /// What one listing of the directory found (its label), restored beside `kind` under
    /// the same classifier version.
    #[serde(
        default,
        skip_serializing_if = "crate::home::discover::Holds::is_empty"
    )]
    pub holds: crate::home::discover::Holds,
}

/// What an open learned about a dataset's files, so the next open shows columns and
/// row count without reading footers (seconds for thousands of objects). The listing,
/// which happens anyway, decides via the fingerprint whether this still holds. A cache,
/// not a catalog, like [`DatasetFacts`].
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetShape {
    /// What the files looked like when taken; a listing with a different one describes a
    /// changed dataset, and the rest is ignored.
    pub fingerprint: String,
    /// One entry per file, in listing order, which is scan order.
    pub files: Vec<CachedFooter>,
    /// The distinct schemas, by index, since thousands of files usually share one. Types
    /// are Polars' own serialization (names cannot rebuild nested types); if a Polars
    /// upgrade changes it, entries stop parsing and the cache is just empty.
    pub schemas: Vec<Vec<(String, polars::prelude::DataType)>>,
    /// Seconds since the Unix epoch, for a human reading the file.
    pub taken_at: u64,
}

/// What one file's footer said, as much as a reopen needs. It comes back as a
/// `FileFooter`, so a cached dataset goes through the same code as a fresh read.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CachedFooter {
    /// Which [`DatasetShape::schemas`] entry this file has; `None` for an unreadable
    /// footer, remembered as such so a reopen gives the same dataset.
    pub schema: Option<usize>,
    /// The rows in each of its row groups, in order.
    pub row_group_rows: Vec<usize>,
    /// Compressed bytes of each row group, kept because a note uses them, and a note
    /// present only on first open is a worse bug than a slow open.
    pub row_group_bytes: Vec<usize>,
    /// Uncompressed bytes of each schema column, in order: local footers carry them (the
    /// only source for binary widths); cloud footers leave this empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_bytes: Vec<usize>,
}

impl DatasetShape {
    /// A listing's fingerprint: file names, count, sizes, mtimes and store tags, all seen
    /// without opening anything. Names are included since remembered footers join to a
    /// fresh listing by position; ETags since size and whole-second mtime miss
    /// same-length rewrites within a second.
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

/// Point the cache and config at this test process's own scratch directories, once
/// per process (`CacheManager::new` and `ConfigManager::new` call it). Named at random,
/// not by pid (reused pids inherited old recents), and removed at exit.
#[cfg(test)]
pub(crate) fn isolate_cache() {
    // Held for the process's life; a static never drops, so an exit handler removes them.
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

/// Whether this process is a cargo-built test binary (`target/<profile>/deps/…`, or
/// `target/<triple>/<profile>/deps/` cross-built); the program itself never is.
pub(crate) fn running_as_a_cargo_test() -> bool {
    std::env::current_exe()
        .is_ok_and(|exe| cargo_test_layout(&exe, std::env::var_os("CARGO_TARGET_DIR").as_deref()))
}

/// The directory is `deps` under a `target` at most three levels up, or under cargo's
/// configured target dir, so a program installed in some `deps` is not mistaken.
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

/// A cloud source's buckets from an earlier run, shown at once while a fresh listing
/// is out.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloudListing {
    /// What the source pointed at when listed; a mismatch means another server, ignored.
    pub fingerprint: String,
    pub buckets: Vec<String>,
    /// Seconds since the Unix epoch.
    pub listed_at: u64,
}

/// Dataset shapes by path, at their listing's fingerprint. Bounded by bytes: shape
/// size follows file count (842k files is megabytes), so a count bound would let a
/// few huge datasets evict a small one in use.
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

/// The footers a dataset's count has read, each by [`file_identity`], so a recount
/// reads only new files and a stopped count keeps its progress. By file, unlike
/// [`DatasetShape`], so it stays right for a changed dataset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileFooters {
    /// The distinct schemas, as in [`DatasetShape::schemas`].
    pub schemas: Vec<Vec<(String, polars::prelude::DataType)>>,
    /// Each file's identity, and its footer.
    pub files: Vec<(u64, CachedFooter)>,
}

/// A file's identity for [`FileFooters`]: key, size, mtime and store tag, any of which
/// changes on rewrite.
pub fn file_identity(key: &str, size: u64, stamp: u64, etag: Option<&str>) -> u64 {
    let mut hasher = StableHasher::default();
    hasher.bytes(key.as_bytes()).u64(size).u64(stamp);
    match etag {
        Some(etag) => hasher.u64(1).bytes(etag.as_bytes()),
        None => hasher.u64(0),
    };
    hasher.finish()
}

/// [`FileFooters`] by dataset path; empty fingerprint, as each file checks itself.
pub(crate) struct FileFootersKind;

impl Kind for FileFootersKind {
    const DIR: &'static str = "file_footers";
    const EXT: &'static str = "footers";
    const VERSION: u16 = 1;
    const BUDGET: u64 = 64 << 20;
    type Value = FileFooters;

    fn encode(footers: &FileFooters) -> Result<Vec<u8>> {
        let shape = DatasetShape {
            fingerprint: String::new(),
            files: footers.files.iter().map(|(_, f)| f.clone()).collect(),
            schemas: footers.schemas.clone(),
            taken_at: 0,
        };
        let mut out = encode_shape(&shape)?;
        for (identity, _) in &footers.files {
            out.extend_from_slice(&identity.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(payload: &[u8]) -> Option<FileFooters> {
        // The identities follow the shape, eight bytes a file.
        let count = payload.len().checked_sub(4)?;
        let (len, rest) = payload.split_first_chunk::<4>()?;
        let header = usize::try_from(u32::from_le_bytes(*len)).ok()?;
        let mut body = rest.get(header..)?;
        let files = usize::try_from(take_varint(&mut body)?).ok()?;
        let ids = files.checked_mul(8)?;
        if ids > count {
            return None;
        }
        let (shape, identities) = payload.split_at(payload.len() - ids);
        let shape = decode_shape(shape)?;
        (shape.files.len() == files).then(|| FileFooters {
            schemas: shape.schemas,
            files: identities
                .as_chunks::<8>()
                .0
                .iter()
                .map(|id| u64::from_le_bytes(*id))
                .zip(shape.files)
                .collect(),
        })
    }
}

/// What home measured of each dataset, by path; records carry their own size and
/// mtime, so the store fingerprint is empty.
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

/// A shape's header, read before its footers: fingerprint, schemas, time. JSON for the
/// Polars-serialized schemas; the bulk per-file footers follow as varints.
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
    // Each value is at least a byte: a longer length means a broken file, not an
    // allocation.
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
    // Dataset totals must fit, so no later sum overflows on a damaged file.
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
    #[cfg(test)]
    pub fn dataset_shapes_kept(&self) -> usize {
        Store::<Shapes>::new(self).len()
    }

    /// The remembered shape of a dataset if `fingerprint` still matches (a mismatched
    /// entry has no correct use). A hit counts as use, so unchanging datasets stay.
    pub fn dataset_shape(&self, path: &str, fingerprint: &str) -> Option<DatasetShape> {
        Store::<Shapes>::new(self).get(path, fingerprint)
    }

    /// Whether any shape is kept for `path`: one stat.
    pub fn has_dataset_shape(&self, path: &str) -> bool {
        Store::<Shapes>::new(self).file(path).exists()
    }

    /// Remember one dataset's shape, keeping the others while they fit the budget.
    pub fn save_dataset_shape(&self, path: &str, shape: DatasetShape) {
        Store::<Shapes>::new(self).put(path, &shape.fingerprint, &shape);
    }

    /// The footers counts of the dataset at `path` have read, by file.
    pub fn file_footers(&self, path: &str) -> Option<FileFooters> {
        Store::<FileFootersKind>::new(self).get(path, "")
    }

    /// Remember the footers a count of the dataset at `path` read, by file.
    pub fn save_file_footers(&self, path: &str, footers: &FileFooters) {
        Store::<FileFootersKind>::new(self).put(path, "", footers);
    }

    /// The last listing of cloud source `id`, if it was taken at `fingerprint`.
    pub fn cloud_listing(&self, id: &str, fingerprint: &str) -> Option<CloudListing> {
        Store::<CloudListings>::new(self).get(id, fingerprint)
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

    /// Whether Delete on its heading hid the bundled Example datasets (a user's
    /// `examples.toml` still shows).
    pub fn examples_hidden(&self) -> bool {
        !self.load_history_or_log("examples_hidden").is_empty()
    }

    /// Hide the bundled Example datasets until the cache is cleared.
    pub fn hide_examples(&self) {
        self.save_history_file("examples_hidden", &["hidden".to_string()])
            .or_log("hide the example datasets");
    }

    /// The directories Ctrl+D kept here before 0.4.0 (now in `catalog.toml`), in added
    /// order.
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
    /// Every remembered dataset fact, not counted as use. Unreadable records are absent:
    /// failing to read a cache must never be worse than lacking it.
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

    /// Count these records as used (shown on home); least recently used go first past
    /// the budget.
    pub fn touch_dataset_facts<'a>(&self, paths: impl IntoIterator<Item = &'a Path>) {
        let store = Store::<Facts>::new(self);
        for path in paths.into_iter().filter_map(Path::to_str) {
            store.touch(path);
        }
    }

    /// Record newly measured datasets, one file each; non-UTF-8 paths are skipped.
    pub fn record_dataset_facts(&self, facts: &[(PathBuf, DatasetFacts)]) {
        Store::<Facts>::new(self).put_all(
            facts
                .iter()
                .filter_map(|(path, facts)| Some((path.to_str()?, "", facts))),
        );
    }

    /// Run `work` under the named cache lock, or skip it if contended past the deadline.
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
mod harness_tests;

#[cfg(test)]
mod recents_pruning_tests;

#[cfg(test)]
mod dataset_shape_tests;

/// The same checks for every kind the [`Store`] holds.
#[cfg(test)]
mod store_harness_tests;

#[cfg(test)]
mod facts_compat_tests;
