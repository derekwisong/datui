use color_eyre::Result;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Registry of known cache files
const CACHE_FILES: &[&str] = &["query_history.txt", "dataset_shapes.json"];

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
    /// Note: Templates are stored in config directory, not cache, so they are not cleared here.
    /// Note: History files (e.g., `{id}_history.txt`) are dynamic and excluded from `clear_all()`.
    /// They can be cleared individually via `clear_file()` if needed.
    pub fn clear_all(&self) -> Result<()> {
        for filename in CACHE_FILES {
            let file_path = self.cache_file(filename);
            if file_path.exists()
                && let Err(_e) = fs::remove_file(&file_path)
            {
                // Silently ignore cache file removal failures — this runs in a TUI
                // context where stderr output would corrupt the terminal display.
            }
        }

        Ok(())
    }

    /// Load history from a history file
    /// History files are dynamic (`{id}_history.txt`) and are NOT included in `CACHE_FILES`
    pub fn load_history_file(&self, history_id: &str) -> Result<Vec<String>> {
        let history_file = self.cache_file(&format!("{}_history.txt", history_id));

        if !history_file.exists() {
            return Ok(Vec::new());
        }

        let file = fs::File::open(&history_file)?;
        let reader = BufReader::new(file);
        let mut history = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                history.push(line);
            }
        }

        Ok(history)
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
        let lock = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        let deadline = std::time::Instant::now() + LOCK_TIMEOUT;
        let mut held = false;
        loop {
            if lock.try_lock_exclusive().is_ok() {
                held = true;
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        if !held {
            return Ok(HistoryUpdate::SkippedBusy);
        }

        // Read, modify and write all inside the lock; the whole point is that another
        // instance cannot land between the read and the write.
        let mut entries = self.load_history_file(history_id).unwrap_or_default();
        update(&mut entries);
        let result = self.save_history_file(history_id, &entries);

        // Released explicitly, though dropping the file would do it too.
        let _ = FileExt::unlock(&lock);
        result.map(|()| HistoryUpdate::Written)
    }

    /// Save history to a history file
    /// History files are dynamic (`{id}_history.txt`) and are NOT included in `CACHE_FILES`
    pub fn save_history_file(&self, history_id: &str, history: &[String]) -> Result<()> {
        self.ensure_cache_dir()?;
        let history_file = self.cache_file(&format!("{}_history.txt", history_id));

        // Write to a sibling and rename over the target. Truncating in place leaves the
        // file readable in a half-written state, and two datui instances writing at
        // once interleave into a single corrupt file — entries torn mid-path, or two
        // paths concatenated onto one line. A rename is atomic on the same filesystem,
        // so a reader sees either the old file or the new one, and the last writer
        // wins cleanly instead of both losing.
        let temp_file = self.cache_file(&format!(
            "{}_history.{}.tmp",
            history_id,
            std::process::id()
        ));

        {
            let mut file = fs::File::create(&temp_file)?;
            // Oldest first, but we keep the most recent entries.
            for entry in history {
                writeln!(file, "{}", entry)?;
            }
            file.sync_all()?;
        }

        fs::rename(&temp_file, &history_file).inspect_err(|_| {
            let _ = fs::remove_file(&temp_file);
        })?;

        Ok(())
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
        self.load_history_file("recents")
            .unwrap_or_default()
            .into_iter()
            .map(std::path::PathBuf::from)
            .collect()
    }

    /// Which home-screen sections the user folded or opened, by title.
    ///
    /// One line per section, `title<TAB>1` for folded and `title<TAB>0` for opened.
    /// A section not listed takes its own default.
    pub fn load_folds(&self) -> std::collections::HashMap<String, bool> {
        self.load_history_file("home_folds")
            .unwrap_or_default()
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
        let _ = self.save_history_file("home_folds", &lines);
    }

    /// Forget a single recently opened path.
    ///
    /// A recents list you cannot edit is one people stop trusting: an experiment, a
    /// file that would not open, something private — all land there, and clearing the
    /// whole cache to remove one is too blunt.
    pub fn forget_recent(&self, path: &std::path::Path) {
        let target = path.to_string_lossy().into_owned();
        let _ = self.update_history_file("recents", |recents| {
            recents.retain(|p| p != &target);
        });
    }

    /// Forget several recently opened paths at once: every recent under one place.
    pub fn forget_recents(&self, paths: &[std::path::PathBuf]) {
        let targets: Vec<String> = paths
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        let _ = self.update_history_file("recents", |recents| {
            recents.retain(|p| !targets.contains(p));
        });
    }

    /// Forget every recently opened path, leaving other caches alone.
    pub fn clear_recents(&self) {
        let _ = self.update_history_file("recents", |recents| recents.clear());
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
            path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
        };
        let entry = stored.to_string_lossy().into_owned();

        // One read of the mount table for the whole prune. It is a kernel-generated
        // file, so reading it cannot block on the filesystems it describes.
        let mounts = crate::locality::Mounts::current();

        self.update_history_file("recents", |recents| {
            recents.retain(|p| p != &entry);
            recents.insert(0, entry.clone());
            recents.retain(|p| Self::recent_is_worth_keeping(p, &mounts));
            recents.truncate(MAX_RECENTS);
        })
        .unwrap_or(HistoryUpdate::SkippedBusy)
    }
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
    /// Whether `cols` is a floor rather than a total: the folder was too large to read
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
    /// What one listing of the folder found in it, which is what its label says.
    /// Restored beside `kind` and gated by the same classifier version: both are what
    /// looking into the folder produced, and a build that classified differently
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
}

/// Whether this process is a test binary cargo built, which is where `cargo test`
/// and `cargo bench` put everything: `target/<profile>/deps/<crate>-<hash>`. The
/// program itself is `target/<profile>/datui`, and an installed one is nowhere near.
fn running_as_a_cargo_test() -> bool {
    std::env::current_exe().is_ok_and(|exe| exe.components().any(|part| part.as_os_str() == "deps"))
}

/// Entries kept in the dataset index.
///
/// Large enough to cover everywhere someone actually works, small enough that the
/// file stays trivial to read and rewrite.
pub const MAX_DATASET_FACTS: usize = 4096;

/// Dataset shapes kept. Each one holds a row count per file, so a few hundred large
/// datasets is already a sizeable file; this is the point where speed stops being worth
/// the bytes.
pub const MAX_DATASET_SHAPES: usize = 256;

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

impl CacheManager {
    fn dataset_shape_path(&self) -> PathBuf {
        self.cache_file("dataset_shapes.json")
    }

    /// Every dataset shape datui has kept, by the path or URL it was opened as.
    /// Unreadable means empty — a cache that cannot be read is one that has nothing to
    /// say, not an error worth stopping an open for.
    pub fn load_dataset_shapes(&self) -> std::collections::HashMap<String, DatasetShape> {
        let Ok(text) = fs::read_to_string(self.dataset_shape_path()) else {
            return Default::default();
        };
        serde_json::from_str(&text).unwrap_or_default()
    }

    /// What datui remembers about one dataset, if the fingerprint still matches.
    ///
    /// Taking the fingerprint as an argument rather than returning the entry and
    /// letting the caller check is deliberate: an entry whose fingerprint has moved on
    /// describes a dataset that no longer exists, and there is no use for it that is
    /// not a mistake.
    ///
    /// A hit is recorded as use, so what ages out is what has not been opened in
    /// longest rather than what has not been rebuilt in longest.
    pub fn dataset_shape(&self, path: &str, fingerprint: &str) -> Option<DatasetShape> {
        let shape = self
            .load_dataset_shapes()
            .remove(path)
            .filter(|shape| shape.fingerprint == fingerprint)?;
        // A hit counts as use. Without this the clock only moves on a miss, so the
        // entries that age out first are the datasets that never change — the ones with
        // a perfect hit rate and the most to gain — while a dataset rewritten every day
        // keeps resetting its own and stays forever.
        self.touch_dataset_shape(path);
        Some(shape)
    }

    /// Mark a shape as used just now, so eviction sees it as recent.
    ///
    /// Best effort: a shape that cannot be re-dated is still a shape that can be used,
    /// and failing the open over it would be absurd.
    fn touch_dataset_shape(&self, path: &str) {
        let _ = self.with_cache_lock("dataset_shapes", || {
            let mut all = self.load_dataset_shapes();
            if let Some(shape) = all.get_mut(path) {
                shape.taken_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default();
            }
            let json = serde_json::to_string(&all)?;
            let temp = self.cache_file(&format!("dataset_shapes.{}.tmp", std::process::id()));
            fs::write(&temp, json)?;
            fs::rename(&temp, self.dataset_shape_path()).inspect_err(|_| {
                let _ = fs::remove_file(&temp);
            })?;
            Ok(())
        });
    }

    /// Remember one dataset's shape, keeping the others.
    ///
    /// Bounded the same way the dataset index is: a cache that grows without limit
    /// stops being one. The oldest entries go first, since what someone opened least
    /// recently is what they are least likely to open next.
    pub fn save_dataset_shape(&self, path: &str, shape: DatasetShape) {
        let _ = self.with_cache_lock("dataset_shapes", || {
            self.ensure_cache_dir()?;
            let mut all = self.load_dataset_shapes();
            all.insert(path.to_string(), shape);
            if all.len() > MAX_DATASET_SHAPES {
                let mut by_age: Vec<(String, u64)> =
                    all.iter().map(|(k, v)| (k.clone(), v.taken_at)).collect();
                by_age.sort_by_key(|(_, at)| *at);
                for (old, _) in by_age.into_iter().take(all.len() - MAX_DATASET_SHAPES) {
                    all.remove(&old);
                }
            }
            let json = serde_json::to_string(&all)?;
            let temp = self.cache_file(&format!("dataset_shapes.{}.tmp", std::process::id()));
            fs::write(&temp, json)?;
            fs::rename(&temp, self.dataset_shape_path()).inspect_err(|_| {
                let _ = fs::remove_file(&temp);
            })?;
            Ok(())
        });
    }

    fn cloud_listing_path(&self) -> PathBuf {
        self.cache_file("cloud_sources.json")
    }

    /// Every source's last listing, by source ID. Unreadable means empty.
    pub fn load_cloud_listings(&self) -> std::collections::HashMap<String, CloudListing> {
        let Ok(text) = fs::read_to_string(self.cloud_listing_path()) else {
            return Default::default();
        };
        serde_json::from_str(&text).unwrap_or_default()
    }

    /// Record one source's listing, keeping the others.
    pub fn save_cloud_listing(&self, id: &str, listing: CloudListing) {
        let _ = self.with_cache_lock("cloud_sources", || {
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
        });
    }

    /// Source IDs hidden from the home screen with Delete.
    pub fn load_hidden_cloud_sources(&self) -> Vec<String> {
        self.load_history_file("cloud_hidden").unwrap_or_default()
    }

    /// Public buckets and containers read on an earlier run, as URLs.
    pub fn load_public_places(&self) -> Vec<String> {
        self.load_history_file("cloud_public").unwrap_or_default()
    }

    /// Remember a public bucket or container, so it is listed with the public datasets.
    pub fn remember_public_place(&self, url: &str) {
        let url = url.to_string();
        let _ = self.update_history_file("cloud_public", |places| {
            if !places.contains(&url) {
                places.push(url.clone());
            }
        });
    }

    /// Hide a source from the home screen until the cache is cleared.
    pub fn hide_cloud_source(&self, id: &str) {
        let id = id.to_string();
        let _ = self.update_history_file("cloud_hidden", |hidden| {
            if !hidden.contains(&id) {
                hidden.push(id.clone());
            }
        });
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
        let Ok(text) = fs::read_to_string(self.dataset_index_path()) else {
            return Default::default();
        };
        serde_json::from_str::<std::collections::HashMap<PathBuf, DatasetFacts>>(&text)
            .unwrap_or_default()
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
        let _ = self.with_cache_lock("datasets", || {
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
        });
    }

    /// Run `work` holding the named cache lock, or skip it if the lock is contended
    /// past the deadline.
    fn with_cache_lock<F>(&self, name: &str, work: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        use fs2::FileExt;

        self.ensure_cache_dir()?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(self.cache_file(&format!("{name}.lock")))?;

        let deadline = std::time::Instant::now() + LOCK_TIMEOUT;
        loop {
            if lock.try_lock_exclusive().is_ok() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        let result = work();
        let _ = FileExt::unlock(&lock);
        result
    }
}

#[cfg(test)]
mod harness_tests {
    /// This test binary is one cargo built into `deps`, so the refusal in
    /// `CacheManager::new` is armed here. That it is armed is the whole guarantee: a
    /// test that reaches `new` without `DATUI_CACHE_DIR` set stops instead of writing
    /// its fixtures into the developer's own recents.
    #[test]
    fn a_cargo_test_binary_is_recognized() {
        assert!(super::running_as_a_cargo_test());
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
        let dataset = dataset.canonicalize().expect("canonicalize");

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
        let dataset = dataset.canonicalize().expect("canonicalize");
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
                },
                CachedFooter {
                    schema: Some(0),
                    row_group_rows: vec![20],
                    row_group_bytes: vec![2_000],
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
        let cache = CacheManager::with_dir(dir.path().to_path_buf());
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

    /// The cache is bounded, oldest first.
    #[test]
    fn the_oldest_shapes_are_the_ones_that_go() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::with_dir(dir.path().to_path_buf());
        for i in 0..(MAX_DATASET_SHAPES + 10) {
            cache.save_dataset_shape(&format!("s3://b/{i}/"), shape("f", i as u64));
        }
        let all = cache.load_dataset_shapes();
        assert_eq!(all.len(), MAX_DATASET_SHAPES, "kept to its bound");
        assert!(
            !all.contains_key("s3://b/0/"),
            "and what went is what was opened longest ago"
        );
        assert!(
            all.contains_key(&format!("s3://b/{}/", MAX_DATASET_SHAPES + 9)),
            "while the most recent is still there"
        );
    }

    /// `--clear-cache` takes it with everything else.
    #[test]
    fn clearing_the_cache_forgets_the_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CacheManager::with_dir(dir.path().to_path_buf());
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
}
