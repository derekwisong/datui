use color_eyre::Result;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Registry of known cache files
const CACHE_FILES: &[&str] = &["query_history.txt"];

/// Manages cache directory and cache file operations
#[derive(Clone)]
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
            if file_path.exists() {
                if let Err(_e) = fs::remove_file(&file_path) {
                    // Silently ignore cache file removal failures — this runs in a TUI
                    // context where stderr output would corrupt the terminal display.
                }
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
    pub fn update_history_file<F>(&self, history_id: &str, update: F) -> Result<()>
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
            return Ok(());
        }

        // Read, modify and write all inside the lock; the whole point is that another
        // instance cannot land between the read and the write.
        let mut entries = self.load_history_file(history_id).unwrap_or_default();
        update(&mut entries);
        let result = self.save_history_file(history_id, &entries);

        // Released explicitly, though dropping the file would do it too.
        let _ = FileExt::unlock(&lock);
        result
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
/// The critical section is a read and an atomic rewrite of a small file, so this is
/// orders of magnitude more than contention actually needs. It exists to bound the
/// wait if a peer wedges, not to be reached.
const LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

/// Maximum number of recently opened paths kept. Enough to span a few days of work;
/// small enough that the home screen never has to paginate it.
pub const MAX_RECENTS: usize = 50;

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

    /// Forget every recently opened path, leaving other caches alone.
    pub fn clear_recents(&self) {
        let _ = self.update_history_file("recents", |recents| recents.clear());
    }

    /// Record a path as most recently opened, de-duplicating and capping the list.
    ///
    /// Failures are ignored: not being able to write a convenience list must never
    /// interfere with opening data.
    pub fn push_recent(&self, path: &std::path::Path) {
        // A URL is recorded exactly as given: canonicalising one is meaningless, and
        // it would also stat a path that does not exist locally.
        let looks_like_url = path.to_string_lossy().contains("://");
        let stored = if looks_like_url {
            path.to_path_buf()
        } else {
            path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
        };
        let entry = stored.to_string_lossy().into_owned();

        let _ = self.update_history_file("recents", |recents| {
            recents.retain(|p| p != &entry);
            recents.insert(0, entry.clone());
            recents.truncate(MAX_RECENTS);
        });
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
    /// Column names, which is what makes searching by column possible before
    /// anything has been read this run.
    #[serde(default)]
    pub columns: Vec<String>,
    /// What the dataset turned out to be. Recorded rather than re-derived, because a
    /// remote path cannot be classified without reading it, and a row that says
    /// `hive` under its own directory should not say something else under Recent.
    #[serde(default)]
    pub kind: Option<crate::discover::EntryKind>,
    /// What opening it will cost: compression, layout, partitioning. Worth keeping
    /// for the same reason the row count is — it came from a footer read that a
    /// remote dataset may not get a second chance at.
    #[serde(default)]
    pub cost: crate::discover::Cost,
}

/// Entries kept in the dataset index.
///
/// Large enough to cover everywhere someone actually works, small enough that the
/// file stays trivial to read and rewrite.
pub const MAX_DATASET_FACTS: usize = 4096;

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
