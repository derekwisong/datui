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
