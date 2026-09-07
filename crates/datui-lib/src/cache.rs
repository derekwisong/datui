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
    pub fn new(app_name: &str) -> Result<Self> {
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

    /// Save history to a history file
    /// History files are dynamic (`{id}_history.txt`) and are NOT included in `CACHE_FILES`
    pub fn save_history_file(&self, history_id: &str, history: &[String]) -> Result<()> {
        self.ensure_cache_dir()?;
        let history_file = self.cache_file(&format!("{}_history.txt", history_id));

        let mut file = fs::File::create(&history_file)?;

        // Write history entries (oldest first, but we keep the most recent entries)
        for entry in history {
            writeln!(file, "{}", entry)?;
        }

        Ok(())
    }
}

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
        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let mut recents = self.load_recents();
        recents.retain(|p| p != &canonical);
        recents.insert(0, canonical);
        recents.truncate(MAX_RECENTS);

        let as_strings: Vec<String> = recents
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        let _ = self.save_history_file("recents", &as_strings);
    }
}
