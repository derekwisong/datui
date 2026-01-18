use color_eyre::Result;
use std::fs;
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
    /// Note: Templates are stored in config directory, not cache, so they are not cleared here
    pub fn clear_all(&self) -> Result<()> {
        for filename in CACHE_FILES {
            let file_path = self.cache_file(filename);
            if file_path.exists() {
                if let Err(e) = fs::remove_file(&file_path) {
                    eprintln!("Warning: Could not remove cache file {}: {}", filename, e);
                }
            }
        }

        Ok(())
    }
}
