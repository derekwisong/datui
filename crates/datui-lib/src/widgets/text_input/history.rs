//! Recall of previously submitted values for [`super::TextInput`].
//!
//! Each history has an id, which is also the name of the cache file it is
//! persisted to, so the query, SQL and fuzzy inputs each keep their own list.
//! Loading is lazy: nothing touches the disk until the user actually walks back
//! through the history or submits a value.
//!
//! Reading and writing go through [`CacheManager`], which holds a lock across
//! the whole read-modify-write. Two datui instances submitting a query at the
//! same moment then merge instead of one overwriting the other.

use color_eyre::Result;

use crate::cache::CacheManager;

/// Append an entry, skipping it when it repeats the previous one.
///
/// Only consecutive duplicates are dropped: a value the user returns to after
/// trying something else is worth its own slot in the list.
pub fn push_entry(entries: &mut Vec<String>, entry: String) {
    if entries.last() == Some(&entry) {
        return;
    }
    entries.push(entry);
}

/// Drop the oldest entries until at most `limit` remain.
fn trim(entries: &mut Vec<String>, limit: usize) {
    let excess = entries.len().saturating_sub(limit);
    entries.drain(..excess);
}

/// One input's recall state: the entries, where the user is in them, and the
/// value that was being edited before they started walking back.
#[derive(Debug, Clone, Default)]
pub(super) struct InputHistory {
    pub id: Option<String>,
    entries: Vec<String>,
    /// Index into `entries` while walking; `None` means editing a fresh value.
    index: Option<usize>,
    /// The in-progress value stashed when the walk began.
    stash: Option<String>,
    pub limit: usize,
    loaded: bool,
}

impl InputHistory {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            ..Self::default()
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.id.is_some()
    }

    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    /// Load entries from the cache the first time they are needed.
    pub fn ensure_loaded(&mut self, cache: &CacheManager) -> Result<()> {
        if self.loaded {
            return Ok(());
        }
        if let Some(id) = &self.id {
            self.entries = cache.load_history_file(id)?;
            self.loaded = true;
        }
        Ok(())
    }

    /// Record `value` as the newest entry and persist the list.
    ///
    /// The persisted copy is re-derived from whatever is on disk, so an entry
    /// added by another running instance survives this write.
    pub fn remember(&mut self, value: &str, cache: &CacheManager) -> Result<()> {
        let Some(id) = self.id.clone() else {
            return Ok(());
        };
        if value.is_empty() {
            return Ok(());
        }
        push_entry(&mut self.entries, value.to_string());
        trim(&mut self.entries, self.limit);

        let entry = value.to_string();
        let limit = self.limit;
        cache.update_history_file(&id, move |entries| {
            push_entry(entries, entry);
            trim(entries, limit);
        })?;
        Ok(())
    }

    /// Add an entry without touching the cache. Test support.
    #[cfg(test)]
    pub fn seed(&mut self, entry: String) {
        self.loaded = true;
        push_entry(&mut self.entries, entry);
    }

    /// Stop walking the history without changing the current value.
    pub fn reset_position(&mut self) {
        self.index = None;
        self.stash = None;
    }

    /// Step to an older entry, returning the value to show.
    ///
    /// `current` is stashed on the first step so that walking back down returns
    /// to it.
    pub fn older(&mut self, current: &str, cache: Option<&CacheManager>) -> Option<String> {
        self.id.as_ref()?;
        if !self.loaded {
            let cache = cache?;
            self.ensure_loaded(cache).ok()?;
        }
        if self.entries.is_empty() {
            return None;
        }
        if self.index.is_none() {
            self.stash = Some(current.to_string());
        }
        let index = match self.index {
            Some(current) => current.saturating_sub(1),
            None => self.entries.len() - 1,
        };
        self.index = Some(index);
        self.entries.get(index).cloned()
    }

    /// Step to a newer entry, returning the value to show. Stepping past the
    /// newest entry restores the stashed in-progress value.
    pub fn newer(&mut self) -> Option<String> {
        self.id.as_ref()?;
        let index = self.index?;
        if index + 1 >= self.entries.len() {
            let stashed = self.stash.take();
            self.index = None;
            return stashed;
        }
        self.index = Some(index + 1);
        self.entries.get(index + 1).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history_with(entries: &[&str]) -> InputHistory {
        let mut history = InputHistory::new(100);
        history.id = Some("test".to_string());
        history.entries = entries.iter().map(|s| s.to_string()).collect();
        history.loaded = true;
        history
    }

    #[test]
    fn push_entry_skips_consecutive_duplicates_only() {
        let mut entries = Vec::new();
        push_entry(&mut entries, "query1".to_string());
        push_entry(&mut entries, "query2".to_string());
        push_entry(&mut entries, "query2".to_string());
        push_entry(&mut entries, "query1".to_string());
        assert_eq!(entries, vec!["query1", "query2", "query1"]);
    }

    #[test]
    fn walking_back_and_forward_returns_to_the_draft() {
        let mut history = history_with(&["one", "two", "three"]);
        assert_eq!(history.older("draft", None).as_deref(), Some("three"));
        assert_eq!(history.older("draft", None).as_deref(), Some("two"));
        assert_eq!(history.older("draft", None).as_deref(), Some("one"));
        // Already at the oldest entry.
        assert_eq!(history.older("draft", None).as_deref(), Some("one"));
        assert_eq!(history.newer().as_deref(), Some("two"));
        assert_eq!(history.newer().as_deref(), Some("three"));
        assert_eq!(history.newer().as_deref(), Some("draft"));
        assert_eq!(history.newer(), None);
    }

    #[test]
    fn walking_forward_without_walking_back_does_nothing() {
        let mut history = history_with(&["one"]);
        assert_eq!(history.newer(), None);
    }

    #[test]
    fn history_is_inert_without_an_id() {
        let mut history = InputHistory::new(100);
        assert!(!history.is_enabled());
        assert_eq!(history.older("draft", None), None);
        assert_eq!(history.newer(), None);
    }

    #[test]
    fn an_empty_history_has_nothing_to_walk() {
        let mut history = history_with(&[]);
        assert_eq!(history.older("draft", None), None);
    }

    #[test]
    fn unloaded_history_needs_a_cache_to_walk() {
        let mut history = InputHistory::new(100);
        history.id = Some("test".to_string());
        assert_eq!(history.older("draft", None), None);
    }

    fn temp_cache() -> (tempfile::TempDir, CacheManager) {
        let dir = tempfile::tempdir().expect("temp dir");
        let cache = CacheManager::with_dir(dir.path().to_path_buf());
        (dir, cache)
    }

    #[test]
    fn remembered_entries_survive_a_reload() {
        let (_dir, cache) = temp_cache();
        let mut history = InputHistory::new(100);
        history.id = Some("query".to_string());
        history.remember("select one", &cache).expect("remember");
        history.remember("select two", &cache).expect("remember");

        let mut reloaded = InputHistory::new(100);
        reloaded.id = Some("query".to_string());
        reloaded.ensure_loaded(&cache).expect("load");
        assert_eq!(reloaded.entries(), ["select one", "select two"]);
    }

    #[test]
    fn empty_values_are_not_remembered() {
        let (_dir, cache) = temp_cache();
        let mut history = InputHistory::new(100);
        history.id = Some("query".to_string());
        history.remember("", &cache).expect("remember");
        assert!(history.entries().is_empty());
    }

    #[test]
    fn the_limit_drops_the_oldest_entries() {
        let (_dir, cache) = temp_cache();
        let mut history = InputHistory::new(2);
        history.id = Some("query".to_string());
        for entry in ["a", "b", "c"] {
            history.remember(entry, &cache).expect("remember");
        }
        assert_eq!(history.entries(), ["b", "c"]);

        let mut reloaded = InputHistory::new(2);
        reloaded.id = Some("query".to_string());
        reloaded.ensure_loaded(&cache).expect("load");
        assert_eq!(reloaded.entries(), ["b", "c"]);
    }

    #[test]
    fn an_entry_written_by_another_instance_is_kept() {
        let (_dir, cache) = temp_cache();
        cache
            .save_history_file("query", &["from elsewhere".to_string()])
            .expect("seed file");

        let mut history = InputHistory::new(100);
        history.id = Some("query".to_string());
        history.remember("mine", &cache).expect("remember");

        let stored = cache.load_history_file("query").expect("load");
        assert_eq!(stored, ["from elsewhere", "mine"]);
    }

    #[test]
    fn a_history_without_an_id_is_never_persisted() {
        let (_dir, cache) = temp_cache();
        let mut history = InputHistory::new(100);
        history.remember("ignored", &cache).expect("remember");
        assert!(history.entries().is_empty());
    }

    #[test]
    fn resetting_the_position_drops_the_stash() {
        let mut history = history_with(&["one"]);
        history.older("draft", None);
        history.reset_position();
        assert_eq!(history.newer(), None);
    }
}
