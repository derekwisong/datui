//! Recall of submitted values for [`super::TextInput`]. Each history's id names its
//! cache file (query, SQL and fuzzy keep separate lists); loaded lazily on first use.
//! Reads and writes go through [`CacheManager`] under a lock across read-modify-write,
//! so concurrent instances merge.

use color_eyre::Result;

use crate::cache::CacheManager;

/// Append an entry unless it repeats the previous one (only consecutive duplicates are
/// dropped).
pub fn push_entry(entries: &mut Vec<String>, entry: String) {
    if entries.last() == Some(&entry) {
        return;
    }
    entries.push(entry);
}

/// Stands for a line break on disk. The file holds one entry per line, and a
/// statement can span several; U+2028 is Unicode's own line separator, and
/// nothing typed into a query plausibly holds one.
const LINE_BREAK_ON_DISK: char = '\u{2028}';

fn to_disk(entry: &str) -> String {
    entry.replace('\n', &LINE_BREAK_ON_DISK.to_string())
}

fn from_disk(entry: String) -> String {
    if entry.contains(LINE_BREAK_ON_DISK) {
        entry.replace(LINE_BREAK_ON_DISK, "\n")
    } else {
        entry
    }
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
            self.entries = cache
                .load_history_file(id)?
                .into_iter()
                .map(from_disk)
                .collect();
            self.loaded = true;
        }
        Ok(())
    }

    /// Record `value` as newest and persist, merged with what is on disk so other instances'
    /// entries survive.
    pub fn remember(&mut self, value: &str, cache: &CacheManager) -> Result<()> {
        let Some(id) = self.id.clone() else {
            return Ok(());
        };
        if value.is_empty() {
            return Ok(());
        }
        push_entry(&mut self.entries, value.to_string());
        trim(&mut self.entries, self.limit);

        let entry = to_disk(value);
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

    /// Step to an older entry, returning it; `current` is stashed on the first step so
    /// walking back returns to it.
    pub fn older(&mut self, current: &str, cache: Option<&CacheManager>) -> Option<String> {
        self.id.as_ref()?;
        if !self.loaded {
            let cache = cache?;
            self.ensure_loaded(cache)
                .inspect_err(|e| log::warn!(target: "datui", "read input history: {e:#}"))
                .ok()?;
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
    fn a_statement_over_several_lines_is_one_entry_on_disk() {
        let sql = "SELECT *\nFROM df";
        assert!(!to_disk(sql).contains('\n'));
        assert_eq!(from_disk(to_disk(sql)), sql);
        assert_eq!(from_disk("select a".to_string()), "select a");
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
