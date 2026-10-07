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
