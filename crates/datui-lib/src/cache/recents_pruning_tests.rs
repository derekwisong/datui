use super::*;

fn cache() -> (CacheManager, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    (CacheManager::with_dir(dir.path().to_path_buf()), dir)
}

/// Each terminal keeps its own last answer; a new answer replaces the old.
#[test]
fn a_terminals_last_answer_is_kept_per_terminal() {
    use crate::config::ThemeMode;
    let (cache, _keep) = cache();
    assert_eq!(cache.terminal_mode("WezTerm"), None);
    cache.remember_terminal_mode("WezTerm", ThemeMode::Light);
    cache.remember_terminal_mode("tmux", ThemeMode::Dark);
    assert_eq!(cache.terminal_mode("WezTerm"), Some(ThemeMode::Light));
    assert_eq!(cache.terminal_mode("tmux"), Some(ThemeMode::Dark));
    cache.remember_terminal_mode("WezTerm", ThemeMode::Dark);
    assert_eq!(cache.terminal_mode("WezTerm"), Some(ThemeMode::Dark));
    assert_eq!(cache.terminal_mode("tmux"), Some(ThemeMode::Dark));
    assert_eq!(cache.terminal_mode(""), None);
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
