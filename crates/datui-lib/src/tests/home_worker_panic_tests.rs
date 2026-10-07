use crate::*;
use std::sync::mpsc;

fn app() -> (App, mpsc::Receiver<AppEvent>, tempfile::TempDir) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "x\n1\n").unwrap();
    app.home.browsing = Some(dir.path().to_path_buf());
    (app, rx, dir)
}

/// Kill the first worker that would owe `owed` in its place.
fn dies_once(app: &mut App, owed: fn(&AppEvent) -> bool) {
    let mut died = false;
    app.home_worker_dies = Some(Box::new(move |instead| {
        let dies = !died && owed(instead);
        died |= dies;
        dies
    }));
}

/// Handle what the workers send until `done`.
fn pump(app: &mut App, rx: &mpsc::Receiver<AppEvent>, done: impl Fn(&App) -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !done(app) {
        assert!(
            std::time::Instant::now() < deadline,
            "the worker never answered"
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            app.event(&event);
        }
    }
}

/// A listing for somewhere the user has left lands without saying nothing is in
/// flight: the listing for where they are still runs.
#[test]
fn a_stale_listing_leaves_the_current_one_in_flight() {
    let (mut app, _rx, _dir) = app();
    app.home_refresh();
    let stale = app.home_generation;
    app.home_refresh();
    assert!(app.home.listing_in_flight);
    app.event(&AppEvent::HomeListingReady {
        generation: stale,
        listing: Box::default(),
        known: Default::default(),
        folds: None,
        visits: Default::default(),
        newest: None,
    });
    assert!(app.home.listing_in_flight, "the current listing still runs");
    app.event(&AppEvent::HomeListingReady {
        generation: app.home_generation,
        listing: Box::default(),
        known: Default::default(),
        folds: None,
        visits: Default::default(),
        newest: None,
    });
    assert!(!app.home.listing_in_flight);
}

#[test]
fn a_listing_whose_worker_dies_stops_looking_and_the_next_one_lists() {
    let (mut app, rx, _dir) = app();
    dies_once(&mut app, |e| matches!(e, AppEvent::HomeListingFailed));
    app.home_refresh();
    assert!(app.home.listing_in_flight);
    pump(&mut app, &rx, |a| !a.home.listing_in_flight);
    assert!(
        app.home.sections.iter().all(|s| s.rows.is_empty()),
        "nothing was listed"
    );

    app.home_refresh();
    pump(&mut app, &rx, |a| !a.home.listing_in_flight);
    assert!(
        app.home.sections.iter().any(|s| !s.rows.is_empty()),
        "the next listing lands"
    );
}

#[test]
fn a_probe_whose_worker_dies_gives_its_slot_back_and_says_so() {
    let (mut app, rx, dir) = app();
    app.home.network_check = |_| true;
    dies_once(&mut app, |e| matches!(e, AppEvent::HomeProbeFailed { .. }));
    app.spawn_home_probes();
    let root = dir.path().to_path_buf();
    assert!(app.home_probes_inflight.contains(&root));
    pump(&mut app, &rx, |a| a.home_probes_inflight.is_empty());
    assert_eq!(
        app.home.probes.error(&root),
        Some("Could not read it; see the log")
    );
}

#[test]
fn a_schema_read_that_dies_is_not_asked_for_again() {
    let (mut app, rx, dir) = app();
    dies_once(&mut app, |e| matches!(e, AppEvent::HomeSchemaReady { .. }));
    let entry = discover::Entry::new(dir.path().join("a.csv"), discover::EntryKind::File);
    assert!(app.home_schema(&entry).is_none());
    assert!(app.home_schema_pending(&entry.path));
    pump(&mut app, &rx, |a| !a.home_schema_pending(&entry.path));
    assert!(app.home_schema(&entry).is_none());
    assert!(
        !app.home_schema_pending(&entry.path),
        "remembered as having none"
    );
}

#[test]
fn a_search_whose_walk_dies_ends() {
    let (mut app, rx, _dir) = app();
    app.app_config.home.search.enabled = true;
    app.home.network_check = |_| false;
    dies_once(&mut app, |e| matches!(e, AppEvent::HomeSearchDone { .. }));
    app.spawn_home_search();
    assert!(app.home.search.running);
    pump(&mut app, &rx, |a| !a.home_search_inflight);
    assert!(!app.home.search.running);
    assert!(app.home.search.done);
    assert_eq!(app.home.search.limited.as_deref(), Some("partial · failed"));
}
