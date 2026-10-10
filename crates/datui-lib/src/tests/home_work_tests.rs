//! What the home screen reads, and when: answers come batched, a listing that never
//! answers stops spinning, one schema read is out at a time, the cache's index is read
//! once, and nothing on the key thread touches a file.

use crate::*;
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn app() -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.input_mode = InputMode::Home;
    (app, rx)
}

/// Handle what the workers send, with each frame's work, until `done`.
fn until(app: &mut App, rx: &mpsc::Receiver<AppEvent>, what: &str, done: impl Fn(&App) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        app.frame_work();
        if done(app) {
            return;
        }
        assert!(Instant::now() < deadline, "never: {what}");
        if let Ok(event) = rx.recv_timeout(Duration::from_millis(20)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(event);
            }
        }
    }
}

fn listing_of(rows: Vec<home::discover::Entry>) -> home::Listing {
    home::Listing {
        sections: vec![home::Section::titled("HERE", rows)],
        ..Default::default()
    }
}

fn file(path: std::path::PathBuf) -> home::discover::Entry {
    home::discover::Entry::new(path, home::discover::EntryKind::File)
}

/// A batch of rows measured quickly lands as one event, not one per row: each event is
/// a pass of the loop and a frame.
#[test]
fn a_quick_batch_of_measurements_lands_as_one_answer() {
    let (mut app, rx) = app();
    let dir = tempfile::tempdir().unwrap();
    let rows = (0..12)
        .map(|i| {
            let path = dir.path().join(format!("f{i:02}.csv"));
            std::fs::write(&path, "a\n1\n").unwrap();
            file(path)
        })
        .collect();
    app.home.apply_listing(listing_of(rows));
    app.home.view_height = 20;
    app.request_home_measurements();
    assert!(app.home.measure_in_flight);

    let mut answers = 0;
    let mut measured = 0;
    while app.home.measure_in_flight {
        let event = rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the batch answers");
        if let AppEvent::HomeMeasured { measured: m, .. } = &event {
            answers += 1;
            measured += m.len();
        }
        app.event(event);
    }
    assert_eq!(answers, 1, "one answer for the batch");
    assert_eq!(measured, 12, "every row in it");
}

/// A network place that never answers stops spinning after the wait and says so;
/// Ctrl+R waits on it again, and its answer still lands when it comes.
#[test]
fn a_place_that_never_answers_stops_spinning_and_ctrl_r_waits_again() {
    let (mut app, rx) = app();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "x\n1\n").unwrap();
    let place = dir.path().to_path_buf();
    app.home.network_check = |_| true;
    app.home.browsing = Some(place.clone());
    app.home_app.probe_patience = Some(Duration::from_millis(50));
    let gate = std::sync::Arc::new(std::sync::Mutex::new(()));
    app.home_app.probe_gate = Some(gate.clone());
    let held = gate.lock().unwrap();

    app.home.rebuild(&[]);
    app.spawn_home_probes();
    assert!(app.something_is_spinning(), "waiting on the place");

    until(&mut app, &rx, "the place is not answering", |app| {
        app.home.probes.silent(&place)
    });
    assert!(!app.something_is_spinning(), "the spinner stops");
    assert_eq!(
        app.home.sections[0].unavailable_note.as_deref(),
        Some(home::NOT_ANSWERING)
    );
    assert!(
        app.home_app.probes_inflight.contains(&place),
        "its thread is still out, and not started again"
    );

    // The second wait must outlast the checks below on a slow runner; only the first
    // needed to run out.
    app.home_app.probe_patience = Some(Duration::from_secs(60));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('r'),
        KeyModifiers::CONTROL,
    )));
    app.frame_work();
    assert!(app.something_is_spinning(), "Ctrl+R waits on it again");
    assert_eq!(app.home_app.probes_inflight, vec![place.clone()]);

    drop(held);
    until(&mut app, &rx, "the late answer lands", |app| {
        app.home.probes.listed(&place).is_some()
    });
    assert!(!app.something_is_spinning());
}

/// Holding ↓ through datasets reads one schema at a time, for the row the cursor is on
/// when the last read lands, not one per row passed.
#[test]
fn one_schema_read_is_out_for_the_row_the_cursor_rests_on() {
    let (mut app, rx) = app();
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<_> = (0..5)
        .map(|i| {
            let path = dir.path().join(format!("p{i}.parquet"));
            std::fs::write(&path, b"not really").unwrap();
            file(path)
        })
        .collect();
    let path = |i: usize| rows[i].path.clone();
    let paths: Vec<_> = (0..5).map(path).collect();
    app.home.apply_listing(listing_of(rows));
    app.home.select(1);
    app.request_home_schema();
    assert!(app.home_schema_pending(&paths[0]));
    for row in 2..=4 {
        app.home.select(row + 1);
        app.request_home_schema();
    }
    assert!(
        app.home_schema_pending(&paths[0]),
        "still the one read, none queued behind it"
    );
    assert_eq!(app.home_app.reads.schemas, 1);

    until(&mut app, &rx, "the read where the cursor rests", |app| {
        app.home_app.schema_cache.contains_key(&paths[4])
    });
    assert_eq!(
        app.home_app.reads.schemas, 2,
        "the first, then where it rests"
    );
}

/// The cache's index of what earlier runs measured is read with the first listing and
/// kept: later listings neither scan the cache again nor date its records again.
#[test]
fn listings_after_the_first_do_not_scan_the_cache() {
    let (mut app, rx) = app();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = crate::cache::CacheManager::with_dir(cache_dir.path().to_path_buf());
    let dir = tempfile::tempdir().unwrap();
    app.home.browsing = Some(dir.path().to_path_buf());
    let record = crate::cache::DatasetFacts {
        rows: Some(3),
        ..Default::default()
    };
    cache.record_dataset_facts(&[("/before".into(), record.clone())]);
    app.use_cache(cache.clone());

    app.home_refresh();
    until(&mut app, &rx, "the first listing", |app| {
        !app.home.listing_in_flight
    });
    assert!(app.home_app.facts_read);
    assert!(app.home.known.contains_key(std::path::Path::new("/before")));

    cache.record_dataset_facts(&[("/after".into(), record)]);
    app.home_refresh();
    until(&mut app, &rx, "the second listing", |app| {
        !app.home.listing_in_flight
    });
    assert!(
        !app.home.known.contains_key(std::path::Path::new("/after")),
        "the index is not read again"
    );
    assert!(app.home.known.contains_key(std::path::Path::new("/before")));
}

/// Typing within one directory at `~` lists it once, however many keys arrive before
/// its listing does.
#[test]
fn the_path_prompt_lists_a_directory_once() {
    let (mut app, rx) = app();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("alpha.csv"), "x\n").unwrap();
    app.home.path_input_active = true;
    let typed = format!("{}/", dir.path().display());
    for c in ["a", "l", "p"] {
        app.home.path_input = format!("{typed}{c}");
        app.list_the_typed_directory();
    }
    assert_eq!(app.home_app.path_listings_out.len(), 1);
    let mut listed = 0;
    while !app.home_app.path_listings_out.is_empty() {
        let event = rx.recv_timeout(Duration::from_secs(60)).expect("listed");
        if matches!(event, AppEvent::HomePathListed { .. }) {
            listed += 1;
        }
        app.event(event);
    }
    assert_eq!(listed, 1);
    let names: Vec<&str> = (app.home.path_candidates().into_iter())
        .map(|n| n.name.as_str())
        .collect();
    assert_eq!(names, ["alpha.csv"]);
}

/// Enter on a directory nothing has looked into asks a worker what it is: the key
/// thread touches no file, whatever the mount.
#[test]
fn enter_on_a_directory_not_looked_into_asks_a_worker() {
    let (mut app, _rx) = app();
    let dir = tempfile::tempdir().unwrap();
    let inner = dir.path().join("inner");
    std::fs::create_dir(&inner).unwrap();
    let mut row = home::discover::Entry::directory(&inner);
    row.kind = home::discover::EntryKind::Unknown;
    app.home.apply_listing(listing_of(vec![row]));
    app.home.select(1);
    let next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(next, Some(AppEvent::ClassifyThenOpen { ref path, typed: None }) if *path == inner),
        "a look on a worker, not an answer on this thread"
    );
}

/// Coming back to home puts the cursor on the file left, its rows compared as they
/// are listed and only the open path resolved: no row is resolved on the key thread.
#[test]
fn coming_back_lands_on_the_file_left_as_listed() {
    let (mut app, _rx) = app();
    let dir = tempfile::tempdir().unwrap();
    let dir_path = crate::canonical::canonicalize(dir.path()).unwrap();
    let left = dir_path.join("left.csv");
    std::fs::write(&left, "x\n").unwrap();
    // A row that resolves to the file but is not spelled as it: never taken for it.
    let alias = dir_path.join("alias.csv");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&left, &alias).unwrap();
    let rows = vec![file(alias), file(left.clone())];
    app.home.apply_listing(listing_of(rows));
    app.path = Some(dir_path.join(".").join("left.csv"));
    app.enter_home();
    assert_eq!(
        app.home.selected_entry().map(|e| e.path.clone()),
        Some(left),
        "the row spelled as the open path resolves"
    );
}

/// A classification pass stuck on one filesystem leaves the rest to their own pass.
#[cfg(target_os = "linux")]
#[test]
fn a_pass_stuck_on_one_filesystem_leaves_the_others() {
    let (mut app, _rx) = app();
    let unknown = |path: &str| {
        let mut row = home::discover::Entry::directory(std::path::Path::new(path));
        row.kind = home::discover::EntryKind::Unknown;
        row
    };
    app.home.apply_listing(listing_of(vec![
        unknown("/pretend/stuck/a"),
        unknown("/proc/pretend/b"),
    ]));
    app.home.view_height = 20;
    let root = home::locality::Mounts::cached().mount_point_for(std::path::Path::new("/pretend"));
    let procfs = home::locality::Mounts::cached().mount_point_for(std::path::Path::new("/proc"));
    assert_ne!(root, procfs, "/proc is a filesystem of its own");
    // A pass on the root filesystem that never ends.
    app.home.classifying.insert(root.clone());
    app.request_home_classifications();
    assert!(
        app.home.classifying.contains(&procfs),
        "the other filesystem's rows are looked at"
    );
    assert_eq!(app.home.classifying.len(), 2);
}

/// The cloud sources' answer changes only their rows: nothing else is listed again.
#[cfg(feature = "cloud")]
#[test]
fn the_cloud_sources_answer_relists_only_their_section() {
    let (mut app, _rx) = app();
    let dir = tempfile::tempdir().unwrap();
    app.home
        .apply_listing(listing_of(vec![file(dir.path().join("a.csv"))]));
    let generation = app.home_app.generation;
    app.event(AppEvent::HomeCloudSources {
        sources: vec![home::CloudSource {
            id: "lab".into(),
            label: "Lab".into(),
            ..Default::default()
        }],
    });
    assert_eq!(app.home_app.generation, generation, "no listing asked for");
    assert!(!app.home.listing_in_flight);
    let titles: Vec<&str> = app.home.sections.iter().map(|s| s.title.as_str()).collect();
    assert_eq!(titles, ["HERE", home::HomeState::CLOUD_SECTION]);
}

/// A schema read stuck on a share holds up that share's reads and no others: a local
/// file under the cursor is still read.
#[test]
fn a_schema_read_stuck_on_a_share_leaves_local_reads() {
    let (mut app, rx) = app();
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("p.parquet");
    std::fs::write(&local, b"not really").unwrap();
    app.home
        .apply_listing(listing_of(vec![file(local.clone())]));
    app.home.select(1);
    // A read on a dead share that never answers.
    let stuck = std::path::PathBuf::from("/mnt/dead/q.parquet");
    (app.home_app.schema_reads).insert("/mnt/dead".into(), stuck.clone());

    app.request_home_schema();
    assert!(app.home_schema_pending(&local), "the local read goes out");
    until(&mut app, &rx, "the local read lands", |app| {
        app.home_app.schema_cache.contains_key(&local)
    });
    assert!(
        app.home_schema_pending(&stuck),
        "the stuck one only holds its own slot"
    );
}

/// Ctrl+R moves the stats on: the rows shown are stat'ed again.
#[test]
fn ctrl_r_stats_the_rows_shown_again() {
    let (mut app, _rx) = app();
    assert_eq!(app.home.stat_epoch, 0);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('r'),
        KeyModifiers::CONTROL,
    )));
    assert_eq!(app.home.stat_epoch, 1);
}
