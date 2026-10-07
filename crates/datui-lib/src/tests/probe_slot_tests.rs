use crate::home_app::MAX_CONCURRENT_PROBES;
use crate::*;
use std::sync::mpsc;

/// The directory browsed into is the whole screen, so its listing never waits for
/// a slot held by roots the user has left.
#[test]
fn the_directory_browsed_into_is_never_held_behind_the_cap() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.home.network_check = |_| true;
    let dir = PathBuf::from("/pretend/share/raw");
    app.home.browsing = Some(dir.clone());
    app.home_app.probes_inflight = (0..MAX_CONCURRENT_PROBES)
        .map(|i| PathBuf::from(format!("/pretend/slow{i}")))
        .collect();

    app.spawn_home_probes();

    assert!(app.home_app.probes_inflight.contains(&dir));
}

/// Rows read so far show, marked as still listing, until the listing lands; a batch
/// arriving after the whole answer is dropped. A listing cut at the cap says so.
#[test]
fn rows_so_far_show_until_the_listing_lands() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.home.network_check = |_| true;
    let dir = PathBuf::from("/pretend/share/raw");
    app.home.browsing = Some(dir.clone());
    app.home_app.probes_inflight = vec![dir.clone()];
    let row = |name: &str| discover::Entry::directory(&dir.join(name));

    app.event(&AppEvent::HomeProbeProgress {
        root: dir.clone(),
        rows: vec![row("2009-01-03")],
    });
    app.home.rebuild(&[]);
    let section = &app.home.sections[0];
    assert!(section.waiting);
    assert_eq!(section.subtitle.as_deref(), Some("1 so far"));
    assert_eq!(section.rows.len(), 1);

    app.event(&AppEvent::HomeProbeReady {
        root: dir.clone(),
        rows: Some(vec![row("2009-01-03"), row("2009-01-04")]),
        cut_short: true,
    });
    app.event(&AppEvent::HomeProbeProgress {
        root: dir.clone(),
        rows: vec![row("late")],
    });
    app.home.rebuild(&[]);
    let section = &app.home.sections[0];
    assert!(!section.waiting);
    assert_eq!(section.rows.len(), 2);
    assert_eq!(section.subtitle.as_deref(), Some("first 5,000"));
}

/// A probe that answers must give its slot back. The cap is there to bound threads
/// wedged on a dead mount, and those never answer at all; counting completed probes
/// against it meant that after MAX_CONCURRENT_PROBES roots, no root was ever probed
/// again for the rest of the session. Roots accumulate as datasets are opened on
/// different mounts, so this is reached by ordinary use, and it shows as a network
/// section that stays empty with no error.
#[test]
fn an_answered_probe_frees_its_slot() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());

    let roots: Vec<PathBuf> = (0..MAX_CONCURRENT_PROBES)
        .map(|i| PathBuf::from(format!("/pretend/remote{i}")))
        .collect();
    app.home_app.probes_inflight = roots.clone();

    for (i, root) in roots.iter().enumerate() {
        // Alternate the two ways a probe can answer; both are answers.
        let rows = if i % 2 == 0 { Some(Vec::new()) } else { None };
        app.event(&AppEvent::HomeProbeReady {
            root: root.clone(),
            rows,
            cut_short: false,
        });
    }

    assert!(
        app.home_app.probes_inflight.is_empty(),
        "every probe answered, so nothing should still hold a slot: {:?}",
        app.home_app.probes_inflight
    );
}

/// A root that never answers keeps its slot, which is the whole point of the cap.
#[test]
fn an_unanswered_probe_keeps_its_slot() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());

    let wedged = PathBuf::from("/pretend/dead-mount");
    let answered = PathBuf::from("/pretend/live-mount");
    app.home_app.probes_inflight = vec![wedged.clone(), answered.clone()];

    app.event(&AppEvent::HomeProbeReady {
        root: answered,
        rows: Some(Vec::new()),
        cut_short: false,
    });

    assert_eq!(
        app.home_app.probes_inflight,
        vec![wedged],
        "a thread still stuck on a dead mount must keep costing a slot"
    );
}

/// Without `cloud`, a bucket browsed into says the build cannot list it, rather
/// than reading as a place that did not answer.
#[cfg(not(feature = "cloud"))]
#[test]
fn without_cloud_a_bucket_says_it_cannot_be_listed() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let bucket = PathBuf::from("s3://bucket/prefix/");
    app.home.browsing = Some(bucket.clone());
    app.home.network_check = |_| true;
    app.spawn_home_probes();
    assert!(app.home_app.probes_inflight.contains(&bucket));
    while !app.home_app.probes_inflight.is_empty() {
        let event = rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the probe answers");
        app.event(&event);
    }
    assert_eq!(
        app.home.probes.error(&bucket),
        Some("cloud support not in this build")
    );
}

/// Batches of rows add to what was read, and however many arrive before a frame, the
/// place is listed once for it.
#[test]
fn batches_read_before_a_frame_are_listed_once() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.input_mode = InputMode::Home;
    app.home.network_check = |_| true;
    let dir = PathBuf::from("/pretend/share/raw");
    app.home.browsing = Some(dir.clone());
    app.home_app.probes_inflight = vec![dir.clone()];
    let row = |name: &str| discover::Entry::directory(&dir.join(name));

    let generation = app.home_app.generation;
    for name in ["a", "b", "c"] {
        app.event(&AppEvent::HomeProbeProgress {
            root: dir.clone(),
            rows: vec![row(name)],
        });
    }
    assert_eq!(
        app.home_app.generation, generation,
        "nothing listed between frames"
    );
    let names: Vec<&str> = app
        .home
        .probes
        .so_far(&dir)
        .unwrap()
        .iter()
        .map(|row| row.name.as_str())
        .collect();
    assert_eq!(names, ["a", "b", "c"]);

    app.request_what_the_frame_needs();
    assert_eq!(app.home_app.generation, generation.wrapping_add(1));
    app.request_what_the_frame_needs();
    assert_eq!(app.home_app.generation, generation.wrapping_add(1));
}
