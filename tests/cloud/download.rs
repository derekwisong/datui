//! Opening one remote CSV downloads it to a temporary file: streamed whole from an
//! in-process S3 stand-in (`common/fake_s3.rs`), held by the dataset scanning it, and
//! removed by whatever ends it. Nothing leaves the loopback.

use crate::common::next_event;
use crate::fake_s3::FakeS3;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppConfig, AppEvent, JobKind, OpenOptions};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const KEY: &str = "tables/sales.csv";
const URL: &str = "s3://lake/tables/sales.csv";

/// `rows` rows of CSV: a megabyte and more at the sizes used here, so the body
/// arrives in many chunks.
fn csv(rows: usize) -> Vec<u8> {
    let mut out = String::from("id,region,amount\n");
    for id in 0..rows {
        let region = ["North", "South", "East", "West"][id % 4];
        out.push_str(&format!(
            "{id},{region},{}.{:02}\n",
            id * 7 % 1000,
            id % 100
        ));
    }
    out.into_bytes()
}

fn serve(bytes: Vec<u8>) -> FakeS3 {
    FakeS3::serve("lake", BTreeMap::from([(KEY.to_string(), bytes)]))
}

fn app(s3: &FakeS3) -> (App, mpsc::Receiver<AppEvent>) {
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..AppConfig::default()
    };
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel();
    let app = App::new_with_config(tx, crate::common::test_runtime(), theme, config);
    (app, rx)
}

/// Handle `first` and every event it leads to.
fn chain(app: &mut App, first: AppEvent) {
    let mut next = Some(first);
    while let Some(event) = next {
        next = app.event(&event);
    }
}

/// Open the object with downloads landing in `dir`, and answer the size question with
/// Yes once the probe has asked it. The download is then under way.
fn open_and_confirm(app: &mut App, rx: &mpsc::Receiver<AppEvent>, dir: &Path) {
    let options = OpenOptions {
        temp_dir: Some(dir.to_path_buf()),
        ..OpenOptions::default()
    };
    chain(app, AppEvent::Open(vec![PathBuf::from(URL)], options));
    while app.error_message().is_none() && !app.awaiting_download_confirmation() {
        let event = next_event(app, rx).expect("the size probe answers");
        chain(app, event);
    }
    assert_eq!(app.error_message(), None);
    chain(
        app,
        AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
    );
}

/// Handle events until nothing is queued or owed.
fn settle(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    while let Some(event) = next_event(app, rx) {
        chain(app, event);
    }
}

fn files_in(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

/// Polls until `dir` holds `count` files, failing the test after a generous guard.
#[track_caller]
fn wait_for_files(dir: &Path, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while files_in(dir).len() != count {
        assert!(
            Instant::now() < deadline,
            "{dir:?} still holds {:?}",
            files_in(dir)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The object lands byte for byte in one GET, and the dataset reads it. The file
/// lives while that dataset is on screen and goes when another replaces it.
#[test]
fn a_download_lands_whole_and_lives_with_its_dataset() {
    let bytes = csv(60_000);
    let s3 = serve(bytes.clone());
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, dir.path());
    settle(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 60_000);
    assert_eq!(s3.wire.count().gets, 1, "one GET for the whole object");
    let files = files_in(dir.path());
    assert_eq!(files.len(), 1);
    assert!(files[0].to_string_lossy().ends_with(".csv"), "{files:?}");
    assert_eq!(std::fs::read(&files[0]).unwrap(), bytes);

    let local = tempfile::tempdir().unwrap();
    let other = local.path().join("other.csv");
    std::fs::write(&other, "a,b\n1,2\n").unwrap();
    chain(
        &mut app,
        AppEvent::Open(vec![other.clone()], OpenOptions::default()),
    );
    settle(&mut app, &rx);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 1);
    assert!(
        files_in(dir.path()).is_empty(),
        "the replaced dataset's file went"
    );
}

/// The dataset on screen holds its file, not just the app: `H` reads the same copy
/// again, and an open that fails after the app let go leaves the dataset, and the
/// file it scans lazily, in place.
#[test]
fn a_failed_open_keeps_the_file_the_dataset_scans() {
    let s3 = serve(csv(1_000));
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, dir.path());
    settle(&mut app, &rx);
    let file = files_in(dir.path()).pop().expect("the download");

    chain(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Char('H'), KeyModifiers::NONE)),
    );
    settle(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(s3.wire.count().gets, 1, "H read the copy on hand");
    assert_eq!(files_in(dir.path()), std::slice::from_ref(&file));

    let local = tempfile::tempdir().unwrap();
    let broken = local.path().join("broken.parquet");
    std::fs::write(&broken, b"not parquet").unwrap();
    chain(
        &mut app,
        AppEvent::Open(vec![broken], OpenOptions::default()),
    );
    settle(&mut app, &rx);
    assert!(app.error_message().is_some(), "the open failed");
    assert_eq!(app.open_path(), Some(Path::new(URL)));
    assert!(file.exists(), "the dataset on screen still scans it");
    let rows = app
        .data_table_state
        .as_ref()
        .unwrap()
        .visible_lf()
        .collect()
        .expect("the lazy scan reads the file");
    assert_eq!(rows.height(), 1_001, "H read the header as a row");
    assert!(app.capture_view().is_err(), "a view over a temp file");

    drop(app);
    assert!(!file.exists());
}

/// Quitting removes the file of the dataset on screen.
#[test]
fn exit_removes_the_download() {
    let s3 = serve(csv(1_000));
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, dir.path());
    settle(&mut app, &rx);
    assert_eq!(files_in(dir.path()).len(), 1);
    drop(app);
    assert!(files_in(dir.path()).is_empty());
}

/// Quitting mid-download removes the partial file before the session ends, as the
/// process does not wait for the worker to notice the stop. Before `ExitSweep`, the
/// file outlived the app here and the process could exit with it still on disk (#510).
#[test]
fn quitting_mid_download_removes_the_partial_file_before_exit() {
    let s3 = serve(csv(1_000));
    s3.slow_gets(5_000);
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, dir.path());
    wait_for_files(dir.path(), 1);

    let began = Instant::now();
    let sweep = app.exit_sweep();
    drop(app);
    drop(sweep);
    assert!(
        files_in(dir.path()).is_empty(),
        "the partial file is gone as the session ends"
    );
    assert!(
        began.elapsed() < Duration::from_secs(4),
        "nor did it wait for the store, after {:?}",
        began.elapsed()
    );
}

/// An object gone by the time it is fetched fails the open with the store's reason,
/// and leaves no file.
#[test]
fn a_failed_download_leaves_no_file() {
    let s3 = serve(csv(1_000));
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    let options = OpenOptions {
        temp_dir: Some(dir.path().to_path_buf()),
        ..OpenOptions::default()
    };
    chain(&mut app, AppEvent::Open(vec![PathBuf::from(URL)], options));
    while !app.awaiting_download_confirmation() {
        let event = next_event(&app, &rx).expect("the size probe answers");
        chain(&mut app, event);
    }
    s3.remove(KEY);
    chain(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
    );
    settle(&mut app, &rx);
    let message = app.error_message().expect("the open failed");
    assert!(message.contains("Could not read from S3"), "{message}");
    assert!(files_in(dir.path()).is_empty());
}

/// Going home while the store has not answered stops the download and removes its
/// file, without an error for a load nobody is waiting on; so does quitting.
#[test]
fn an_abandoned_download_stops_and_leaves_no_file() {
    let s3 = serve(csv(1_000));
    s3.slow_gets(5_000);
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, dir.path());
    wait_for_files(dir.path(), 1);
    let began = Instant::now();
    chain(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL)),
    );
    wait_for_files(dir.path(), 0);
    assert!(
        began.elapsed() < Duration::from_secs(4),
        "stopped before the store answered, after {:?}",
        began.elapsed()
    );
    // The stopped worker's failure arrives behind the removal, for a load nobody
    // is waiting on.
    loop {
        let event = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the stopped download reports");
        let failed = matches!(event, AppEvent::JobEnded(t) if t.kind() == JobKind::Load);
        chain(&mut app, event);
        if failed {
            break;
        }
    }
    assert_eq!(app.error_message(), None);

    let quit = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, quit.path());
    wait_for_files(quit.path(), 1);
    drop(app);
    wait_for_files(quit.path(), 0);
}

/// Opening something else while a download runs stops it and removes its file, as
/// going home does; the other dataset opens, with no error for the open it replaced.
#[test]
fn an_open_replacing_a_download_stops_it_and_leaves_no_file() {
    let s3 = serve(csv(1_000));
    s3.slow_gets(5_000);
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, dir.path());
    wait_for_files(dir.path(), 1);

    let local = tempfile::tempdir().unwrap();
    let other = local.path().join("other.csv");
    std::fs::write(&other, "a,b\n1,2\n").unwrap();
    let began = Instant::now();
    chain(
        &mut app,
        AppEvent::Open(vec![other.clone()], OpenOptions::default()),
    );
    wait_for_files(dir.path(), 0);
    assert!(
        began.elapsed() < Duration::from_secs(4),
        "stopped before the store answered, after {:?}",
        began.elapsed()
    );
    // The stopped worker's failure arrives for an open nobody is waiting on.
    let deadline = Instant::now() + Duration::from_secs(30);
    while app.background_work_in_flight() || app.is_busy() {
        assert!(
            Instant::now() < deadline,
            "the stopped download never ended"
        );
        if let Ok(event) = rx.recv_timeout(Duration::from_millis(50)) {
            chain(&mut app, event);
        }
    }
    settle(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.open_path(), Some(other.as_path()));
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 1);
}

/// A finished download whose answer is discarded, or never delivered, takes its file
/// with it.
#[test]
fn a_download_nobody_takes_leaves_no_file() {
    let s3 = serve(csv(1_000));
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, dir.path());
    // The download's answer waits in its job until the app takes it.
    let ready = loop {
        let event = next_event(&app, &rx).expect("the download answers");
        if matches!(event, AppEvent::JobEnded(t) if t.kind() == JobKind::Load) {
            break event;
        }
        chain(&mut app, event);
    };
    assert_eq!(files_in(dir.path()).len(), 1);
    app.abandon_load();
    chain(&mut app, ready);
    assert!(files_in(dir.path()).is_empty(), "the stale answer took it");

    // Delivered to a channel nobody reads any more.
    s3.slow_gets(300);
    let undelivered = tempfile::tempdir().unwrap();
    open_and_confirm(&mut app, &rx, undelivered.path());
    drop(rx);
    wait_for_files(undelivered.path(), 1);
    wait_for_files(undelivered.path(), 0);
}

/// Peak memory of one large download, for the PR's measurement: run alone, so the
/// process's high-water mark is this test's.
///
/// `DATUI_MEASURE_MB=512 scripts/dev/test.sh integration integration_test
/// cloud_download::measure_download_peak_memory -- --ignored --nocapture`
#[test]
#[ignore = "a measurement; run alone with --ignored --nocapture"]
fn measure_download_peak_memory() {
    let mb: usize = std::env::var("DATUI_MEASURE_MB")
        .ok()
        .and_then(|mb| mb.parse().ok())
        .unwrap_or(512);
    let line = b"0123456789,North,123.45,abcdefghijklmnopqrstuvwxyz\n";
    let mut bytes = Vec::with_capacity(mb << 20);
    bytes.extend_from_slice(b"id,region,amount,text\n");
    while bytes.len() < mb << 20 {
        bytes.extend_from_slice(line);
    }
    let size = bytes.len();
    let s3 = serve(bytes);
    let (mut app, rx) = app(&s3);
    let dir = tempfile::tempdir().unwrap();
    let before = status_kib("VmHWM");
    let began = Instant::now();
    open_and_confirm(&mut app, &rx, dir.path());
    // Held: the answer waits in its job, and owns the file, until the app takes it.
    let _ready = loop {
        let event = next_event(&app, &rx).expect("the download answers");
        if matches!(event, AppEvent::JobEnded(t) if t.kind() == JobKind::Load) {
            break event;
        }
        chain(&mut app, event);
        assert_eq!(app.error_message(), None, "the download failed");
    };
    let took = began.elapsed();
    let after = status_kib("VmHWM");
    let written: u64 = files_in(dir.path())
        .iter()
        .map(|path| std::fs::metadata(path).unwrap().len())
        .sum();
    assert_eq!(written, size as u64);
    println!(
        "object {} MiB in {took:.2?}: peak RSS {} MiB before the open, {} MiB after the \
         download; grew {} MiB",
        size >> 20,
        before >> 10,
        after >> 10,
        (after - before) >> 10
    );
}

/// A `/proc/self/status` field in KiB.
fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        .expect("a /proc/self/status field")
}
