//! What a full Data Quality scan leaves on disk: nothing.
//!
//! Polars 0.55's streaming engine can spill to `POLARS_OOC_SPILL_DIR`, but only past
//! a memory budget it measures through its own global allocator, which datui does
//! not install: its estimate stays at zero and nothing spills. These tests hold
//! that, and datui's own temporary files, to account: with the smallest budget
//! Polars accepts, a finished, a cancelled and a failed full scan each leave the
//! spill directory and the temporary directory as they found them.
//!
//! Its own binary: the directories are set in the environment before Polars reads
//! its configuration, once per process.

mod common;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::data_quality::{QualityCompute, QualityGrain};
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// Handle `first` and what follows until nothing is owed, abandoned work included.
fn settle(app: &mut App, rx: &mpsc::Receiver<AppEvent>, first: Option<AppEvent>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    let mut next = first;
    loop {
        if let Some(event) = next.take() {
            next = app.event(&event);
            continue;
        }
        match rx.recv_timeout(std::time::Duration::from_millis(50)) {
            Ok(event) => next = Some(event),
            Err(_) if !app.is_busy() && !app.background_work_in_flight() => return,
            Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "background work never reported back"
            ),
        }
    }
}

/// Every file under `dir`, with its size.
fn files_under(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(files_under(&path));
        } else {
            let size = entry.metadata().map(|meta| meta.len()).unwrap_or(0);
            found.push((path, size));
        }
    }
    found
}

#[test]
fn a_full_scan_leaves_no_temporary_files() {
    let root = tempfile::tempdir().unwrap();
    let (spill, temp, data) = (
        root.path().join("spill"),
        root.path().join("tmp"),
        root.path().join("data"),
    );
    for dir in [&spill, &temp, &data] {
        std::fs::create_dir_all(dir).unwrap();
    }
    // SAFETY: the only test in this binary, before Polars reads its configuration
    // and before any thread that reads the environment starts.
    unsafe {
        std::env::set_var("POLARS_OOC_SPILL_DIR", &spill);
        std::env::set_var("POLARS_OOC_MEMORY_BUDGET_MB", "0");
        std::env::set_var("TMPDIR", &temp);
    }

    let rows = 200_000;
    let path = data.join("wide.parquet");
    let mut df = df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "region" => (0..rows).map(|row| format!("region {}", row % 97)).collect::<Vec<_>>(),
        "note" => (0..rows).map(|row| format!("note {row} {}", "x".repeat(row % 40))).collect::<Vec<_>>(),
        "amount" => (0..rows).map(|row| (row % 11 != 0).then_some(row as f64)).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle(
        &mut app,
        &rx,
        Some(AppEvent::Open(vec![path.clone()], OpenOptions::default())),
    );
    let temp_before = files_under(&temp);
    // The most the spill directory held at any moment, sampled while the scans run.
    let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let watching = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = {
        let (peak, watching, spill) = (peak.clone(), watching.clone(), spill.clone());
        std::thread::spawn(move || {
            while watching.load(std::sync::atomic::Ordering::Relaxed) {
                let held = files_under(&spill).iter().map(|(_, size)| size).sum();
                peak.fetch_max(held, std::sync::atomic::Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let untouched = |what: &str| {
        assert_eq!(files_under(&spill), Vec::new(), "{what}: spilled");
        assert_eq!(files_under(&temp), temp_before, "{what}: temporary files");
    };

    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    press(&mut app, KeyCode::Enter);
    let full = |app: &mut App, grain: QualityGrain| {
        let plan = &mut app.analysis_modal.data_quality_plan;
        plan.method = datui::sampling::SampleMethod::EveryRow;
        plan.compute = QualityCompute::Full;
        plan.grain = grain;
        assert!(press(app, KeyCode::Enter).is_none(), "a full scan asks");
        press(app, KeyCode::Enter)
    };

    // Finished: every pass, a grouping by region among them.
    let run = full(&mut app, QualityGrain::Partition("region".into()));
    settle(&mut app, &rx, run);
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(results.evaluated_rows, rows);
    untouched("a finished full scan");

    // Cancelled as soon as it starts, and waited out.
    press(&mut app, KeyCode::Char('e'));
    let run = full(&mut app, QualityGrain::Dataset);
    let Some(run) = run else {
        panic!("the scan was dispatched");
    };
    app.event(&run);
    assert!(app.is_busy());
    press(&mut app, KeyCode::Esc);
    settle(&mut app, &rx, None);
    assert!(!app.background_work_in_flight());
    untouched("a cancelled full scan");

    // Failed: the file is gone.
    std::fs::remove_file(&path).unwrap();
    if !app.analysis_modal.data_quality_page.is_setup() {
        press(&mut app, KeyCode::Char('e'));
    }
    let run = full(&mut app, QualityGrain::Partition("note".into()));
    settle(&mut app, &rx, run);
    assert!(app.modal_showing(), "the scan failed");
    untouched("a failed full scan");

    watching.store(false, std::sync::atomic::Ordering::Relaxed);
    watcher.join().unwrap();
    assert_eq!(
        peak.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "nothing spilled at any point"
    );
}
