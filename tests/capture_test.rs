//! `App::capture_view`: the frame `datui.view(..., capture=True)` hands back at quit.

mod common;

use common::{drain_events, pump_open_until_loaded};

use std::path::PathBuf;
use std::sync::mpsc;

use datui::{App, AppEvent, OpenOptions};

fn open_fixture(name: &str) -> (App, mpsc::Receiver<AppEvent>) {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data").join(name);
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    (app, rx)
}

/// A plain open captures every row, under the dataset's own columns and none of
/// datui's: the frame is the one the user saw, not the one the buffer reads.
#[test]
fn capture_returns_the_whole_table_without_internal_columns() {
    let (app, _rx) = open_fixture("people.parquet");
    let lf = app
        .capture_view()
        .expect("capture is allowed for a plain local file")
        .expect("a dataset is open");
    let df = lf.collect().expect("captured plan collects");
    assert_eq!(
        df.height(),
        1000,
        "all matching rows, not the screen buffer"
    );
    assert!(
        df.get_column_names()
            .iter()
            .all(|c| !c.starts_with("__datui")),
        "no internal columns in a captured view"
    );
}

/// The capture is the committed view: a query applied in the TUI shapes what comes
/// back, over all matching rows.
#[test]
fn capture_reflects_the_applied_query() {
    let (mut app, rx) = open_fixture("people.parquet");
    app.event(&AppEvent::Search("select where age < 30".to_string()));
    drain_events(&mut app, &rx);

    let expected = app
        .data_table_state
        .as_ref()
        .expect("dataset open")
        .visible_lf()
        .collect()
        .unwrap()
        .height();
    assert!(
        expected > 0 && expected < 1000,
        "the query narrowed the rows"
    );

    let df = app.capture_view().unwrap().unwrap().collect().unwrap();
    assert_eq!(df.height(), expected, "capture matches the queried view");
}

/// No dataset open — quitting from a fresh home screen — captures nothing, and that
/// is a None rather than an error.
#[test]
fn capture_is_none_when_no_dataset_is_open() {
    common::ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let app = App::new(tx, common::test_runtime());
    assert!(
        app.capture_view()
            .expect("no dataset is not an error")
            .is_none()
    );
}

/// A compressed CSV is decompressed into a temp file the table state removes when it
/// drops, so a captured plan over it would scan a deleted path. Refused, clearly.
#[test]
fn capture_is_refused_for_a_decompressed_temp_source() {
    let (app, _rx) = open_fixture("people.csv.gz");
    assert!(
        app.data_table_state.is_some(),
        "the compressed fixture loaded"
    );
    let Err(err) = app.capture_view() else {
        panic!("a temp-backed view must be refused");
    };
    let err = err.to_string();
    assert!(
        err.contains("temporary file"),
        "the refusal says why: {err}"
    );
}

/// A dataset whose files disagree carries the hidden drift column so rows can be
/// traced to their files on screen. The captured frame never does: its rows collect
/// under the dataset's columns alone.
#[test]
fn capture_drops_the_drift_column_a_drifting_dataset_carries() {
    use polars::prelude::*;
    common::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    for (sub, df) in [
        ("date=2024-01-01", polars::df!("id" => &[1i64]).unwrap()),
        (
            "date=2024-01-02",
            polars::df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
        ),
    ] {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let f = std::fs::File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut df.clone()).unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts);

    assert!(
        app.data_table_state.as_ref().unwrap().drifts(),
        "this dataset drifts"
    );
    let df = app.capture_view().unwrap().unwrap().collect().unwrap();
    assert_eq!(df.height(), 2);
    assert!(
        df.get_column_names()
            .iter()
            .all(|c| !c.starts_with("__datui")),
        "the drift column never leaves with a capture"
    );
}

/// The captured plan stands on its own: it still collects after the App — and with
/// it the terminal, the runtime handle and the table state — is gone. This is the
/// Python binding's life: `run` returns, everything is dropped, then Python collects.
#[test]
fn capture_outlives_the_app() {
    let (app, rx) = open_fixture("people.parquet");
    let lf = app.capture_view().unwrap().unwrap();
    drop(app);
    drop(rx);
    let df = lf.collect().expect("collects after teardown");
    assert_eq!(df.height(), 1000);
}
