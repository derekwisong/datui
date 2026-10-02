//! GPS logs: NMEA 0183 as merged fixes or one sentence type, and GPX points.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/gps/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, pump_open_until_loaded};
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

fn gps() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/gps")
}

fn open_with(path: PathBuf, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    (app, rx)
}

/// Converted files go to a scratch directory of the test's own, to be counted.
fn scratch() -> (OpenOptions, PathBuf) {
    let dir = common::fixture_dir().join(format!(
        "gps-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    (
        OpenOptions {
            temp_dir: Some(dir.clone()),
            ..OpenOptions::default()
        },
        dir,
    )
}

fn frame(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .expect("a dataset is open")
        .lf()
        .clone()
        .collect()
        .expect("collect")
}

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect()
}

fn times(df: &DataFrame) -> Vec<Option<i64>> {
    df.column("time")
        .unwrap()
        .datetime()
        .unwrap()
        .physical()
        .iter()
        .collect()
}

fn utc_ms(text: &str) -> i64 {
    datui::gps::gpx::parse_time(text).unwrap()
}

fn summaries(app: &App) -> Vec<String> {
    app.data_table_state
        .as_ref()
        .unwrap()
        .notes()
        .into_iter()
        .map(|n| n.summary)
        .collect()
}

fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

#[test]
fn an_nmea_log_opens_as_its_fixes() {
    let (options, dir) = scratch();
    let (app, _rx) = open_with(gps().join("drive.nmea"), options);
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(
        names(&df),
        [
            "time",
            "lat",
            "lon",
            "alt",
            "speed",
            "course",
            "sats",
            "hdop",
            "fix",
            "gap",
            "checksum_ok"
        ]
    );
    assert_eq!(df.height(), 300, "one fix a second");
    assert_eq!(
        df.column("time").unwrap().dtype(),
        &DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC))
    );
    let t = times(&df);
    assert_eq!(
        t[0],
        Some(utc_ms("2024-03-09T23:58:00Z")),
        "dated by the RMC after it"
    );
    assert_eq!(
        t[120],
        Some(utc_ms("2024-03-10T00:00:00Z")),
        "across midnight"
    );
    assert_eq!(
        t[150].unwrap() - t[149].unwrap(),
        21_000,
        "the dropout is a gap in time"
    );
    let gap = df.column("gap").unwrap().f64().unwrap();
    assert_eq!(
        (gap.get(0), gap.get(1), gap.max()),
        (None, Some(1.0), Some(21.0))
    );
    let speed = df.column("speed").unwrap().f64().unwrap();
    assert!(
        speed.max().unwrap() > 70.0,
        "the spike, in m/s: {:?}",
        speed.max()
    );
    let lat = df.column("lat").unwrap().f64().unwrap();
    assert!((lat.get(0).unwrap() - 47.37698).abs() < 1e-4);
    let ok = df.column("checksum_ok").unwrap().bool().unwrap();
    assert_eq!(ok.sum(), Some(299), "one VTG has a bad checksum");

    let notes = summaries(&app);
    assert!(
        notes.iter().any(|n| n == "1 line is not NMEA and left out"),
        "{notes:?}"
    );
    assert!(
        notes
            .iter()
            .any(|n| n.starts_with("1 sentence fails its checksum")),
        "{notes:?}"
    );
    assert_eq!(notes.len(), 2, "the other tables are not a note: {notes:?}");
    let state = app.data_table_state.as_ref().unwrap();
    let others = state.other_tables();
    assert!(
        others.iter().any(|t| t == "GSV 600") && others.iter().any(|t| t == "sentences"),
        "the Schema tab names the other tables: {others:?}"
    );
    assert!(state.scans_a_temp_file());
    assert_eq!(files_in(&dir), 1, "one converted file");
    drop(app);
    assert_eq!(files_in(&dir), 0, "removed with the dataset");
}

#[test]
fn table_opens_one_sentence_type() {
    let (options, _) = scratch();
    let (app, _rx) = open_with(
        gps().join("drive.nmea"),
        OpenOptions {
            table: Some("gsv".to_string()),
            ..options
        },
    );
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(df.height(), 2400, "a row per satellite per second");
    let prn: Vec<u32> = df
        .column("prn")
        .unwrap()
        .u32()
        .unwrap()
        .into_no_null_iter()
        .take(8)
        .collect();
    assert_eq!(prn, [1, 3, 7, 8, 11, 17, 19, 28]);
    assert_eq!(times(&df)[0], Some(utc_ms("2024-03-09T23:58:00Z")));

    let (options, _) = scratch();
    let (app, _rx) = open_with(
        gps().join("drive.nmea"),
        OpenOptions {
            table: Some("sentences".to_string()),
            ..options
        },
    );
    let df = frame(&app);
    assert_eq!(
        df.height(),
        300 * 6 - 1 + 1,
        "every sentence: six a second, the first RMC missing, one vendor's"
    );

    let (options, _) = scratch();
    let (app, _rx) = open_with(
        gps().join("drive.nmea"),
        OpenOptions {
            table: Some("XYZ".to_string()),
            ..options
        },
    );
    let message = app.error_message().expect("an unknown table is refused");
    assert!(message.contains("fixes, GGA, RMC"), "{message}");

    // A file of one table is refused too, rather than opened as if it were the table.
    for path in [
        gps().join("ride.gpx"),
        PathBuf::from("tests/sample-data/people.parquet"),
    ] {
        let (options, _) = scratch();
        let (app, _rx) = open_with(
            path,
            OpenOptions {
                table: Some("GGA".to_string()),
                ..options
            },
        );
        let message = app
            .error_message()
            .expect("--table on one table is refused");
        assert!(message.contains("holds one table"), "{message}");
    }
}

#[test]
fn a_gpx_file_opens_as_its_points() {
    let (options, _) = scratch();
    let (app, _rx) = open_with(gps().join("ride.gpx"), options);
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(
        names(&df),
        [
            "time",
            "lat",
            "lon",
            "ele",
            "kind",
            "track",
            "track_name",
            "segment",
            "gap",
            "name",
            "sym",
            "hr",
            "cad",
            "atemp"
        ]
    );
    assert_eq!(df.height(), 2 + 150 + 3);
    let kinds = df.column("kind").unwrap().str().unwrap();
    assert_eq!(kinds.get(0), Some("waypoint"));
    assert_eq!(kinds.get(2), Some("track"));
    assert_eq!(kinds.get(154), Some("route"));
    assert_eq!(df.column("hr").unwrap().dtype(), &DataType::Int64);
    assert_eq!(df.column("atemp").unwrap().null_count(), 2 + 100 + 3);
    let segment = df.column("segment").unwrap().u32().unwrap();
    assert_eq!((segment.get(2), segment.get(102)), (Some(0), Some(1)));
    let names = df.column("track_name").unwrap().str().unwrap();
    assert_eq!(names.get(2), Some("Morning ride"));
    assert_eq!(names.get(154), Some("Way home"));
    assert_eq!(
        df.column("name").unwrap().str().unwrap().get(0),
        Some("Start & finish")
    );
    assert_eq!(times(&df)[2], Some(utc_ms("2024-05-01T06:00:01Z")));
}

/// A log whose name says nothing opens by its first bytes; a text file that is not
/// one is still refused as before.
#[test]
fn a_log_is_known_by_its_first_bytes() {
    let (options, dir) = scratch();
    let log = dir.join("capture.log");
    std::fs::copy(gps().join("drive.nmea"), &log).unwrap();
    let (app, _rx) = open_with(log, options.clone());
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 300);

    let notes = dir.join("notes.txt");
    std::fs::write(&notes, "just some notes\n").unwrap();
    let (app, _rx) = open_with(notes, options);
    assert!(app.error_message().is_some(), "not a GPS log");
}

/// A directory of GPX activities opens as one table with a `file` column, its logs
/// converted together and removed with the dataset; the home screen offers it as one.
#[test]
fn a_directory_of_activities_is_one_table() {
    let (options, dir) = scratch();
    let logs = dir.join("activities");
    std::fs::create_dir(&logs).unwrap();
    for name in ["monday.gpx", "tuesday.gpx"] {
        std::fs::copy(gps().join("ride.gpx"), logs.join(name)).unwrap();
    }
    let (kind, holds) = datui::discover::look_at_directory(&logs);
    assert_eq!(kind, datui::discover::EntryKind::MultiFile);
    assert_eq!(holds.formats, [("gpx".to_string(), 2)]);

    let (app, _rx) = open_with(logs.clone(), options);
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(names(&df)[..3], ["file", "time", "lat"]);
    assert_eq!(df.height(), 2 * (2 + 150 + 3));
    let files = df.column("file").unwrap().str().unwrap();
    assert_eq!(
        (files.get(0), files.get(155)),
        (Some("monday.gpx"), Some("tuesday.gpx"))
    );
    assert_eq!(df.column("hr").unwrap().dtype(), &DataType::Int64);
    assert!(app.data_table_state.as_ref().unwrap().scans_a_temp_file());
    assert_eq!(files_in(&dir), 3, "the logs' directory and a file per log");
    drop(app);
    assert_eq!(files_in(&dir), 1, "removed with the dataset");
}
