//! Flight logs: PX4 ULog and ArduPilot DataFlash, a table per topic or message type,
//! listed on the home screen like a directory and opened without reading the log
//! again.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/flight/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, drain_events, pump_open_until_loaded};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::home::Row;
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc;

fn flight() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/flight")
}

fn open_with(path: PathBuf, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    (app, rx)
}

fn table(name: &str) -> OpenOptions {
    OpenOptions {
        table: Some(name.to_string()),
        ..OpenOptions::default()
    }
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

fn listed(app: &App) -> Vec<String> {
    app.home
        .visible()
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } => Some(entry.name.clone()),
            _ => None,
        })
        .collect()
}

fn key(app: &mut App, rx: &mpsc::Receiver<AppEvent>, code: KeyCode) {
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(app, rx);
}

/// Handle events until the home screen lists the log's tables.
fn settle_home(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        while let Ok(event) = rx.try_recv() {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
        let listed = app
            .home
            .sections
            .iter()
            .any(|s| s.rows.iter().any(|r| r.table.is_some()));
        if !app.home.listing_in_flight && listed {
            let area = ratatui::layout::Rect::new(0, 0, 100, 30);
            let mut buf = ratatui::buffer::Buffer::empty(area);
            ratatui::widgets::Widget::render(&mut *app, area, &mut buf);
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the listing never came"
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(20)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
    }
}

/// A ULog file lands on the home screen inside it, a row per topic and instance and
/// the text and parameter tables; Enter opens one, q comes back to the list.
#[test]
fn a_ulog_lands_on_its_topics() {
    let ulg = flight().join("flight.ulg");
    let (mut app, rx) = open_with(ulg.clone(), OpenOptions::default());
    settle_home(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.input_mode, InputMode::Home);
    assert_eq!(app.home.browsing.as_deref(), Some(ulg.as_path()));
    assert_eq!(
        listed(&app),
        [
            "sensor_accel.0",
            "sensor_accel.1",
            "vehicle_status",
            "logged_messages",
            "parameters"
        ]
    );
    let at = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, Row::Entry { entry, .. } if entry.name == "sensor_accel.1"))
        .unwrap();
    app.home.select(at);
    key(&mut app, &rx, KeyCode::Enter);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.input_mode, InputMode::Normal, "{:?}", app.home.status);
    let df = frame(&app);
    assert_eq!(df.height(), 50);
    assert_eq!(
        names(&df),
        [
            "timestamp",
            "device_id",
            "accel.x",
            "accel.y",
            "accel.z",
            "temperature",
            "raw"
        ]
    );
    assert_eq!(
        df.column("device_id").unwrap().u32().unwrap().get(0),
        Some(102)
    );
    key(&mut app, &rx, KeyCode::Char('q'));
    settle_home(&mut app, &rx);
    assert_eq!(app.home.browsing.as_deref(), Some(ulg.as_path()));
}

#[test]
fn a_ulog_topic_by_table() {
    let (app, _rx) = open_with(flight().join("flight.ulg"), table("sensor_accel.0"));
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    // 100 written, one cut off by the end of the file.
    assert_eq!(df.height(), 100);
    assert_eq!(
        df.column("timestamp").unwrap().dtype(),
        &DataType::Duration(TimeUnit::Microseconds)
    );
    assert_eq!(
        df.column("raw").unwrap().dtype(),
        &DataType::Array(Box::new(DataType::Int16), 3)
    );
    let x = df.column("accel.x").unwrap().f32().unwrap();
    assert_eq!(x.get(99), Some(49.5));
    let state = app.data_table_state.as_ref().unwrap();
    let detail = state.format_detail().expect("the ULog tab");
    assert_eq!(detail.tab, "ULog");
    assert!(
        detail.list.iter().any(|(k, _)| k == "sys_name"),
        "{:?}",
        detail.list
    );
    let notes: Vec<String> = state.notes().iter().map(|n| n.summary.clone()).collect();
    assert!(notes.iter().any(|n| n.contains("passed over")), "{notes:?}");
    assert!(notes.iter().any(|n| n.contains("cut short")), "{notes:?}");

    let (app, _rx) = open_with(flight().join("flight.ulg"), table("vehicle_status"));
    let df = frame(&app);
    assert_eq!(df.height(), 10);
    let mode = df.column("mode").unwrap().str().unwrap();
    assert_eq!(mode.get(0), Some("MANUAL"));
    assert_eq!(mode.get(9), Some("MISSION"));

    let (app, _rx) = open_with(flight().join("flight.ulg"), table("logged_messages"));
    let df = frame(&app);
    assert_eq!(df.height(), 2);
    let text = df.column("message").unwrap().str().unwrap();
    assert_eq!(text.get(1), Some("Low battery"));
    assert_eq!(
        df.column("level").unwrap().str().unwrap().get(1),
        Some("warning")
    );

    let (app, _rx) = open_with(flight().join("flight.ulg"), table("parameters"));
    let df = frame(&app);
    assert_eq!(df.height(), 3, "two at the start and one change in flight");
}

#[test]
fn a_dataflash_log_by_its_first_bytes() {
    let bin = flight().join("00000042.BIN");
    let (mut app, rx) = open_with(bin.clone(), OpenOptions::default());
    settle_home(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(
        listed(&app),
        ["ATT", "FMTU", "GPS", "MSG", "MULT", "PARM", "UNIT"]
    );

    let (app, _rx) = open_with(bin.join("GPS"), OpenOptions::default());
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(df.height(), 50);
    assert_eq!(names(&df), ["TimeUS", "Status", "Lat", "Lng", "Alt", "Ms"]);
    let lat = df.column("Lat").unwrap().f64().unwrap().get(0).unwrap();
    assert!((lat - 47.3977418).abs() < 1e-9, "{lat}");
    assert_eq!(df.column("Alt").unwrap().f64().unwrap().get(0), Some(488.5));
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.unit_of("Lat"), Some("deglatitude"));
    assert_eq!(state.unit_of("Alt"), Some("m"));
    assert_eq!(state.format_detail().map(|d| d.tab), Some("DataFlash"));

    let (app, _rx) = open_with(bin, table("ATT"));
    let df = frame(&app);
    assert_eq!(df.height(), 200, "the record cut off at the end is not one");
    let roll = df.column("Roll").unwrap().f64().unwrap();
    assert_eq!(roll.get(0), Some(1.5));
    let state = app.data_table_state.as_ref().unwrap();
    let notes: Vec<String> = state.notes().iter().map(|n| n.summary.clone()).collect();
    assert!(notes.iter().any(|n| n.contains("passed over")), "{notes:?}");
}

/// The list comes from the pass the open made: a table chosen from it reads the same
/// index, and the home screen counts the tables once the log has been opened.
#[test]
fn the_home_screen_lists_an_opened_log() {
    let ulg = flight().join("flight.ulg");
    let (app, _rx) = open_with(ulg.clone(), table("vehicle_status"));
    assert_eq!(app.error_message(), None);
    let mut entry = datui::discover::Entry::for_test(&ulg, "flight.ulg");
    datui::discover::enrich(&mut entry);
    assert_eq!(entry.cost.tables, Some(5));
    let row = datui::discover::table_row(&ulg.join("sensor_accel.0")).unwrap();
    assert_eq!(row.columns.first().map(String::as_str), Some("timestamp"));
}
