//! CAN logs as candump writes them: the raw frames, and with DBC files a table per
//! message, listed on the home screen like a directory, and a long table of every
//! decoded value.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/can/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, drain_events, pump_open_until_loaded};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::formats::Registry;
use datui::home::Row;
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc;

fn can() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/can")
}

fn log() -> PathBuf {
    can().join("candump-2024-01-31_081640.log")
}

/// A copy of the log of its own, so an index another test keeps for the fixture with
/// other DBC files does not answer for this one.
fn copy_of_log(name: &str) -> PathBuf {
    let dir = common::fixture_dir().join(format!("can-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let copy = dir.join("bus.log");
    std::fs::copy(log(), &copy).unwrap();
    copy
}

fn open_with(
    path: PathBuf,
    options: OpenOptions,
    registry: Registry,
) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_formats(registry);
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    (app, rx)
}

fn with_dbc(table: Option<&str>) -> OpenOptions {
    OpenOptions {
        dbc: Some(can().join("dbc/car.dbc")),
        table: table.map(str::to_string),
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

fn strings(df: &DataFrame, column: &str) -> Vec<Option<String>> {
    df.column(column)
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_string))
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
            "the listing never came: mode {:?}, browsing {:?}, error {:?}, rows {:?}",
            app.input_mode,
            app.home.browsing,
            app.error_message(),
            app.home
                .sections
                .iter()
                .flat_map(|s| s.rows.iter().map(|r| (r.name.clone(), r.table.clone())))
                .collect::<Vec<_>>()
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(20)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
    }
}

/// Without a DBC file, a log opens as its frames, known by its lines.
#[test]
fn a_log_opens_as_its_frames() {
    let (app, _rx) = open_with(
        copy_of_log("raw"),
        OpenOptions::default(),
        Registry::default(),
    );
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(
        df.get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>(),
        [
            "ts", "iface", "id", "ext", "dlc", "data", "fd", "flags", "kind"
        ]
    );
    assert_eq!(
        df.height(),
        303,
        "300 frames and three more, less the comment"
    );
    assert_eq!(
        df.column("ts").unwrap().dtype(),
        &DataType::Datetime(TimeUnit::Microseconds, None)
    );
    let id = strings(&df, "id");
    let kind = strings(&df, "kind");
    assert_eq!(id[1].as_deref(), Some("18FEF1FE"));
    assert!(kind.contains(&Some("remote".to_string())));
    assert!(kind.contains(&Some("error".to_string())));
    let fd = df.column("fd").unwrap().bool().unwrap();
    let at = (0..df.height()).find(|&i| fd.get(i) == Some(true)).unwrap();
    assert_eq!(df.column("dlc").unwrap().u8().unwrap().get(at), Some(12));
    assert_eq!(df.column("flags").unwrap().u8().unwrap().get(at), Some(1));
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.format_detail().map(|d| d.tab), Some("CAN"));
    let notes: Vec<String> = state.notes().iter().map(|n| n.summary.clone()).collect();
    assert!(notes.iter().any(|n| n.contains("not frames")), "{notes:?}");
}

/// The form candump prints reads to the same frames.
#[test]
fn the_printed_form_reads_alike() {
    let (app, _rx) = open_with(
        can().join("printed.txt"),
        OpenOptions::default(),
        Registry::default(),
    );
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(df.height(), 300);
    assert_eq!(strings(&df, "id")[0].as_deref(), Some("123"));
    assert_eq!(df.column("dlc").unwrap().u8().unwrap().get(4), Some(1));
}

/// With a DBC file the log lands on its tables; Enter opens a message, q comes back.
#[test]
fn a_dbc_file_lists_a_table_per_message() {
    let path = copy_of_log("list");
    let (mut app, rx) = open_with(path.clone(), with_dbc(None), Registry::default());
    settle_home(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.input_mode, InputMode::Home);
    assert_eq!(
        listed(&app),
        ["frames", "signals", "BATTERY", "BRAKES", "ENGINE"]
    );
    let at = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, Row::Entry { entry, .. } if entry.name == "ENGINE"))
        .unwrap();
    app.home.select(at);
    key(&mut app, &rx, KeyCode::Enter);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.input_mode, InputMode::Normal, "{:?}", app.home.status);
    let df = frame(&app);
    assert_eq!(df.height(), 51, "50 classic frames and the FD one");
    key(&mut app, &rx, KeyCode::Char('q'));
    settle_home(&mut app, &rx);
    assert_eq!(app.home.browsing.as_deref(), Some(path.as_path()));
}

/// Intel and Motorola bits, signed values, factor and offset, value names and units.
#[test]
fn signals_decode() {
    let (app, _rx) = open_with(
        copy_of_log("engine"),
        with_dbc(Some("ENGINE")),
        Registry::default(),
    );
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(
        df.get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>(),
        ["ts", "Speed", "Temp", "Gear", "Throttle"]
    );
    // Frame 0: Speed 8000 * 0.125, Temp -10 - 40, Gear 0, Throttle 100 * 0.4.
    assert_eq!(
        df.column("Speed").unwrap().f64().unwrap().get(0),
        Some(1000.0)
    );
    assert_eq!(
        df.column("Temp").unwrap().f64().unwrap().get(0),
        Some(-50.0)
    );
    assert_eq!(strings(&df, "Gear")[0].as_deref(), Some("Neutral"));
    let throttle = df
        .column("Throttle")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap();
    assert!((throttle - 40.0).abs() < 1e-9);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.unit_of("Speed"), Some("rpm"));

    let (app, _rx) = open_with(
        copy_of_log("brakes"),
        with_dbc(Some("BRAKES")),
        Registry::default(),
    );
    let df = frame(&app);
    let pressure = df
        .column("Pressure")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap();
    assert!((pressure - 466.1).abs() < 1e-9, "{pressure}");
    assert_eq!(
        df.column("Balance").unwrap().i64().unwrap().get(0),
        Some(-1)
    );

    // A multiplexed signal is null where its page is not selected.
    let (app, _rx) = open_with(
        copy_of_log("battery"),
        with_dbc(Some("BATTERY")),
        Registry::default(),
    );
    let df = frame(&app);
    let volts = df.column("Volts").unwrap().f64().unwrap();
    let amps = df.column("Amps").unwrap().f64().unwrap();
    assert!((volts.get(0).unwrap() - 100.02).abs() < 1e-9);
    assert_eq!(amps.get(0), None);
    assert_eq!(volts.get(1), None);
    let a = amps.get(1).unwrap();
    assert!((a - -1.3).abs() < 1e-9, "{a}");
    assert_eq!(df.column("Ratio").unwrap().f64().unwrap().get(0), Some(1.5));
}

/// The long table: a row per decoded value, in time order.
#[test]
fn the_long_table_has_every_value() {
    let (app, _rx) = open_with(
        copy_of_log("long"),
        with_dbc(Some("signals")),
        Registry::default(),
    );
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(
        df.get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>(),
        ["ts", "message", "signal", "value", "unit"]
    );
    // ENGINE 51 x 4, BRAKES 50 x 2, BATTERY 100 x (page, its one signal, ratio).
    assert_eq!(df.height(), 51 * 4 + 50 * 2 + 100 * 3);
    let first = strings(&df, "signal");
    assert_eq!(first[0].as_deref(), Some("Speed"));
    let units = strings(&df, "unit");
    assert_eq!(units[0].as_deref(), Some("rpm"));
}

/// DBC files on the search path: one for every interface, and one a TOML file names
/// for can1 only.
#[test]
fn dbc_files_from_the_search_path_match_an_interface() {
    let registry = Registry::load(&[can().join("dbc")]);
    assert_eq!(registry.dbc.len(), 2, "{:?}", registry.errors);
    let (mut app, rx) = open_with(copy_of_log("path"), OpenOptions::default(), registry);
    settle_home(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(
        listed(&app),
        ["frames", "signals", "BATTERY", "BRAKES", "DOORS", "ENGINE"]
    );
}
