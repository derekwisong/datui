//! Opening delimited text through `kind = "delimited"` format specs: header rows with
//! roles, units through a sort and a query, the metadata line in the Info panel, a
//! derived datetime with an offset, a compressed file, and a directory of logs.

use super::*;
use datui::formats::{Registry, Spec};
use std::io::Write as _;

const SPEC: &str = r##"
name = "acme.instrument-log"
kind = "delimited"
match = { magic = "#device_info" }

comment_char = "#"
skip_initial_space = true
header_rows = { name = 3, unit = 2 }
metadata_line = 1

[columns]
time = { from = ["Lcl Date", "Lcl Time", "UTCOfst"], as = "datetime" }
"##;

/// A padded log: a metadata line, a units line, a padded header, a row of blank
/// cells, then `rows` readings on `day` from 10:00:00 local time, at -05:00.
fn log_text(day: &str, rows: usize) -> String {
    let mut text = String::from(
        "#device_info, log_version=\"1.03\", model=\"Unit 7, rev B\", serial=123\n\
         #yyyy-mm-dd, hh:mm:ss, hh:mm,  degrees,  volts,  deg F\n  \
         Lcl Date,   Lcl Time, UTCOfst,  Latitude,  volts,  cht1\n          \
         ,           ,        ,          ,   25.1,  187.2\n",
    );
    for i in 0..rows {
        text.push_str(&format!(
            "{day}, 10:00:{:02},  -05:00, {:9.6},  {:5.1},  {:5.1}\n",
            i % 60,
            40.1 + i as f64 * 1e-3,
            25.0 + (i % 10) as f64 * 0.1,
            180.0 + i as f64,
        ));
    }
    text
}

fn app_with_spec() -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    app.set_formats(Registry::of(vec![Spec::parse(SPEC, None).unwrap()]));
    (app, rx, tx)
}

fn screen(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    rendered_text(&buffer)
}

/// Opened as `datui FILE` opens it: text columns typed (`--parse-strings`).
fn open(app: &mut App, rx: &mpsc::Receiver<AppEvent>, path: PathBuf, options: OpenOptions) {
    let options = OpenOptions {
        parse_strings: Some(datui::ParseStringsTarget::All),
        ..options
    };
    pump_open_until_loaded(app, rx, vec![path], options);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
}

fn collected(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap()
}

#[test]
fn a_log_opens_by_its_magic_with_units_metadata_and_a_utc_time() {
    let path = common::fixture_dir().join("delimited_spec_log.csv");
    std::fs::write(&path, log_text("2024-03-01", 5)).unwrap();
    let (mut app, rx, _tx) = app_with_spec();
    open(&mut app, &rx, path, OpenOptions::default());

    let df = collected(&app);
    assert_eq!(
        df.get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>(),
        [
            "time", "Lcl Date", "Lcl Time", "UTCOfst", "Latitude", "volts", "cht1"
        ],
        "the derived column comes before its first source"
    );
    assert_eq!(df.height(), 6, "the header lines are never data");
    let time = df.column("time").unwrap();
    assert!(time.get(0).unwrap().is_null(), "a blank row has no time");
    assert_eq!(time.get(1).unwrap().to_string(), "2024-03-01 15:00:00 UTC");
    assert_eq!(df.column("cht1").unwrap().dtype(), &DataType::Float64);
    assert_eq!(
        df.column("volts").unwrap().f64().unwrap().get(0),
        Some(25.1),
        "a padded number with blank cells beside it"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let read = state.delimited_read().expect("read through the spec");
    assert_eq!(read.spec.name, "acme.instrument-log");
    assert_eq!(state.unit_of("cht1"), Some("deg F"));
    assert_eq!(state.unit_of("Lcl Date"), Some("yyyy-mm-dd"));
    assert_eq!(state.unit_of("time"), None);
    let metadata = read.metadata.as_ref().unwrap();
    assert_eq!(metadata.title.as_deref(), Some("device_info"));
    assert_eq!(
        metadata.pairs[1],
        ("model".to_string(), "Unit 7, rev B".to_string())
    );
    let notes: Vec<String> = state.notes().into_iter().map(|n| n.summary).collect();
    assert!(
        notes
            .iter()
            .any(|n| n == "read as acme.instrument-log, chosen by its magic"),
        "{notes:?}"
    );

    let shown = screen(&mut app);
    assert!(
        shown.contains("f64 · deg F"),
        "the unit on the type row: {shown}"
    );
}

#[test]
fn units_are_kept_through_a_sort_and_a_query() {
    let path = common::fixture_dir().join("delimited_spec_units.csv");
    std::fs::write(&path, log_text("2024-03-01", 20)).unwrap();
    let (mut app, rx, tx) = app_with_spec();
    open(&mut app, &rx, path, OpenOptions::default());

    app.data_table_state
        .as_mut()
        .unwrap()
        .sort(vec!["cht1".to_string()], false);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(screen(&mut app).contains("f64 · deg F"));

    run_query(&mut app, &rx, &tx, "select time, cht1 where cht1 > 190");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.lf().clone().collect().unwrap().height(), 9);
    assert_eq!(state.unit_of("cht1"), Some("deg F"));
    let shown = screen(&mut app);
    assert!(shown.contains("f64 · deg F"), "{shown}");
    assert!(
        !shown.contains("degrees"),
        "a column the query left out: {shown}"
    );
}

#[test]
fn the_info_panel_shows_the_metadata_and_the_units() {
    let path = common::fixture_dir().join("delimited_spec_info.csv");
    std::fs::write(&path, log_text("2024-03-01", 3)).unwrap();
    let (mut app, rx, _tx) = app_with_spec();
    open(&mut app, &rx, path, OpenOptions::default());

    press(&mut app, KeyCode::Char('i'));
    // The panel opens on the unread notes; walk the tabs to Metadata.
    let mut shown = screen(&mut app);
    for _ in 0..5 {
        if shown.contains("model        Unit 7, rev B") {
            break;
        }
        press(&mut app, KeyCode::Right);
        shown = screen(&mut app);
    }
    assert!(shown.contains("device_info"), "{shown}");
    assert!(shown.contains("Metadata  3"), "{shown}");
    assert!(shown.contains("log_version  1.03"), "{shown}");
    assert!(shown.contains("model        Unit 7, rev B"), "{shown}");
    assert!(shown.contains("serial       123"), "{shown}");

    // Schema: each column's unit beside its type.
    for _ in 0..5 {
        if shown.contains("Unit") && shown.contains("Column") {
            break;
        }
        press(&mut app, KeyCode::Right);
        shown = screen(&mut app);
    }
    let unit_row = shown
        .lines()
        .find(|l| l.contains("cht1"))
        .unwrap_or_default();
    assert!(unit_row.contains("deg F"), "{shown}");
}

#[test]
fn a_metadata_line_that_is_not_pairs_is_shown_as_it_is() {
    let path = common::fixture_dir().join("delimited_spec_raw_meta.csv");
    let text = log_text("2024-03-01", 2).replacen(
        "#device_info, log_version=\"1.03\", model=\"Unit 7, rev B\", serial=123",
        "#device_info exported by hand",
        1,
    );
    std::fs::write(&path, text).unwrap();
    let (mut app, rx, _tx) = app_with_spec();
    open(&mut app, &rx, path, OpenOptions::default());
    let state = app.data_table_state.as_ref().unwrap();
    let metadata = state
        .delimited_read()
        .unwrap()
        .metadata
        .as_ref()
        .unwrap()
        .clone();
    assert!(metadata.pairs.is_empty());
    assert_eq!(metadata.raw, "device_info exported by hand");
    press(&mut app, KeyCode::Char('i'));
    let mut shown = screen(&mut app);
    for _ in 0..5 {
        if shown.contains("line 1") {
            break;
        }
        press(&mut app, KeyCode::Right);
        shown = screen(&mut app);
    }
    assert!(shown.contains("device_info exported by hand"), "{shown}");
}

#[test]
fn a_directory_of_logs_opens_as_one_table() {
    let dir = common::fixture_dir().join("delimited_spec_logs");
    std::fs::create_dir_all(&dir).unwrap();
    for (name, day) in [("log_001.csv", "2024-03-01"), ("log_002.csv", "2024-03-02")] {
        std::fs::write(dir.join(name), log_text(day, 1_500)).unwrap();
    }
    let (mut app, rx, tx) = app_with_spec();
    let options = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    open(&mut app, &rx, dir, options);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(3_002));
    let read = state.delimited_read().expect("each file through the spec");
    assert_eq!(read.facts_from.as_deref(), Some("log_001.csv"));
    assert_eq!(state.unit_of("volts"), Some("volts"));
    let df = state.lf().clone().collect().unwrap();
    let time = df.column("time").unwrap();
    assert!(time.get(1).unwrap().to_string().starts_with("2024-03-01"));
    assert!(
        time.get(df.height() - 1)
            .unwrap()
            .to_string()
            .starts_with("2024-03-02")
    );
}

#[test]
fn a_compressed_log_is_matched_through_its_decompressor() {
    let path = common::fixture_dir().join("delimited_spec_log.csv.gz");
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gz.write_all(log_text("2024-03-01", 4).as_bytes()).unwrap();
    std::fs::write(&path, gz.finish().unwrap()).unwrap();
    let (mut app, rx, _tx) = app_with_spec();
    open(&mut app, &rx, path, OpenOptions::default());
    let df = collected(&app);
    assert_eq!(df.height(), 5);
    assert_eq!(
        df.column("time").unwrap().get(1).unwrap().to_string(),
        "2024-03-01 15:00:00 UTC"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.unit_of("cht1"), Some("deg F"));
}

#[test]
fn format_name_reads_a_file_its_magic_does_not_match() {
    let path = common::fixture_dir().join("delimited_spec_named.txt");
    let text = log_text("2024-03-01", 2).replacen("#device_info", "#other_device", 1);
    std::fs::write(&path, text).unwrap();
    let (mut app, rx, _tx) = app_with_spec();
    let options = OpenOptions {
        spec_name: Some("acme.instrument-log".into()),
        ..OpenOptions::default()
    };
    open(&mut app, &rx, path, options);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.unit_of("cht1"), Some("deg F"));
    let notes: Vec<String> = state.notes().into_iter().map(|n| n.summary).collect();
    assert!(
        notes.iter().any(|n| n.ends_with("chosen by its name")),
        "{notes:?}"
    );
}
