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

comment = "#"
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

/// Opened as `datui FILE` opens it: text columns typed (`--infer-types`).
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
            .any(|n| n == "read as acme.instrument-log, matched by magic #device_info"),
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

/// A unit stays on the loaded column, renamed or not, and never lands on a column a
/// query computes under a name that has one.
#[test]
fn a_computed_column_that_reuses_a_name_has_no_unit() {
    let path = common::fixture_dir().join("delimited_spec_computed.csv");
    std::fs::write(&path, log_text("2024-03-01", 6)).unwrap();
    let (mut app, rx, tx) = app_with_spec();
    open(&mut app, &rx, path, OpenOptions::default());
    let unit = |app: &App, column: &str| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .unit_of(column)
            .map(str::to_string)
    };

    run_query(
        &mut app,
        &rx,
        &tx,
        "select v: volts, volts: volts * 1000, cht1",
    );
    assert_eq!(unit(&app, "v").as_deref(), Some("volts"), "renamed");
    assert_eq!(unit(&app, "volts"), None, "computed");
    assert_eq!(unit(&app, "cht1").as_deref(), Some("deg F"));
    let shown = screen(&mut app);
    assert!(!shown.contains("f64 · volts  f64 · volts"), "{shown}");

    run_query(&mut app, &rx, &tx, "select cht1: count volts by UTCOfst");
    assert_eq!(unit(&app, "cht1"), None, "a count");
    assert_eq!(unit(&app, "UTCOfst").as_deref(), Some("hh:mm"), "a key");

    // A melt carries its id columns; its value column is not any one of them.
    let melt = datui::pivot_melt_modal::MeltSpec {
        index: vec!["UTCOfst".to_string()],
        value_columns: vec!["volts".to_string(), "cht1".to_string()],
        variable_name: "variable".to_string(),
        value_name: "Latitude".to_string(),
    };
    run_query(&mut app, &rx, &tx, "select UTCOfst, volts, cht1");
    app.data_table_state.as_mut().unwrap().melt(&melt).unwrap();
    assert_eq!(unit(&app, "UTCOfst").as_deref(), Some("hh:mm"));
    assert_eq!(unit(&app, "Latitude"), None, "the melt's values");

    run_query(&mut app, &rx, &tx, "");
    assert_eq!(unit(&app, "volts").as_deref(), Some("volts"), "reset");
}

#[cfg(feature = "sql")]
#[test]
fn sql_keeps_units_only_on_columns_it_passes_through() {
    let path = common::fixture_dir().join("delimited_spec_sql.csv");
    std::fs::write(&path, log_text("2024-03-01", 6)).unwrap();
    let (mut app, rx, tx) = app_with_spec();
    open(&mut app, &rx, path, OpenOptions::default());
    let sql = |app: &mut App, statement: &str| {
        app.event(&AppEvent::SqlQuery(statement.to_string()));
        pump_until_idle(app, &rx, &tx);
        let state = app.data_table_state.as_ref().unwrap();
        assert!(state.error().is_none(), "{statement}: {:?}", state.error());
    };
    let unit = |app: &App, column: &str| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .unit_of(column)
            .map(str::to_string)
    };

    sql(&mut app, "SELECT * FROM df WHERE cht1 > 182");
    assert_eq!(unit(&app, "cht1").as_deref(), Some("deg F"));
    assert_eq!(unit(&app, "volts").as_deref(), Some("volts"));

    sql(
        &mut app,
        "SELECT cht1 * 2 AS cht1, volts AS v, COUNT(*) AS volts FROM df GROUP BY cht1, volts",
    );
    assert_eq!(unit(&app, "cht1"), None);
    assert_eq!(unit(&app, "v").as_deref(), Some("volts"));
    assert_eq!(unit(&app, "volts"), None);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.units(), [("v".to_string(), "volts".to_string())]);
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

const SEMICOLON_SPEC: &str = r##"
name = "acme.semicolons"
kind = "delimited"
match = { magic = "#semi" }

delimiter = ";"
comment = "#"
"##;

/// A flag typed on the command line wins over the spec's option; one only in the
/// options (config) loses to it (#651).
#[test]
fn a_typed_delimiter_wins_over_the_spec() {
    let path = common::fixture_dir().join("delimited_spec_typed.csv");
    std::fs::write(&path, "#semi\na,b;c\n1,2;3\n").unwrap();
    let opened = |options: OpenOptions| {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.set_formats(Registry::of(vec![
            Spec::parse(SEMICOLON_SPEC, None).unwrap(),
        ]));
        pump_open_until_loaded(&mut app, &rx, vec![path.clone()], options);
        assert!(app.error_message().is_none(), "{:?}", app.error_message());
        collected(&app)
            .get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(opened(OpenOptions::default()), ["a,b", "c"], "the spec's ;");
    let configured = OpenOptions {
        delimiter: Some(b','),
        ..OpenOptions::default()
    };
    assert_eq!(opened(configured), ["a,b", "c"], "untyped, the spec wins");
    let typed = OpenOptions {
        delimiter: Some(b','),
        typed_dialect: datui::TypedDialect {
            delimiter: true,
            ..Default::default()
        },
        ..OpenOptions::default()
    };
    assert_eq!(opened(typed), ["a", "b;c"], "typed, the flag wins");
}

/// `text` followed by `nuls` NUL bytes, as a logger that preallocates its file leaves it.
fn padded(text: &str, nuls: usize) -> Vec<u8> {
    let mut bytes = text.as_bytes().to_vec();
    bytes.resize(bytes.len() + nuls, 0);
    bytes
}

/// A run of NULs after the last line ends the file: no junk row, the numbers stay
/// numbers, and the count agrees with the rows shown. A NUL inside the text is kept.
#[test]
fn a_nul_tail_ends_a_csv() {
    let dir = common::fixture_dir().join("nul_tail");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("padded.csv");
    std::fs::write(
        &path,
        padded("id,volts,name\n1,25.1,a\n2,25.2,b\n3,25.3,c\n", 4096),
    )
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    pump_until_idle(&mut app, &rx, &tx);
    let df = collected(&app);
    assert_eq!(df.height(), 3);
    assert_eq!(df.column("id").unwrap().dtype(), &DataType::Int64);
    assert_eq!(df.column("volts").unwrap().dtype(), &DataType::Float64);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.num_rows_if_valid(),
        Some(3),
        "the count is of the rows shown"
    );

    let inside = dir.join("inside.csv");
    std::fs::write(&inside, "id,name\n1,a\0b\n2,c\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![inside], OpenOptions::default());
    let df = collected(&app);
    assert_eq!(df.height(), 2);
    let name = df
        .column("name")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap()
        .to_string();
    assert_eq!(name, "a\0b", "an interior NUL is the text's");
}

/// A spec's log padded with NULs reads its header lines and its rows up to the padding,
/// typed, without `comment = "\u0000"`.
#[test]
fn a_padded_log_reads_through_its_spec() {
    let path = common::fixture_dir().join("delimited_spec_padded.csv");
    std::fs::write(&path, padded(&log_text("2024-03-01", 5), 32768)).unwrap();
    let (mut app, rx, tx) = app_with_spec();
    open(&mut app, &rx, path, OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let df = collected(&app);
    assert_eq!(df.height(), 6, "the blank row and five readings");
    assert_eq!(df.column("cht1").unwrap().dtype(), &DataType::Float64);
    assert_eq!(df.column("Latitude").unwrap().dtype(), &DataType::Float64);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(6));
    assert_eq!(state.unit_of("cht1"), Some("deg F"));
}

/// A fresh directory under the fixture directory, emptied of a run before.
fn fresh_dir(name: &str) -> PathBuf {
    let dir = common::fixture_dir().join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn open_dir(app: &mut App, rx: &mpsc::Receiver<AppEvent>, dir: PathBuf) {
    let options = OpenOptions {
        hive: true,
        parse_strings: Some(datui::ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(app, rx, vec![dir], options);
}

fn note_summaries(app: &App) -> Vec<String> {
    let state = app.data_table_state.as_ref().unwrap();
    state.notes().into_iter().map(|n| n.summary).collect()
}

/// A file with no header in a directory, all NUL or empty, is passed over with a note;
/// the rest open as one table, and a first file with nothing in it gives up its place
/// as the one the units are read from.
#[test]
fn a_file_with_no_header_is_skipped_in_a_directory() {
    let dir = fresh_dir("delimited_spec_no_header");
    std::fs::write(dir.join("log_000.csv"), vec![0u8; 32768]).unwrap();
    std::fs::write(
        dir.join("log_001.csv"),
        padded(&log_text("2024-03-01", 3), 4096),
    )
    .unwrap();
    std::fs::write(dir.join("log_002.csv"), "").unwrap();
    std::fs::write(dir.join("log_003.csv"), log_text("2024-03-02", 3)).unwrap();
    let (mut app, rx, tx) = app_with_spec();
    open_dir(&mut app, &rx, dir.clone());
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(8));
    assert_eq!(state.unit_of("cht1"), Some("deg F"));
    let read = state.delimited_read().unwrap();
    assert_eq!(read.facts_from.as_deref(), Some("log_001.csv"));
    let notes = note_summaries(&app);
    assert!(
        notes
            .iter()
            .any(|n| n == "2 files with no header skipped: log_000.csv, log_002.csv"),
        "{notes:?}"
    );

    // Without a spec, a plain CSV directory passes them over too.
    let plain = fresh_dir("csv_no_header");
    std::fs::write(plain.join("a.csv"), vec![0u8; 100]).unwrap();
    std::fs::write(plain.join("b.csv"), "x,y\n1,2\n").unwrap();
    std::fs::write(plain.join("c.csv"), " \n").unwrap();
    std::fs::write(plain.join("d.csv"), "x,y\n3,4\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    open_dir(&mut app, &rx, plain);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert_eq!(collected(&app).height(), 2);
    let notes = note_summaries(&app);
    assert!(
        notes
            .iter()
            .any(|n| n == "2 files with no header skipped: a.csv, c.csv"),
        "{notes:?}"
    );
}

/// Every file empty is an error that says so; one all-NUL file opened alone says its
/// header is past its end, as an empty file does; a short file with text still fails
/// a directory read, by its name.
#[test]
fn files_with_no_header_alone_are_an_error() {
    let dir = fresh_dir("delimited_spec_all_empty");
    for name in ["log_000.csv", "log_001.csv", "log_002.csv"] {
        std::fs::write(dir.join(name), vec![0u8; 4096]).unwrap();
    }
    std::fs::write(dir.join("log_003.csv"), "").unwrap();
    let (mut app, rx, _tx) = app_with_spec();
    // Nothing matches the spec's magic in a file of NULs: read as plain CSV.
    open_dir(&mut app, &rx, dir.clone());
    let message = app.error_message().expect("an error");
    assert!(
        message.contains("one of these 4 files has a header"),
        "{message}"
    );

    let one = dir.join("log_000.csv");
    let (mut app, rx, _tx) = app_with_spec();
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![one],
        OpenOptions {
            spec_name: Some("acme.instrument-log".into()),
            ..OpenOptions::default()
        },
    );
    let message = app.error_message().expect("an error");
    assert!(
        message.contains("log_000.csv") && message.contains("past the end of the file"),
        "{message}"
    );

    let short = fresh_dir("delimited_spec_short");
    std::fs::write(short.join("log_001.csv"), log_text("2024-03-01", 3)).unwrap();
    std::fs::write(short.join("log_002.csv"), "#device_info, a=\"1\"\n#units\n").unwrap();
    let (mut app, rx, _tx) = app_with_spec();
    open_dir(&mut app, &rx, short);
    let message = app.error_message().expect("a short file with text fails");
    assert!(
        message.contains("log_002.csv") && message.contains("past the end of the file"),
        "names the file: {message}"
    );
}
