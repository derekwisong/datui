//! The systemd journal as `journalctl -o json` writes it, from a synthetic fixture of
//! twelve entries over two boots: `time`, `level` and readable messages derived, the
//! columns in reading order, a field first seen late, a pipe, `--follow` from a pipe,
//! and Copy as Python.

use super::*;
use std::io::Write as _;

const FIXTURE: &str = "tests/fixtures/journal.json";

fn app() -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    (App::new(tx.clone(), common::test_runtime()), rx, tx)
}

fn open(paths: Vec<PathBuf>, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (mut app, rx, tx) = app();
    pump_open_until_loaded(&mut app, &rx, paths, options);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    (app, rx)
}

/// The table's frame, without the row index that numbers it.
fn frame(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .drop(by_name(["__datui_row"], false, false))
        .collect()
        .unwrap()
}

fn texts(df: &DataFrame, name: &str) -> Vec<Option<String>> {
    df.column(name)
        .unwrap()
        .cast(&DataType::String)
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_string))
        .collect()
}

fn piped(bytes: Vec<u8>, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (reader, mut producer) = std::io::pipe().unwrap();
    let writer = std::thread::spawn(move || {
        producer.write_all(&bytes).unwrap();
    });
    let (mut app, rx, tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from("-")], options);
    pump_until_idle(&mut app, &rx, &tx);
    writer.join().unwrap();
    (app, rx)
}

#[test]
fn a_journal_has_time_level_and_readable_messages_first() {
    let (app, _rx) = open(vec![PathBuf::from(FIXTURE)], OpenOptions::default());
    let state = app.data_table_state.as_ref().unwrap();
    let df = frame(&app);
    let names: Vec<&str> = df.get_column_names().iter().map(|n| n.as_str()).collect();
    assert_eq!(
        names[..5],
        ["time", "level", "_SYSTEMD_UNIT", "_PID", "MESSAGE"]
    );
    // Every field stays, bookkeeping last.
    let tail: Vec<&str> = names[names.len() - 8..].to_vec();
    for kept in [
        "__CURSOR",
        "__REALTIME_TIMESTAMP",
        "__SEQNUM",
        "_BOOT_ID",
        "_MACHINE_ID",
    ] {
        assert!(tail.contains(&kept), "{kept} among {tail:?}");
    }
    assert!(names.contains(&"SYSLOG_IDENTIFIER") && names.contains(&"_HOSTNAME"));

    assert_eq!(
        df.column("time").unwrap().dtype(),
        &DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
    );
    let time = texts(&df, "time");
    assert!(
        time[0]
            .as_deref()
            .unwrap()
            .starts_with("2026-01-01 00:00:00"),
        "{time:?}"
    );
    assert!(matches!(
        df.column("level").unwrap().dtype(),
        DataType::Enum(..)
    ));
    let levels = texts(&df, "level");
    assert_eq!(levels[0].as_deref(), Some("info"));
    assert_eq!(levels[3].as_deref(), Some("warning"));
    assert_eq!(levels[9].as_deref(), Some("emerg"));

    // A message journalctl wrote as bytes reads as text, lossily.
    let messages = texts(&df, "MESSAGE");
    assert_eq!(
        messages[6].as_deref(),
        Some("status: \u{fffd}\x1b[1mok"),
        "{messages:?}"
    );
    let notes: Vec<String> = state.notes().into_iter().map(|n| n.summary).collect();
    assert!(
        notes
            .iter()
            .any(|n| n.starts_with("1 message stored as bytes")),
        "{notes:?}"
    );
}

/// `level` is ordered by severity: a filter keeps the entries at `err` or worse, and a
/// sort puts `emerg` first.
#[test]
fn level_filters_and_sorts_by_severity() {
    let (mut app, rx) = open(vec![PathBuf::from(FIXTURE)], OpenOptions::default());
    let mut next = Some(AppEvent::QQuery(
        "select level, MESSAGE where level <= \"err\"".to_string(),
    ));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    let df = frame(&app);
    assert_eq!(df.height(), 4, "emerg, alert, crit and err");
    let sorted = frame(&app).sort(["level"], Default::default()).unwrap();
    assert_eq!(texts(&sorted, "level")[0].as_deref(), Some("emerg"));
}

#[test]
fn journal_json_is_known_from_a_pipe() {
    let (app, _rx) = piped(std::fs::read(FIXTURE).unwrap(), OpenOptions::default());
    let df = frame(&app);
    assert_eq!(df.height(), 12);
    assert_eq!(df.get_column_names()[0].as_str(), "time");
}

/// A journal piped in larger than the first rows shown, as `journalctl -o json -n
/// 1000 | datui` sends: it shows its rows and reads on to the end.
#[test]
fn a_large_journal_from_a_pipe_shows_its_rows() {
    let fixture = std::fs::read(FIXTURE).unwrap();
    let bytes: Vec<u8> = std::iter::repeat_n(fixture.as_slice(), 300)
        .flatten()
        .copied()
        .collect();
    let (reader, mut producer) = std::io::pipe().unwrap();
    let writer = std::thread::spawn(move || producer.write_all(&bytes));
    let (mut app, rx, tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert_eq!(frame(&app).get_column_names()[..2], ["time", "level"]);
    let _ = writer.join();
}

/// NDJSON piped in, larger than the first rows: read to its end and shown whole, never
/// cut mid-object.
#[test]
fn a_large_ndjson_pipe_shows_every_row() {
    let bytes: Vec<u8> = (0..5_000)
        .flat_map(|i| {
            format!("{{\"id\": {i}, \"note\": \"{}\"}}\n", "x".repeat(i % 40)).into_bytes()
        })
        .collect();
    let (reader, mut producer) = std::io::pipe().unwrap();
    let writer = std::thread::spawn(move || producer.write_all(&bytes));
    let (mut app, rx, tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions::default(),
    );
    // The rows show as they arrive, so idle is not the end of the pipe: wait for
    // the last row, or for an error.
    pump_until(&mut app, &rx, &tx, |app| {
        app.error_message().is_some() || frame(app).height() == 5_000
    });
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert_eq!(frame(&app).height(), 5_000);
    let _ = writer.join();
}

/// A field first seen after a thousand entries is a column too.
#[test]
fn a_field_first_seen_late_is_a_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("late.jsonl");
    let mut text = String::new();
    for i in 0..1500u64 {
        let late = if i == 1499 {
            ",\"CODE_FILE\":\"late.c\""
        } else {
            ""
        };
        text.push_str(&format!(
            "{{\"__CURSOR\":\"s=1;i={i:x}\",\"__REALTIME_TIMESTAMP\":\"{}\",\"PRIORITY\":\"6\",\"MESSAGE\":\"m{i}\"{late}}}\n",
            1_767_225_600_000_000u64 + i
        ));
    }
    std::fs::write(&path, text).unwrap();
    let (app, _rx) = open(vec![path], OpenOptions::default());
    let df = frame(&app);
    let late = texts(&df, "CODE_FILE");
    assert_eq!(late[1499].as_deref(), Some("late.c"));
    assert_eq!(late.iter().filter(|v| v.is_some()).count(), 1);
}

/// `journalctl -o json -f | datui -f -`: the derived columns from the first entries
/// on, and the entries that arrive after.
#[test]
fn a_followed_journal_from_a_pipe_keeps_its_columns() {
    let lines: Vec<String> = std::fs::read_to_string(FIXTURE)
        .unwrap()
        .lines()
        .map(|l| format!("{l}\n"))
        .collect();
    let (reader, mut producer) = std::io::pipe().unwrap();
    for line in &lines[..6] {
        producer.write_all(line.as_bytes()).unwrap();
    }
    let (mut app, rx, _tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions {
            follow: true,
            ..Default::default()
        },
    );
    drain_events(&mut app, &rx);
    assert_eq!(frame(&app).get_column_names()[..2], ["time", "level"]);
    for line in &lines[6..] {
        producer.write_all(line.as_bytes()).unwrap();
    }
    drop(producer);
    let deadline = std::time::Instant::now() + common::HANG_GUARD;
    while !(app.follow().is_some_and(|f| f.shown() == 12) && app.follow_settled()) {
        assert!(
            std::time::Instant::now() < deadline,
            "the follow never got there"
        );
        app.check_follow_now();
        app.request_what_the_frame_needs();
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
            drain_events(&mut app, &rx);
        }
    }
    let df = frame(&app);
    assert_eq!(df.height(), 12);
    assert_eq!(texts(&df, "level")[9].as_deref(), Some("emerg"));
    // A message as bytes arriving later is read as text, and fits.
    assert_eq!(
        texts(&df, "MESSAGE")[6].as_deref(),
        Some("status: \u{fffd}\x1b[1mok")
    );
    assert_eq!(app.follow().unwrap().misfits(), 0);
}

/// Copy as Python reads the journal and derives the same columns.
#[test]
fn copy_as_python_derives_the_journal_s_columns() {
    let (app, _rx) = open(vec![PathBuf::from(FIXTURE)], OpenOptions::default());
    let Some((rows, script)) = run_python_script(&app) else {
        eprintln!("skipped: no .venv to run the scripts with");
        return;
    };
    assert!(script.contains("infer_schema_length=None"), "{script}");
    assert_eq!(rows, view_csv(&app), "{script}");
}

/// Hand the app what arrives, asking the watcher to look each time, until `done`.
#[track_caller]
fn follow_until(app: &mut App, rx: &mpsc::Receiver<AppEvent>, done: impl Fn(&App) -> bool) {
    let caller = std::panic::Location::caller();
    let deadline = std::time::Instant::now() + common::HANG_GUARD;
    while !done(app) {
        assert!(
            std::time::Instant::now() < deadline,
            "the follow at {caller} never got there"
        );
        assert!(app.error_message().is_none(), "{:?}", app.error_message());
        app.check_follow_now();
        app.request_what_the_frame_needs();
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
            drain_events(app, rx);
        }
    }
}

/// Synthetic journal entry `i`: fields vary from line to line, `CODE_FILE` on every
/// seventh, and `LATE_FIELD` on every third from entry `late` on.
fn entry(i: usize, late: usize) -> String {
    let code = if i.is_multiple_of(7) {
        ",\"CODE_FILE\":\"main.c\""
    } else {
        ""
    };
    let extra = if i >= late && i.is_multiple_of(3) {
        format!(",\"LATE_FIELD\":\"late {i}\"")
    } else {
        String::new()
    };
    format!(
        "{{\"__CURSOR\":\"s=1;i={i:x}\",\"__REALTIME_TIMESTAMP\":\"{}\",\"PRIORITY\":\"{}\",\
         \"_SYSTEMD_UNIT\":\"unit{}.service\",\"_BOOT_ID\":\"b\",\"MESSAGE\":\"m{i}\"{code}{extra}}}\n",
        1_767_225_600_000_000u64 + i as u64,
        i % 8,
        i % 5
    )
}

fn ended(app: &App) -> bool {
    app.follow()
        .is_some_and(|f| *f.standing() == datui::follow::Standing::Ended)
        && app.follow_settled()
        && !app.is_busy()
}

/// Rows the follow shows.
fn shown(app: &App) -> usize {
    app.follow().map_or(0, |f| f.shown())
}

/// Write `bytes` from `at` on in steps cut anywhere, mid-entry included, each read
/// before the next lands: every read of every row stops at the last whole entry.
fn write_in_steps(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    producer: &mut std::io::PipeWriter,
    bytes: &[u8],
    mut at: usize,
    step: usize,
) {
    while at < bytes.len() {
        let to = (at + step).min(bytes.len());
        producer.write_all(&bytes[at..to]).unwrap();
        let whole = bytes[..to].iter().filter(|&&b| b == b'\n').count();
        follow_until(app, rx, |app| shown(app) == whole && app.follow_settled());
        assert_eq!(frame(app).height(), whole, "at byte {to}");
        at = to;
    }
}

/// `journalctl -o json | datui` from a producer slower than the copy: the first rows
/// show while it still sends, each read of the spool stops at the last whole entry
/// wherever the producer has got to, and once it ends every entry is there, with the
/// field first seen late a column at the end.
#[test]
fn a_slow_journal_pipe_shows_rows_before_it_ends() {
    let (total, late) = (6_000, 4_000);
    let bytes: Vec<u8> = (0..total)
        .flat_map(|i| entry(i, late).into_bytes())
        .collect();
    let first = bytes
        .iter()
        .enumerate()
        .filter(|(_, b)| **b == b'\n')
        .nth(1_999)
        .unwrap()
        .0
        + 41;
    let (reader, mut producer) = std::io::pipe().unwrap();
    // Two thousand entries, and part of the next: more than a pipe holds, so written
    // while the app reads.
    let head = bytes[..first].to_vec();
    let writer = std::thread::spawn(move || {
        producer.write_all(&head).unwrap();
        producer
    });
    let (mut app, rx, _tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions::default(),
    );
    let mut producer = writer.join().unwrap();
    drain_events(&mut app, &rx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert!((1..total).contains(&shown(&app)), "{}", shown(&app));
    assert!(app.follow().is_some_and(|f| f.live()));
    assert_eq!(frame(&app).get_column_names()[..2], ["time", "level"]);
    write_in_steps(&mut app, &rx, &mut producer, &bytes, first, 9_973);
    drop(producer);
    follow_until(&mut app, &rx, ended);
    let df = frame(&app);
    assert_eq!(df.height(), total);
    // On screen, the new column goes on the end.
    let area = Rect::new(0, 0, 160, 30);
    app.render(area, &mut Buffer::empty(area));
    follow_until(&mut app, &rx, |app| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .display_slice_df()
            .is_some()
    });
    let shown_df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .display_slice_df()
        .unwrap();
    let names: Vec<&str> = shown_df
        .get_column_names()
        .iter()
        .map(|n| n.as_str())
        .collect();
    assert_eq!(names.last(), Some(&"LATE_FIELD"), "{names:?}");
    let late_values = texts(&df, "LATE_FIELD");
    assert_eq!(late_values[4_002].as_deref(), Some("late 4002"));
    assert_eq!(
        late_values.iter().filter(|v| v.is_some()).count(),
        (late..total).filter(|i| i.is_multiple_of(3)).count()
    );
    assert_eq!(texts(&df, "MESSAGE")[total - 1].as_deref(), Some("m5999"));
    assert_eq!(
        app.follow().unwrap().misfits(),
        0,
        "a new field is no misfit"
    );
    // The Info tab is read again over every entry once the stream has ended.
    let entries = format!("Entries: {}", datui::numfmt::group_chrome(total));
    follow_until(&mut app, &rx, |app| {
        app.data_table_state
            .as_ref()
            .and_then(|s| s.format_detail())
            .is_some_and(|d| d.lines.contains(&entries))
    });
}

/// NDJSON whose later lines add fields, piped whole: the columns are every line's,
/// typed by their values, and a last line with no newline is a row too.
#[test]
fn ndjson_piped_whole_has_every_line_s_fields() {
    let mut text: String = (0..300).map(|i| format!("{{\"id\": {i}}}\n")).collect();
    text.push_str("{\"id\": 300, \"score\": 1.5, \"ok\": true}\n{\"id\": 301, \"score\": 2}");
    let (mut app, rx) = piped(text.into_bytes(), OpenOptions::default());
    follow_until(&mut app, &rx, ended);
    let df = frame(&app);
    assert_eq!(df.height(), 302);
    assert_eq!(df.column("ok").unwrap().dtype(), &DataType::Boolean);
    let score = df.column("score").unwrap().f64().unwrap().clone();
    assert_eq!(score.get(300), Some(1.5));
    assert_eq!(score.get(301), Some(2.0));
    assert_eq!(score.null_count(), 300);
}

/// `journalctl -o json -f | datui -f -`, cut mid-entry between writes: no read fails,
/// and a field first seen after the open joins once the stream ends.
#[test]
fn a_followed_journal_pipe_reads_whole_entries_only() {
    let total = 3_000;
    let bytes: Vec<u8> = (0..total)
        .flat_map(|i| entry(i, 2_500).into_bytes())
        .collect();
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(&bytes[..1_234]).unwrap();
    let (mut app, rx, _tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions {
            follow: true,
            ..Default::default()
        },
    );
    drain_events(&mut app, &rx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    write_in_steps(&mut app, &rx, &mut producer, &bytes, 1_234, 31_337);
    drop(producer);
    follow_until(&mut app, &rx, ended);
    let df = frame(&app);
    assert_eq!(df.height(), total);
    assert!(
        df.get_column_names()
            .iter()
            .any(|n| n.as_str() == "LATE_FIELD")
    );
}

/// Nothing piped in says so, NDJSON named or not.
#[test]
fn an_empty_pipe_says_nothing_came_in() {
    let (reader, producer) = std::io::pipe().unwrap();
    drop(producer);
    let (mut app, rx, tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions {
            format: Some(datui::FileFormat::Jsonl),
            ..Default::default()
        },
    );
    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        app.error_message()
            .is_some_and(|m| m.contains("Nothing came in on standard input")),
        "{:?}",
        app.error_message()
    );
}

/// Fields that arrive while a query is the view wait for the view to come back to the
/// data, which the query is built on: then they join.
#[test]
fn new_fields_wait_for_a_query_to_be_cleared() {
    let (reader, mut producer) = std::io::pipe().unwrap();
    let head: String = (0..50).map(|i| format!("{{\"id\": {i}}}\n")).collect();
    producer.write_all(head.as_bytes()).unwrap();
    let (mut app, rx, _tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions::default(),
    );
    drain_events(&mut app, &rx);
    let mut next = Some(AppEvent::QQuery("select id where id < 10".to_string()));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    producer
        .write_all(b"{\"id\": 50, \"extra\": \"x\"}\n")
        .unwrap();
    drop(producer);
    follow_until(&mut app, &rx, ended);
    let has_extra = |app: &App| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .schema()
            .contains("extra")
    };
    assert!(!has_extra(&app), "held under the query");
    let mut next = Some(AppEvent::QQuery(String::new()));
    while let Some(event) = next {
        next = app.event(&event);
    }
    follow_until(&mut app, &rx, |app| has_extra(app) && !app.is_busy());
    let df = frame(&app);
    assert_eq!(df.height(), 51);
    assert_eq!(texts(&df, "extra")[50].as_deref(), Some("x"));
}

/// Blank, whitespace-only and non-JSON lines in piped NDJSON: a query over it reads
/// every object, and a line that is not JSON is a row of nulls.
#[test]
fn a_query_reads_ndjson_with_short_lines() {
    let text = "{\"a\":1}\n\n   \n{\"a\":2}\ngarbage\n{\"a\":3}\n";
    let (mut app, rx) = piped(text.as_bytes().to_vec(), OpenOptions::default());
    follow_until(&mut app, &rx, ended);
    let mut next = Some(AppEvent::QQuery("select a where a > 0".to_string()));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    let a = frame(&app).column("a").unwrap().i64().unwrap().to_vec();
    assert_eq!(a, vec![Some(1), Some(2), Some(3)]);
}
